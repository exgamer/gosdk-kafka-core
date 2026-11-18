package kafkamq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

// fakeDLT — dltSender в памяти: первые failN отправок падают.
type fakeDLT struct {
	mu     sync.Mutex
	failN  int
	calls  int
	sent   []*sarama.ProducerMessage
	closed bool
}

func (f *fakeDLT) Send(msg *sarama.ProducerMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if f.failN < 0 || f.calls <= f.failN {
		return errors.New("broker unavailable")
	}

	f.sent = append(f.sent, msg)

	return nil
}

func (f *fakeDLT) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true

	return nil
}

func (f *fakeDLT) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.sent)
}

func (f *fakeDLT) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

// setRetryBackoff подменяет ступени пауз на время теста.
func setRetryBackoff(t *testing.T, steps ...time.Duration) {
	t.Helper()

	prev := retryBackoff
	retryBackoff = steps

	t.Cleanup(func() { retryBackoff = prev })
}

func headerMap(pm *sarama.ProducerMessage) map[string]string {
	out := make(map[string]string, len(pm.Headers))
	for _, h := range pm.Headers {
		out[string(h.Key)] = string(h.Value)
	}

	return out
}

func encoded(t *testing.T, e sarama.Encoder) string {
	t.Helper()

	if e == nil {
		return ""
	}

	b, err := e.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	return string(b)
}

func assertNotAckedNorNacked(t *testing.T, msg *message.Message) {
	t.Helper()

	select {
	case <-msg.Acked():
		t.Fatal("message must not be acked")
	case <-msg.Nacked():
		t.Fatal("message must not be nacked")
	default:
	}
}

// TestRetryDelay_Steps — ступени 1с, 5с, 15с; дальше последней — последняя; сумма пауз серии
// с запасом меньше Rebalance.Timeout sarama по умолчанию.
func TestRetryDelay_Steps(t *testing.T) {
	want := map[int]time.Duration{
		0: time.Second,
		1: time.Second,
		2: 5 * time.Second,
		3: 15 * time.Second,
		4: 15 * time.Second,
		9: 15 * time.Second,
	}

	var total time.Duration
	for attempt := 1; attempt < MaxHandleAttempts; attempt++ {
		total += retryDelay(attempt)
	}

	if rebalance := sarama.NewConfig().Consumer.Group.Rebalance.Timeout; total > rebalance*2/3 {
		t.Errorf("retry pauses %s too close to sarama Rebalance.Timeout %s", total, rebalance)
	}

	for attempt, d := range want {
		if got := retryDelay(attempt); got != d {
			t.Errorf("retryDelay(%d) = %s, want %s", attempt, got, d)
		}
	}

	if MaxHandleAttempts != 5 {
		t.Fatalf("MaxHandleAttempts = %d, want 5", MaxHandleAttempts)
	}
}

// TestHandleMessage_RetriesThenDLT — handler падает всегда: ровно MaxHandleAttempts вызовов
// с паузами по ступеням, затем одна публикация в DLT и Ack.
func TestHandleMessage_RetriesThenDLT(t *testing.T) {
	withFakeReporter(t)
	setRetryBackoff(t, 10*time.Millisecond, 20*time.Millisecond)

	var calls []time.Time

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`poison`))
	dlt := &fakeDLT{}
	reg := registration{
		cc: config.ConsumeConfig{
			Topic: "orders",
			Handler: func(_ context.Context, _ *message.Message) error {
				calls = append(calls, time.Now())

				return errors.New("bad payload")
			},
		},
		consumerGroup: "orders-service",
		dlt:           dlt,
	}

	c.handleMessage(context.Background(), reg, msg)

	if len(calls) != MaxHandleAttempts {
		t.Fatalf("handler calls = %d, want %d", len(calls), MaxHandleAttempts)
	}

	// паузы: 10мс, 20мс, 20мс, 20мс
	wantMin := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}
	for i, min := range wantMin {
		if gap := calls[i+1].Sub(calls[i]); gap < min {
			t.Errorf("pause after attempt %d = %s, want >= %s", i+1, gap, min)
		}
	}

	if dlt.sentCount() != 1 {
		t.Fatalf("DLT messages = %d, want 1", dlt.sentCount())
	}

	h := headerMap(dlt.sent[0])
	if h[DLTHeaderAttempts] != "5" || h[DLTHeaderError] != "bad payload" || h[DLTHeaderConsumerGroup] != "orders-service" {
		t.Fatalf("unexpected DLT headers: %v", h)
	}

	select {
	case <-msg.Acked():
	default:
		t.Fatal("expected Ack after DLT")
	}
}

// TestHandleMessage_SucceedsOnRetry — успех на третьей попытке: Ack, DLT не трогается.
func TestHandleMessage_SucceedsOnRetry(t *testing.T) {
	setRetryBackoff(t, time.Millisecond)

	n := 0
	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))
	dlt := &fakeDLT{}
	reg := registration{
		cc: config.ConsumeConfig{Topic: "orders", Handler: func(_ context.Context, _ *message.Message) error {
			n++
			if n < 3 {
				return errors.New("temporary")
			}

			return nil
		}},
		dlt: dlt,
	}

	c.handleMessage(context.Background(), reg, msg)

	if n != 3 || dlt.callCount() != 0 {
		t.Fatalf("calls=%d dltCalls=%d, want 3 and 0", n, dlt.callCount())
	}

	select {
	case <-msg.Acked():
	default:
		t.Fatal("expected Ack")
	}
}

// TestHandleMessage_ActionAck_NoRetryNoDLT — политика ActionAck сохраняет прежнее поведение:
// одна попытка, Ack, без DLT.
func TestHandleMessage_ActionAck_NoRetryNoDLT(t *testing.T) {
	f := withFakeReporter(t)
	setRetryBackoff(t, time.Millisecond)

	n := 0
	c := &Consumer{onError: ActionAck, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))
	dlt := &fakeDLT{}
	reg := registration{
		cc: config.ConsumeConfig{Topic: "orders", Handler: func(_ context.Context, _ *message.Message) error {
			n++

			return errors.New("business error")
		}},
		dlt: dlt,
	}

	c.handleMessage(context.Background(), reg, msg)

	if n != 1 || dlt.callCount() != 0 || f.count() != 1 {
		t.Fatalf("calls=%d dltCalls=%d events=%d, want 1, 0, 1", n, dlt.callCount(), f.count())
	}

	select {
	case <-msg.Acked():
	default:
		t.Fatal("expected Ack")
	}
}

// TestHandleMessage_DLTFailureRetriedUntilSuccess — DLT недоступен: сообщение не
// подтверждается, публикация повторяется; после успеха — Ack.
func TestHandleMessage_DLTFailureRetriedUntilSuccess(t *testing.T) {
	withFakeReporter(t)
	setRetryBackoff(t, time.Millisecond)

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))
	dlt := &fakeDLT{failN: 2}
	reg := registration{
		cc: config.ConsumeConfig{Topic: "orders", Handler: func(_ context.Context, _ *message.Message) error {
			return errors.New("bad")
		}},
		dlt: dlt,
	}

	c.handleMessage(context.Background(), reg, msg)

	if dlt.callCount() != 3 || dlt.sentCount() != 1 {
		t.Fatalf("dlt calls=%d sent=%d, want 3 and 1", dlt.callCount(), dlt.sentCount())
	}

	select {
	case <-msg.Acked():
	default:
		t.Fatal("expected Ack after successful DLT publish")
	}
}

// TestHandleMessage_DLTDownAndStop_NotAcked — DLT недоступен и пришла остановка:
// сообщение не подтверждается и не Nack-ается (будет доставлено снова после рестарта).
func TestHandleMessage_DLTDownAndStop_NotAcked(t *testing.T) {
	withFakeReporter(t)
	setRetryBackoff(t, time.Millisecond, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	dlt := &fakeDLT{failN: -1}

	go func() {
		for dlt.callCount() < 3 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))
	reg := registration{
		cc: config.ConsumeConfig{Topic: "orders", Handler: func(_ context.Context, _ *message.Message) error {
			return errors.New("bad")
		}},
		dlt: dlt,
	}

	done := make(chan struct{})
	go func() {
		c.handleMessage(ctx, reg, msg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleMessage did not return after stop")
	}

	assertNotAckedNorNacked(t, msg)
}

// TestHandleMessage_StopDuringBackoff — остановка во время паузы между попытками прерывает
// паузу сразу, без Ack/Nack.
func TestHandleMessage_StopDuringBackoff(t *testing.T) {
	setRetryBackoff(t, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))
	reg := registration{
		cc: config.ConsumeConfig{Topic: "orders", Handler: func(_ context.Context, _ *message.Message) error {
			calls.Add(1)
			cancel()

			return errors.New("bad")
		}},
		dlt: &fakeDLT{},
	}

	start := time.Now()
	c.handleMessage(ctx, reg, msg)

	if time.Since(start) > time.Second {
		t.Fatal("backoff was not interrupted by stop")
	}

	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}

	assertNotAckedNorNacked(t, msg)
}

// TestHandleMessage_HandlerCtxNotCancelledByStop — ctx handler-а не отменяется остановкой
// посреди обработки (значения контекста сохраняются).
func TestHandleMessage_HandlerCtxNotCancelledByStop(t *testing.T) {
	type key struct{}

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "v"))

	var handlerCtxErr error
	var value any

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))
	reg := registration{
		cc: config.ConsumeConfig{Topic: "orders", Handler: func(hctx context.Context, _ *message.Message) error {
			cancel() // остановка посреди обработки
			handlerCtxErr = hctx.Err()
			value = hctx.Value(key{})

			return nil
		}},
		dlt: &fakeDLT{},
	}

	c.handleMessage(ctx, reg, msg)

	if handlerCtxErr != nil {
		t.Fatalf("handler ctx cancelled by stop: %v", handlerCtxErr)
	}

	if value != "v" {
		t.Fatalf("handler ctx lost values, got %v", value)
	}

	select {
	case <-msg.Acked():
	default:
		t.Fatal("handler finished successfully, message must be acked")
	}
}

// TestBuildDLTMessage — исходные ключ, payload и заголовки + причина, попытки, позиция.
func TestBuildDLTMessage(t *testing.T) {
	d := deliveryInfo{
		topic:         "orders",
		consumerGroup: "orders-service",
		uuid:          "uuid-1",
		key:           []byte("order-42"),
		payload:       []byte(`{"id":42}`),
		metadata: map[string]string{
			"trace-id":             "abc",
			DLTHeaderAttempts:      "99", // от прежнего попадания в DLT — должен замениться
			DLTHeaderOriginalTopic: "old",
		},
		partition: 3, hasPartition: true,
		offset: 1234, hasOffset: true,
	}

	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	pm := buildDLTMessage(d, errors.New(strings.Repeat("e", maxDLTErrorLen+100)), 5, now)

	if pm.Topic != "orders.dlt" {
		t.Fatalf("topic = %q, want orders.dlt", pm.Topic)
	}

	if got := encoded(t, pm.Key); got != "order-42" {
		t.Fatalf("key = %q, want order-42", got)
	}

	if got := encoded(t, pm.Value); got != `{"id":42}` {
		t.Fatalf("value = %q", got)
	}

	h := headerMap(pm)
	want := map[string]string{
		"_watermill_message_uuid":  "uuid-1",
		"trace-id":                 "abc",
		DLTHeaderOriginalTopic:     "orders",
		DLTHeaderOriginalPartition: "3",
		DLTHeaderOriginalOffset:    "1234",
		DLTHeaderConsumerGroup:     "orders-service",
		DLTHeaderAttempts:          "5",
		DLTHeaderFailedAt:          "2026-11-18T10:00:00Z",
	}

	for k, v := range want {
		if h[k] != v {
			t.Errorf("header %s = %q, want %q", k, h[k], v)
		}
	}

	if len(h[DLTHeaderError]) != maxDLTErrorLen {
		t.Errorf("error header len = %d, want %d (truncated)", len(h[DLTHeaderError]), maxDLTErrorLen)
	}

	// заголовки без дублей
	if len(pm.Headers) != len(h) {
		t.Errorf("duplicate headers: %d headers, %d unique", len(pm.Headers), len(h))
	}
}

// TestBuildDLTMessage_NilKey — сообщение без ключа уходит в DLT без ключа.
func TestBuildDLTMessage_NilKey(t *testing.T) {
	pm := buildDLTMessage(deliveryInfo{topic: "t", payload: []byte("p")}, errors.New("x"), 1, time.Now())
	if pm.Key != nil {
		t.Fatalf("key = %v, want nil", pm.Key)
	}

	h := headerMap(pm)
	if _, ok := h[DLTHeaderOriginalPartition]; ok {
		t.Fatal("partition header must be absent when partition is unknown")
	}
}

// TestNewDeliveryInfo_SnapshotIsolated — снимок не зависит от правок handler-а в msg.
func TestNewDeliveryInfo_SnapshotIsolated(t *testing.T) {
	msg := message.NewMessage("u", []byte("orig"))
	msg.Metadata.Set("h", "v")

	d := newDeliveryInfo("t", "g", msg)
	msg.Payload[0] = 'X'
	msg.Metadata.Set("h", "changed")

	if string(d.payload) != "orig" || d.metadata["h"] != "v" {
		t.Fatalf("snapshot changed: payload=%q h=%q", d.payload, d.metadata["h"])
	}
}

// TestCaptureSentry_SafeMetadataOnly — по умолчанию только безопасные поля; payload — лишь
// при явном WithSentryPayloadLimit.
func TestCaptureSentry_SafeMetadataOnly(t *testing.T) {
	f := withFakeReporter(t)

	d := &deliveryInfo{
		topic: "orders", uuid: "u", key: []byte("order-42"), payload: []byte("secret-payload"),
		metadata:  map[string]string{"authorization": "Bearer x"},
		partition: 1, hasPartition: true, offset: 7, hasOffset: true,
	}

	c, _ := NewConsumer(&config.KafkaConfig{Brokers: "127.0.0.1:0"})
	c.captureSentry(context.Background(), errors.New("e"), "h", "orders", d, 3, false)

	ctxMap := f.last().opts.Extra["consumer"].(map[string]any)
	for _, k := range []string{"payload", "metadata", "key"} {
		if _, ok := ctxMap[k]; ok {
			t.Errorf("field %q must not be sent by default", k)
		}
	}

	if ctxMap["topic"] != "orders" || ctxMap["partition"] != int32(1) || ctxMap["offset"] != int64(7) ||
		ctxMap["attempt"] != 3 || ctxMap["key_len"] != 8 {
		t.Fatalf("unexpected safe metadata: %v", ctxMap)
	}

	if s, _ := ctxMap["key_sha256"].(string); len(s) != 16 || strings.Contains(s, "order") {
		t.Fatalf("key_sha256 = %v, want 16 hex chars", ctxMap["key_sha256"])
	}

	c.WithSentryPayloadLimit(6)
	c.captureSentry(context.Background(), errors.New("e"), "h", "orders", d, 3, false)

	ctxMap = f.last().opts.Extra["consumer"].(map[string]any)
	if ctxMap["payload"] != "secret" || ctxMap["payload_truncated"] != true {
		t.Fatalf("explicit payload limit: got payload=%v truncated=%v", ctxMap["payload"], ctxMap["payload_truncated"])
	}

	// 0 и меньше — не отправлять (как в gosdk-rabbit-core)
	for _, limit := range []int{0, -1} {
		c.WithSentryPayloadLimit(limit)
		c.captureSentry(context.Background(), errors.New("e"), "h", "orders", d, 3, false)

		ctxMap = f.last().opts.Extra["consumer"].(map[string]any)
		if _, ok := ctxMap["payload"]; ok {
			t.Errorf("WithSentryPayloadLimit(%d): payload must not be sent, got %v", limit, ctxMap["payload"])
		}
	}
}

// TestLazyDLTProducer_DeclaresMissingTopic — нет топика DLT: одна попытка создать, затем повтор публикации.
func TestLazyDLTProducer_DeclaresMissingTopic(t *testing.T) {
	mock := mocks.NewSyncProducer(t, nil)
	mock.ExpectSendMessageAndFail(sarama.ErrUnknownTopicOrPartition)
	mock.ExpectSendMessageAndSucceed()

	var declared []string
	p := &lazyDLTProducer{
		newProducer: func() (sarama.SyncProducer, error) { return mock, nil },
		declare: func(topic string) error {
			declared = append(declared, topic)

			return nil
		},
	}

	if err := p.Send(&sarama.ProducerMessage{Topic: "orders.dlt", Value: sarama.StringEncoder("x")}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if len(declared) != 1 || declared[0] != "orders.dlt" {
		t.Fatalf("declared = %v, want [orders.dlt]", declared)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestLazyDLTProducer_DeclareFails — создать топик не удалось: ошибка публикации без повтора отправки.
func TestLazyDLTProducer_DeclareFails(t *testing.T) {
	mock := mocks.NewSyncProducer(t, nil)
	mock.ExpectSendMessageAndFail(sarama.ErrUnknownTopicOrPartition)

	p := &lazyDLTProducer{
		newProducer: func() (sarama.SyncProducer, error) { return mock, nil },
		declare:     func(string) error { return errors.New("not authorized") },
	}

	err := p.Send(&sarama.ProducerMessage{Topic: "orders.dlt", Value: sarama.StringEncoder("x")})
	if !errors.Is(err, sarama.ErrUnknownTopicOrPartition) || !strings.Contains(err.Error(), "not authorized") {
		t.Fatalf("Send error = %v", err)
	}

	_ = p.Close()
}

// TestLazyDLTProducer_OtherErrorNoDeclare — другая ошибка публикации топик не создаёт.
func TestLazyDLTProducer_OtherErrorNoDeclare(t *testing.T) {
	mock := mocks.NewSyncProducer(t, nil)
	mock.ExpectSendMessageAndFail(sarama.ErrNotEnoughReplicas)

	p := &lazyDLTProducer{
		newProducer: func() (sarama.SyncProducer, error) { return mock, nil },
		declare: func(string) error {
			t.Fatal("declare must not be called")

			return nil
		},
	}

	if err := p.Send(&sarama.ProducerMessage{Topic: "orders.dlt", Value: sarama.StringEncoder("x")}); !errors.Is(err, sarama.ErrNotEnoughReplicas) {
		t.Fatalf("Send error = %v", err)
	}

	_ = p.Close()
}

// TestConsumer_Close_ClosesDLTProducers — Close закрывает и DLT-продюсеры.
func TestConsumer_Close_ClosesDLTProducers(t *testing.T) {
	dlt := &fakeDLT{}
	c := &Consumer{handlers: []registration{{dlt: dlt}}}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !dlt.closed {
		t.Fatal("DLT producer was not closed")
	}
}
