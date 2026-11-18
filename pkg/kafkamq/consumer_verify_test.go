package kafkamq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-core/pkg/errorreporter"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

type capturedEvent struct {
	err  error
	opts errorreporter.Options
}

type fakeReporter struct {
	mu     sync.Mutex
	events []capturedEvent
}

func (f *fakeReporter) Capture(_ context.Context, err error, opts errorreporter.Options) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, capturedEvent{err: err, opts: opts})
}

func (f *fakeReporter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func (f *fakeReporter) last() capturedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.events[len(f.events)-1]
}

func withFakeReporter(t *testing.T) *fakeReporter {
	t.Helper()
	f := &fakeReporter{}
	errorreporter.SetReporter(f)
	return f
}

// TestHandleMessage_ErrorPath_ReachesReporter — хендлер всё время возвращает error:
// после MaxHandleAttempts попыток сообщение уходит в DLT, подтверждается, и ровно одно
// событие долетает до errorreporter (без payload и заголовков).
func TestHandleMessage_ErrorPath_ReachesReporter(t *testing.T) {
	f := withFakeReporter(t)
	setRetryBackoff(t, time.Millisecond)

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`{"city_id":42}`))
	msg.Metadata.Set("authorization", "Bearer secret")

	dlt := &fakeDLT{}
	reg := registration{
		cc: config.ConsumeConfig{
			Topic: "cities",
			Handler: func(ctx context.Context, msg *message.Message) error {
				return errors.New("handler failed")
			},
		},
		dlt: dlt,
	}

	c.handleMessage(context.Background(), reg, msg)

	select {
	case <-msg.Acked():
	case <-time.After(time.Second):
		t.Fatal("expected message to be Acked after moving to DLT")
	}

	if got := dlt.sentCount(); got != 1 {
		t.Fatalf("expected 1 DLT message, got %d", got)
	}

	if got := f.count(); got != 1 {
		t.Fatalf("expected exactly 1 captured event, got %d", got)
	}

	ev := f.last()
	if ev.opts.Level != errorreporter.LevelError {
		t.Fatalf("expected LevelError, got %v", ev.opts.Level)
	}
	if ev.opts.Tags["component"] != "kafka_consumer" || ev.opts.Tags["topic"] != "cities" {
		t.Fatalf("expected component=kafka_consumer and topic=cities tags, got %v", ev.opts.Tags)
	}

	consumerCtx, ok := ev.opts.Extra["consumer"].(map[string]any)
	if !ok {
		t.Fatal("expected 'consumer' extra to be present")
	}
	if consumerCtx["panic"] != false {
		t.Fatalf("expected panic=false for handler error path, got %v", consumerCtx["panic"])
	}
	if consumerCtx["message_uuid"] != msg.UUID {
		t.Fatalf("expected message_uuid=%s, got %v", msg.UUID, consumerCtx["message_uuid"])
	}
	if consumerCtx["attempt"] != MaxHandleAttempts {
		t.Fatalf("expected attempt=%d, got %v", MaxHandleAttempts, consumerCtx["attempt"])
	}
	if _, ok := consumerCtx["payload"]; ok {
		t.Fatalf("payload must not be sent by default, got %v", consumerCtx["payload"])
	}
	if _, ok := consumerCtx["metadata"]; ok {
		t.Fatalf("metadata (headers) must not be sent, got %v", consumerCtx["metadata"])
	}
}

// TestHandleMessage_PanicPath_ReachesReporter — паника в хендлере: должна
// восстановиться (не уронить процесс), пройти повторы, уйти в DLT и
// отрепортиться с panic=true.
func TestHandleMessage_PanicPath_ReachesReporter(t *testing.T) {
	f := withFakeReporter(t)
	setRetryBackoff(t, time.Millisecond)

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`panic-case`))

	dlt := &fakeDLT{}
	reg := registration{
		cc: config.ConsumeConfig{
			Topic: "cities",
			Handler: func(ctx context.Context, msg *message.Message) error {
				panic("unexpected nil map write")
			},
		},
		dlt: dlt,
	}

	// не должно паниковать наружу - recover внутри handleMessage
	c.handleMessage(context.Background(), reg, msg)

	select {
	case <-msg.Acked():
	case <-time.After(time.Second):
		t.Fatal("expected message to be Acked after panic retries and DLT")
	}

	if got := dlt.sentCount(); got != 1 {
		t.Fatalf("expected 1 DLT message, got %d", got)
	}

	if got := f.count(); got != 1 {
		t.Fatalf("expected exactly 1 captured event, got %d", got)
	}

	consumerCtx, ok := f.last().opts.Extra["consumer"].(map[string]any)
	if !ok {
		t.Fatal("expected 'consumer' extra to be present")
	}
	if consumerCtx["panic"] != true {
		t.Fatalf("expected panic=true for panic path, got %v", consumerCtx["panic"])
	}
}

// TestHandleMessage_Success_DoesNotReachReporter — успешный хендлер не должен
// ничего репортить и должен заакать сообщение.
func TestHandleMessage_Success_DoesNotReachReporter(t *testing.T) {
	f := withFakeReporter(t)

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`ok`))

	dlt := &fakeDLT{}
	reg := registration{
		cc: config.ConsumeConfig{
			Topic: "cities",
			Handler: func(ctx context.Context, msg *message.Message) error {
				return nil
			},
		},
		dlt: dlt,
	}

	c.handleMessage(context.Background(), reg, msg)

	select {
	case <-msg.Acked():
	case <-time.After(time.Second):
		t.Fatal("expected message to be Acked")
	}

	if got := f.count(); got != 0 {
		t.Fatalf("expected 0 captured events on success, got %d", got)
	}

	if got := dlt.sentCount(); got != 0 {
		t.Fatalf("expected no DLT messages on success, got %d", got)
	}
}
