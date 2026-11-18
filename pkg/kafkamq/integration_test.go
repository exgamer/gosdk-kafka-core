//go:build integration

package kafkamq

// Интеграционные тесты с настоящей Kafka во временном контейнере (KRaft, один узел).
// Запуск: go test -tags integration -race -count=1 ./pkg/kafkamq/
// Образ — KAFKA_IT_IMAGE (по умолчанию apache/kafka:4.3.1). Контейнер получает уникальное
// имя и случайный порт и удаляется после теста.

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	sdkctx "github.com/exgamer/gosdk-core/pkg/context"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

const defaultKafkaITImage = "apache/kafka:4.3.1"

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

// startKafka поднимает одиночный узел Kafka (KRaft) и возвращает адрес брокера.
func startKafka(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found")
	}

	image := os.Getenv("KAFKA_IT_IMAGE")
	if image == "" {
		image = defaultKafkaITImage
	}

	port := freePort(t)
	name := "gosdk-kafka-it-" + strings.ToLower(watermill.NewShortUUID())
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	args := []string{
		"run", "-d", "--name", name,
		"-p", fmt.Sprintf("127.0.0.1:%d:%d", port, port),
		"-e", "KAFKA_NODE_ID=1",
		"-e", "KAFKA_PROCESS_ROLES=broker,controller",
		"-e", fmt.Sprintf("KAFKA_LISTENERS=PLAINTEXT://:%d,CONTROLLER://:9093", port),
		"-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + addr,
		"-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
		"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
		"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		"-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		"-e", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
		image,
	}

	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}

	t.Cleanup(func() {
		if out, err := exec.Command("docker", "rm", "-f", "-v", name).CombinedOutput(); err != nil {
			t.Logf("docker rm %s: %v\n%s", name, err, out)
		}
	})

	deadline := time.Now().Add(120 * time.Second)
	for {
		cfg := sarama.NewConfig()
		admin, err := sarama.NewClusterAdmin([]string{addr}, cfg)
		if err == nil {
			_, err = admin.ListTopics()
			_ = admin.Close()

			if err == nil {
				return addr
			}
		}

		if time.Now().After(deadline) {
			logs, _ := exec.Command("docker", "logs", "--tail", "50", name).CombinedOutput()
			t.Fatalf("kafka not ready: %v\n%s", err, logs)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

type itMessage struct {
	key, value string
	headers    map[string]string
}

func produce(t *testing.T, addr, topic string, msgs ...itMessage) {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	p, err := sarama.NewSyncProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()

	for _, m := range msgs {
		pm := &sarama.ProducerMessage{Topic: topic, Partition: 0, Value: sarama.StringEncoder(m.value)}
		if m.key != "" {
			pm.Key = sarama.StringEncoder(m.key)
		}

		pm.Headers = append(pm.Headers, sarama.RecordHeader{Key: []byte("_watermill_message_uuid"), Value: []byte(watermill.NewUUID())})
		for k, v := range m.headers {
			pm.Headers = append(pm.Headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
		}

		if _, _, err := p.SendMessage(pm); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
}

func committedOffset(t *testing.T, addr, group, topic string) int64 {
	t.Helper()

	admin, err := sarama.NewClusterAdmin([]string{addr}, sarama.NewConfig())
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	defer admin.Close()

	resp, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: {0}})
	if err != nil {
		t.Fatalf("list offsets: %v", err)
	}

	b := resp.GetBlock(topic, 0)
	if b == nil {
		return -1
	}

	return b.Offset
}

func waitCommitted(t *testing.T, addr, group, topic string, want int64) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for {
		got := committedOffset(t, addr, group, topic)
		if got == want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("committed offset = %d, want %d", got, want)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func declare(t *testing.T, addr string, topics ...string) {
	t.Helper()

	specs := make([]TopicSpec, 0, len(topics))
	for _, name := range topics {
		specs = append(specs, TopicSpec{Name: name, NumPartitions: 1, ReplicationFactor: 1})
	}

	if err := DeclareTopics([]string{addr}, nil, specs...); err != nil {
		t.Fatalf("declare topics: %v", err)
	}

	// повторная декларация — не ошибка (ErrTopicAlreadyExists по коду)
	if err := DeclareTopics([]string{addr}, nil, specs...); err != nil {
		t.Fatalf("re-declare topics: %v", err)
	}
}

func TestIntegration(t *testing.T) {
	addr := startKafka(t)

	t.Run("PoisonMessageGoesToDLT", func(t *testing.T) { testPoisonMessageGoesToDLT(t, addr) })
	t.Run("StopWaitsForHandler", func(t *testing.T) { testStopWaitsForHandler(t, addr) })
	t.Run("PublishCtxCorrelationID", func(t *testing.T) { testPublishCtxCorrelationID(t, addr) })
	t.Run("DLTDeclaredOnDemand", func(t *testing.T) { testDLTDeclaredOnDemand(t, addr) })
}

// Нет топика <topic>.dlt (автосоздание на брокере выключено): SDK создаёт его с числом партиций
// исходного топика и публикует ядовитое сообщение; offset исходного сдвигается.
func testDLTDeclaredOnDemand(t *testing.T, addr string) {
	setRetryBackoff(t, 20*time.Millisecond)

	suffix := strings.ToLower(watermill.NewShortUUID())
	topic := "it-nodlt-" + suffix
	group := "it-group-nodlt-" + suffix

	if err := DeclareTopics([]string{addr}, nil, TopicSpec{Name: topic, NumPartitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatalf("declare: %v", err)
	}

	// в партицию 0 (waitCommitted смотрит её)
	pcfg := sarama.NewConfig()
	pcfg.Producer.Return.Successes = true
	pcfg.Producer.Partitioner = sarama.NewManualPartitioner

	prod, err := sarama.NewSyncProducer([]string{addr}, pcfg)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}

	_, _, err = prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Partition: 0, Key: sarama.StringEncoder("k-1"), Value: sarama.StringEncoder("poison")})
	_ = prod.Close()

	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	var calls atomic.Int32
	handler := func(ctx context.Context, msg *message.Message) error {
		calls.Add(1)

		return fmt.Errorf("always fails")
	}

	c, err := NewConsumer(&config.KafkaConfig{Brokers: addr, ClientID: "it-client"})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.RegisterHandler(context.Background(), handler, topic, config.NewConsumerFromOldestConfig(group)...); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumeErr := make(chan error, 1)

	go func() { consumeErr <- c.Consume(ctx) }()

	waitCommitted(t, addr, group, topic, 1)

	cancel()

	if err := <-consumeErr; err != nil {
		t.Fatalf("Consume() = %v, want nil on stop", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	if got := calls.Load(); got != MaxHandleAttempts {
		t.Fatalf("handler calls = %d, want %d", got, MaxHandleAttempts)
	}

	admin, err := sarama.NewClusterAdmin([]string{addr}, sarama.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	meta, err := admin.DescribeTopics([]string{DLTTopic(topic)})
	if err != nil || len(meta) != 1 || meta[0].Err != sarama.ErrNoError {
		t.Fatalf("describe DLT: %v %v", meta, err)
	}

	if got := len(meta[0].Partitions); got != 3 {
		t.Fatalf("DLT partitions = %d, want 3 (as the source topic)", got)
	}

	dc, err := sarama.NewConsumer([]string{addr}, sarama.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()

	found := 0
	for _, part := range meta[0].Partitions {
		pc, err := dc.ConsumePartition(DLTTopic(topic), part.ID, sarama.OffsetOldest)
		if err != nil {
			t.Fatal(err)
		}

		select {
		case m := <-pc.Messages():
			if string(m.Value) != "poison" || string(m.Key) != "k-1" {
				t.Errorf("DLT key/value = %q/%q", m.Key, m.Value)
			}

			found++
		case <-time.After(2 * time.Second):
		}

		_ = pc.Close()
	}

	if found != 1 {
		t.Fatalf("DLT messages = %d, want 1", found)
	}
}

// Ядовитое сообщение: MaxHandleAttempts попыток, затем <topic>.dlt (ключ, payload, заголовки,
// причина, попытки), offset сдвинут, следующее сообщение партиции обработано.
func testPoisonMessageGoesToDLT(t *testing.T, addr string) {
	setRetryBackoff(t, 50*time.Millisecond, 100*time.Millisecond)

	suffix := strings.ToLower(watermill.NewShortUUID())
	topic := "it-orders-" + suffix
	group := "it-group-" + suffix

	declare(t, addr, topic, DLTTopic(topic))
	produce(t, addr, topic,
		itMessage{key: "order-1", value: "poison", headers: map[string]string{"trace-id": "t-1", CorrelationIDMetadataKey: "corr-it"}},
		itMessage{key: "order-2", value: "good"},
	)

	var (
		mu            sync.Mutex
		poisonCalls   int
		poisonIDs     []string
		goodAfterPois bool
	)

	goodDone := make(chan struct{})
	handler := func(ctx context.Context, msg *message.Message) error {
		mu.Lock()
		defer mu.Unlock()

		switch string(msg.Payload) {
		case "poison":
			poisonCalls++
			poisonIDs = append(poisonIDs, sdkctx.CorrelationID(ctx))

			return fmt.Errorf("cannot parse order")
		case "good":
			goodAfterPois = poisonCalls == MaxHandleAttempts
			close(goodDone)
		}

		return nil
	}

	c, err := NewConsumer(&config.KafkaConfig{Brokers: addr, ClientID: "it-client"})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.RegisterHandler(context.Background(), handler, topic, config.NewConsumerFromOldestConfig(group)...); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumeErr := make(chan error, 1)

	go func() { consumeErr <- c.Consume(ctx) }()

	select {
	case <-goodDone:
	case err := <-consumeErr:
		t.Fatalf("Consume returned early: %v", err)
	case <-time.After(90 * time.Second):
		t.Fatal("next message of the partition was not processed")
	}

	mu.Lock()
	if poisonCalls != MaxHandleAttempts || !goodAfterPois {
		t.Fatalf("poison calls = %d (want %d), good after poison = %v", poisonCalls, MaxHandleAttempts, goodAfterPois)
	}

	for i, id := range poisonIDs {
		if id != "corr-it" {
			t.Fatalf("poison attempt %d: correlation id = %q, want corr-it", i+1, id)
		}
	}
	mu.Unlock()

	cancel()

	if err := <-consumeErr; err != nil {
		t.Fatalf("Consume() = %v, want nil on stop", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	waitCommitted(t, addr, group, topic, 2)

	// DLT
	dc, err := sarama.NewConsumer([]string{addr}, sarama.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()

	pc, err := dc.ConsumePartition(DLTTopic(topic), 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	select {
	case m := <-pc.Messages():
		h := map[string]string{}
		for _, rh := range m.Headers {
			h[string(rh.Key)] = string(rh.Value)
		}

		if string(m.Key) != "order-1" || string(m.Value) != "poison" {
			t.Fatalf("DLT key/value = %q/%q", m.Key, m.Value)
		}

		want := map[string]string{
			"trace-id":                 "t-1",
			DLTHeaderOriginalTopic:     topic,
			DLTHeaderOriginalPartition: "0",
			DLTHeaderOriginalOffset:    "0",
			DLTHeaderConsumerGroup:     group,
			DLTHeaderAttempts:          "5",
			DLTHeaderError:             "cannot parse order",
			CorrelationIDMetadataKey:   "corr-it",
		}
		for k, v := range want {
			if h[k] != v {
				t.Errorf("DLT header %s = %q, want %q", k, h[k], v)
			}
		}

		if h["_watermill_message_uuid"] == "" {
			t.Error("DLT message lost the original message UUID header")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no message in DLT")
	}

	select {
	case m := <-pc.Messages():
		t.Fatalf("unexpected second DLT message: %q", m.Value)
	case <-time.After(time.Second):
	}
}

// Остановка дожидается текущего handler-а: его ctx не отменяется, Consume возвращается
// после него, Ack коммитится; следующее сообщение в работу не берётся.
func testStopWaitsForHandler(t *testing.T, addr string) {
	suffix := strings.ToLower(watermill.NewShortUUID())
	topic := "it-slow-" + suffix
	group := "it-group-" + suffix

	declare(t, addr, topic, DLTTopic(topic))
	produce(t, addr, topic, itMessage{value: "slow"}, itMessage{value: "next"})

	started := make(chan struct{})

	var (
		finished    atomic.Bool
		nextHandled atomic.Bool
		ctxErr      atomic.Value
	)

	handler := func(hctx context.Context, msg *message.Message) error {
		switch string(msg.Payload) {
		case "slow":
			close(started)
			time.Sleep(2 * time.Second)

			if err := hctx.Err(); err != nil {
				ctxErr.Store(err)
			}

			finished.Store(true)
		case "next":
			nextHandled.Store(true)
		}

		return nil
	}

	c, err := NewConsumer(&config.KafkaConfig{Brokers: addr})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.RegisterHandler(context.Background(), handler, topic, config.NewConsumerFromOldestConfig(group)...); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumeErr := make(chan error, 1)

	go func() { consumeErr <- c.Consume(ctx) }()

	select {
	case <-started:
	case err := <-consumeErr:
		t.Fatalf("Consume returned early: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("handler did not start")
	}

	cancel()

	select {
	case err := <-consumeErr:
		if err != nil {
			t.Fatalf("Consume() = %v, want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Consume did not return after stop")
	}

	if !finished.Load() {
		t.Fatal("Consume returned before the current handler finished")
	}

	if v := ctxErr.Load(); v != nil {
		t.Fatalf("handler ctx was cancelled by stop: %v", v)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	if nextHandled.Load() {
		t.Fatal("next message must not be taken after stop")
	}

	waitCommitted(t, addr, group, topic, 1)
}

// PublishWithMetaDataCtx: id корреляции ctx уходит в заголовок correlation_id и доходит до ctx обработчика;
// явная метадата вызывающего главнее.
func testPublishCtxCorrelationID(t *testing.T, addr string) {
	suffix := strings.ToLower(watermill.NewShortUUID())
	topic := "it-corr-" + suffix
	group := "it-group-" + suffix

	declare(t, addr, topic, DLTTopic(topic))

	pub, err := NewPublisher(&config.KafkaConfig{Brokers: addr, ClientID: "it-client"}, config.NewPublisherConfig()...)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(sdkctx.WithCorrelationID(context.Background(), "corr-pub"), 30*time.Second)
	defer cancel()

	if err := pub.PublishWithMetaDataCtx(ctx, topic, map[string]string{"case": "ctx"}, 1); err != nil {
		t.Fatalf("publish ctx: %v", err)
	}

	if err := pub.PublishWithMetaDataCtx(ctx, topic, map[string]string{"case": "explicit", CorrelationIDMetadataKey: "own"}, 2); err != nil {
		t.Fatalf("publish explicit: %v", err)
	}

	got := make(chan string, 2)
	handler := func(ctx context.Context, msg *message.Message) error {
		got <- msg.Metadata.Get("case") + "=" + sdkctx.CorrelationID(ctx)

		return nil
	}

	c, err := NewConsumer(&config.KafkaConfig{Brokers: addr, ClientID: "it-client"})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.RegisterHandler(context.Background(), handler, topic, config.NewConsumerFromOldestConfig(group)...); err != nil {
		t.Fatal(err)
	}

	runCtx, stop := context.WithCancel(context.Background())
	consumeErr := make(chan error, 1)

	go func() { consumeErr <- c.Consume(runCtx) }()

	defer func() {
		stop()
		<-consumeErr
		_ = c.Close()
	}()

	want := map[string]bool{"ctx=corr-pub": true, "explicit=own": true}

	for range len(want) {
		select {
		case v := <-got:
			if !want[v] {
				t.Errorf("handler saw %q, want one of %v", v, want)
			}

			delete(want, v)
		case <-time.After(60 * time.Second):
			t.Fatalf("not delivered: %v", want)
		}
	}
}
