package kafkamq

import (
	"errors"
	"fmt"
	"testing"

	"github.com/IBM/sarama"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

// TestResolveSubscriberConfig_ClientIDAfterHelpers — помощники подставляют свой sarama.Config
// с ClientID="watermill"; KAFKA_CLIENT_ID должен примениться после них.
func TestResolveSubscriberConfig_ClientIDAfterHelpers(t *testing.T) {
	kc := &config.KafkaConfig{Brokers: "b:9092", ClientID: "orders-service"}

	cases := map[string][]config.ConsumerOption{
		"no options":             nil,
		"NewConsumerGroupConfig": config.NewConsumerGroupConfig("g", sarama.OffsetOldest),
		"FromOldest":             config.NewConsumerFromOldestConfig("g"),
		"FromNewest":             config.NewConsumerFromNewestConfig("g"),
		"sarama.NewConfig":       {config.WithConsumerSaramaConfig(sarama.NewConfig())},
	}

	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			sc := resolveSubscriberConfig(kc, opts)
			if sc.OverwriteSaramaConfig == nil || sc.OverwriteSaramaConfig.ClientID != "orders-service" {
				t.Fatalf("ClientID = %v, want orders-service", sc.OverwriteSaramaConfig)
			}
		})
	}

	// помощник сохранил свой начальный offset
	sc := resolveSubscriberConfig(kc, config.NewConsumerFromOldestConfig("g"))
	if sc.OverwriteSaramaConfig.Consumer.Offsets.Initial != sarama.OffsetOldest || sc.ConsumerGroup != "g" {
		t.Fatalf("helper settings lost: initial=%d group=%q", sc.OverwriteSaramaConfig.Consumer.Offsets.Initial, sc.ConsumerGroup)
	}
}

// TestResolveSubscriberConfig_ExplicitClientIDKept — ClientID, выбранный сервисом, не трогаем;
// чужой sarama.Config не мутируется.
func TestResolveSubscriberConfig_ExplicitClientIDKept(t *testing.T) {
	kc := &config.KafkaConfig{Brokers: "b:9092", ClientID: "orders-service"}

	explicit := sarama.NewConfig()
	explicit.ClientID = "custom"

	sc := resolveSubscriberConfig(kc, []config.ConsumerOption{config.WithConsumerSaramaConfig(explicit)})
	if sc.OverwriteSaramaConfig.ClientID != "custom" {
		t.Fatalf("ClientID = %q, want custom", sc.OverwriteSaramaConfig.ClientID)
	}

	shared := sarama.NewConfig() // ClientID "sarama"
	sc = resolveSubscriberConfig(kc, []config.ConsumerOption{config.WithConsumerSaramaConfig(shared)})
	if shared.ClientID != "sarama" {
		t.Fatalf("service config mutated: ClientID = %q", shared.ClientID)
	}
	if sc.OverwriteSaramaConfig.ClientID != "orders-service" {
		t.Fatalf("ClientID = %q, want orders-service", sc.OverwriteSaramaConfig.ClientID)
	}
}

// TestResolveSubscriberConfig_NoClientID — без KAFKA_CLIENT_ID поведение как раньше
// (дефолт watermill-kafka).
func TestResolveSubscriberConfig_NoClientID(t *testing.T) {
	sc := resolveSubscriberConfig(&config.KafkaConfig{Brokers: "b:9092", ConsumerGroup: "g"}, nil)
	if sc.OverwriteSaramaConfig.ClientID != "watermill" || sc.ConsumerGroup != "g" {
		t.Fatalf("got ClientID=%q group=%q", sc.OverwriteSaramaConfig.ClientID, sc.ConsumerGroup)
	}
}

// TestResolvePublisherConfig_ReliableDefaults — без явного sarama.Config: WaitForAll,
// Idempotent, MaxOpenRequests=1, конфиг валиден; ClientID применяется и после помощника.
func TestResolvePublisherConfig_ReliableDefaults(t *testing.T) {
	kc := &config.KafkaConfig{Brokers: "b:9092", ClientID: "orders-service"}

	for name, opts := range map[string][]config.PublisherOption{
		"no options":         nil,
		"NewPublisherConfig": config.NewPublisherConfig(),
	} {
		t.Run(name, func(t *testing.T) {
			pc := resolvePublisherConfig(kc, opts)
			c := pc.OverwriteSaramaConfig

			if c.Producer.RequiredAcks != sarama.WaitForAll || !c.Producer.Idempotent || c.Net.MaxOpenRequests != 1 {
				t.Fatalf("acks=%v idempotent=%v maxOpen=%d", c.Producer.RequiredAcks, c.Producer.Idempotent, c.Net.MaxOpenRequests)
			}

			if c.ClientID != "orders-service" {
				t.Fatalf("ClientID = %q", c.ClientID)
			}

			if err := c.Validate(); err != nil {
				t.Fatalf("default publisher config invalid: %v", err)
			}
		})
	}
}

// TestResolvePublisherConfig_ExplicitConfigNotOverridden — явный sarama.Config сервиса
// сохраняет свои acks/idempotent/MaxOpenRequests; ClientID "watermill" заменяется на
// KAFKA_CLIENT_ID без мутации конфига сервиса.
func TestResolvePublisherConfig_ExplicitConfigNotOverridden(t *testing.T) {
	kc := &config.KafkaConfig{Brokers: "b:9092", ClientID: "orders-service"}

	explicit := sarama.NewConfig()
	explicit.Producer.RequiredAcks = sarama.WaitForLocal
	explicit.Producer.Return.Successes = true
	explicit.ClientID = "watermill"

	pc := resolvePublisherConfig(kc, []config.PublisherOption{config.WithPublisherSaramaConfig(explicit)})
	c := pc.OverwriteSaramaConfig

	if c.Producer.RequiredAcks != sarama.WaitForLocal || c.Producer.Idempotent || c.Net.MaxOpenRequests != 5 {
		t.Fatalf("explicit config overridden: acks=%v idempotent=%v maxOpen=%d", c.Producer.RequiredAcks, c.Producer.Idempotent, c.Net.MaxOpenRequests)
	}

	if c.ClientID != "orders-service" || explicit.ClientID != "watermill" {
		t.Fatalf("ClientID resolved=%q service=%q", c.ClientID, explicit.ClientID)
	}
}

// TestDLTProducerConfig — DLT-продюсер берёт сеть/версию/ClientID subscriber-а и надёжные
// дефолты публикации.
func TestDLTProducerConfig(t *testing.T) {
	sub := sarama.NewConfig()
	sub.ClientID = "orders-service"
	sub.Net.SASL.Enable = true
	sub.Net.SASL.User = "u"

	c := dltProducerConfig(sub)

	if c.ClientID != "orders-service" || !c.Net.SASL.Enable || c.Net.SASL.User != "u" {
		t.Fatalf("network/ClientID not inherited: %+v", c.Net.SASL)
	}

	if c.Producer.RequiredAcks != sarama.WaitForAll || !c.Producer.Idempotent || c.Net.MaxOpenRequests != 1 || !c.Producer.Return.Successes {
		t.Fatal("DLT producer must be reliable sync producer")
	}

	if sub.Net.MaxOpenRequests != 5 {
		t.Fatalf("subscriber config mutated: MaxOpenRequests=%d", sub.Net.MaxOpenRequests)
	}

	old := sarama.NewConfig()
	old.Version = sarama.V0_10_2_0
	if c := dltProducerConfig(old); c.Producer.Idempotent {
		t.Fatal("idempotent producer must be disabled for Version < 0.11")
	}
}

// TestIsTopicAlreadyExists — DeclareTopics распознаёт «уже есть» по коду ошибки, не по тексту.
func TestIsTopicAlreadyExists(t *testing.T) {
	msg := "anything"
	if !isTopicAlreadyExists(&sarama.TopicError{Err: sarama.ErrTopicAlreadyExists, ErrMsg: &msg}) {
		t.Fatal("TopicError{ErrTopicAlreadyExists} must be recognized")
	}

	if !isTopicAlreadyExists(fmt.Errorf("wrap: %w", sarama.ErrTopicAlreadyExists)) {
		t.Fatal("wrapped ErrTopicAlreadyExists must be recognized")
	}

	if isTopicAlreadyExists(errors.New("Topic with this name already exists")) {
		t.Fatal("plain text must not be recognized")
	}

	if isTopicAlreadyExists(&sarama.TopicError{Err: sarama.ErrInvalidPartitions}) {
		t.Fatal("other errors must not be recognized")
	}
}
