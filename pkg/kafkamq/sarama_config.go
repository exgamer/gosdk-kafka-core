package kafkamq

import (
	"github.com/IBM/sarama"
	wmkafka "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

// Значения ClientID, которые не выбирал сервис: дефолт watermill-kafka
// (DefaultSarama*Config, помощники config.NewConsumer*Config) и дефолт sarama.NewConfig.
// Их заменяем на KAFKA_CLIENT_ID, явно заданный сервисом ClientID — не трогаем.
var defaultClientIDs = map[string]bool{"": true, "watermill": true, "sarama": true}

// withClientID применяет KAFKA_CLIENT_ID к итоговому sarama-конфигу (после всех опций).
// Конфиг сервиса не мутируется: при замене ClientID возвращается поверхностная копия.
func withClientID(cfg *sarama.Config, newDefault func() *sarama.Config, clientID string) *sarama.Config {
	if clientID == "" {
		return cfg
	}

	if cfg == nil {
		cfg = newDefault()
		cfg.ClientID = clientID

		return cfg
	}

	if !defaultClientIDs[cfg.ClientID] {
		return cfg
	}

	cp := *cfg
	cp.ClientID = clientID

	return &cp
}

// applyReliableProducerDefaults — надёжная публикация: подтверждение всеми ISR-репликами,
// идемпотентный продюсер (без дублей при ретраях), один запрос в полёте (порядок сохраняется).
func applyReliableProducerDefaults(cfg *sarama.Config) {
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.MaxOpenRequests = 1

	// Идемпотентность требует протокол >= 0.11; на более старом Version оставляем только acks.
	cfg.Producer.Idempotent = cfg.Version.IsAtLeast(sarama.V0_11_0_0)
	if cfg.Producer.Idempotent && cfg.Producer.Retry.Max < 1 {
		cfg.Producer.Retry.Max = 1
	}
}

// defaultPublisherSaramaConfig — дефолт SDK для publisher-а: sync-конфиг watermill-kafka
// плюс надёжная публикация. Применяется, только если сервис не передал свой sarama.Config.
func defaultPublisherSaramaConfig() *sarama.Config {
	cfg := wmkafka.DefaultSaramaSyncPublisherConfig()
	applyReliableProducerDefaults(cfg)

	return cfg
}

// resolvePublisherConfig — итоговый конфиг publisher-а: опции сервиса, затем дефолты SDK
// (только если sarama.Config не задан явно), затем KAFKA_CLIENT_ID.
func resolvePublisherConfig(kafkaCfg *config.KafkaConfig, opts []config.PublisherOption) wmkafka.PublisherConfig {
	pc := wmkafka.PublisherConfig{Brokers: kafkaCfg.BrokersList()}

	for _, opt := range opts {
		opt(&pc)
	}

	if pc.OverwriteSaramaConfig == nil {
		pc.OverwriteSaramaConfig = defaultPublisherSaramaConfig()
	}

	pc.OverwriteSaramaConfig = withClientID(pc.OverwriteSaramaConfig, defaultPublisherSaramaConfig, kafkaCfg.ClientID)

	return pc
}

// resolveSubscriberConfig — итоговый конфиг subscriber-а: опции сервиса, затем KAFKA_CLIENT_ID.
// sarama.Config всегда заполнен (тот же дефолт, что подставил бы watermill-kafka) — он же
// служит основой для DLT-продюсера.
func resolveSubscriberConfig(kafkaCfg *config.KafkaConfig, opts []config.ConsumerOption) wmkafka.SubscriberConfig {
	sc := wmkafka.SubscriberConfig{
		Brokers:       kafkaCfg.BrokersList(),
		ConsumerGroup: kafkaCfg.ConsumerGroup,
	}

	for _, opt := range opts {
		opt(&sc)
	}

	if sc.OverwriteSaramaConfig == nil {
		sc.OverwriteSaramaConfig = wmkafka.DefaultSaramaSubscriberConfig()
	}

	sc.OverwriteSaramaConfig = withClientID(sc.OverwriteSaramaConfig, wmkafka.DefaultSaramaSubscriberConfig, kafkaCfg.ClientID)

	return sc
}

// dltProducerConfig — конфиг DLT-продюсера: сеть (TLS/SASL/таймауты), версия протокола и
// ClientID берутся из конфига subscriber-а, поверх — надёжная sync-публикация.
func dltProducerConfig(sub *sarama.Config) *sarama.Config {
	cfg := wmkafka.DefaultSaramaSyncPublisherConfig()

	if sub != nil {
		cfg.Net = sub.Net
		cfg.Version = sub.Version
		cfg.ClientID = sub.ClientID
	}

	applyReliableProducerDefaults(cfg)

	return cfg
}
