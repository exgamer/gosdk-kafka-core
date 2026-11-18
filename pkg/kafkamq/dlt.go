package kafkamq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	wmkafka "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Повторы обработки сообщения внутри консюмера (политика ActionNack).
const (
	// MaxHandleAttempts — сколько раз handler вызывается для одного сообщения, прежде чем
	// оно уходит в DLT.
	MaxHandleAttempts = 5

	// Паузы 1, 5, 15, 15 с между пятью попытками - 36 с плюс время обработчика: с запасом меньше
	// Consumer.Group.Rebalance.Timeout sarama по умолчанию (60 с). Ребаланс ждёт всю серию попыток.
	retryStep1 = time.Second
	retryStep2 = 5 * time.Second
	retryStep3 = 15 * time.Second

	// DLTSuffix — суффикс топика недоставленных сообщений: <topic>.dlt.
	DLTSuffix = ".dlt"

	// maxDLTErrorLen — предел длины текста ошибки в заголовке DLT-сообщения.
	maxDLTErrorLen = 1024
)

// Заголовки, которые SDK добавляет к сообщению в DLT.
const (
	DLTHeaderOriginalTopic     = "x-dlt-original-topic"
	DLTHeaderOriginalPartition = "x-dlt-original-partition"
	DLTHeaderOriginalOffset    = "x-dlt-original-offset"
	DLTHeaderConsumerGroup     = "x-dlt-consumer-group"
	DLTHeaderError             = "x-dlt-error"
	DLTHeaderAttempts          = "x-dlt-attempts"
	DLTHeaderFailedAt          = "x-dlt-failed-at"
)

// retryBackoff — паузы между попытками (и между повторами публикации в DLT): после
// попытки N ждём retryBackoff[N-1], дальше последней ступени — последняя ступень.
// Переменная (а не константа) только ради тестов.
var retryBackoff = []time.Duration{retryStep1, retryStep2, retryStep3}

// retryDelay — пауза после попытки attempt (attempt начинается с 1).
func retryDelay(attempt int) time.Duration {
	if len(retryBackoff) == 0 {
		return 0
	}

	i := attempt - 1
	if i < 0 {
		i = 0
	}

	if i >= len(retryBackoff) {
		i = len(retryBackoff) - 1
	}

	return retryBackoff[i]
}

// sleepCtx ждёт d или отмену ctx; false — ctx отменён.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// DLTTopic — имя топика недоставленных сообщений для topic.
func DLTTopic(topic string) string {
	return topic + DLTSuffix
}

// deliveryInfo — снимок исходного сообщения до первой попытки: handler может менять
// msg.Payload/Metadata, а в DLT должно уйти то, что пришло из Kafka.
type deliveryInfo struct {
	topic         string
	consumerGroup string

	uuid     string
	key      []byte
	payload  []byte
	metadata map[string]string

	partition    int32
	hasPartition bool
	offset       int64
	hasOffset    bool
}

func newDeliveryInfo(topic, consumerGroup string, msg *message.Message) deliveryInfo {
	d := deliveryInfo{
		topic:         topic,
		consumerGroup: consumerGroup,
		uuid:          msg.UUID,
		payload:       append([]byte(nil), msg.Payload...),
		metadata:      make(map[string]string, len(msg.Metadata)),
	}

	for k, v := range msg.Metadata {
		d.metadata[k] = v
	}

	ctx := msg.Context()
	if key, ok := wmkafka.MessageKeyFromCtx(ctx); ok && key != nil {
		d.key = append([]byte(nil), key...)
	}

	d.partition, d.hasPartition = wmkafka.MessagePartitionFromCtx(ctx)
	d.offset, d.hasOffset = wmkafka.MessagePartitionOffsetFromCtx(ctx)

	return d
}

// buildDLTMessage — сообщение для <topic>.dlt: исходные ключ, payload и заголовки
// (как их записал бы DefaultMarshaler: UUID + metadata) плюс причина и число попыток.
func buildDLTMessage(d deliveryInfo, cause error, attempts int, now time.Time) *sarama.ProducerMessage {
	headers := make([]sarama.RecordHeader, 0, len(d.metadata)+8)

	if d.uuid != "" {
		headers = append(headers, sarama.RecordHeader{Key: []byte(wmkafka.UUIDHeaderKey), Value: []byte(d.uuid)})
	}

	keys := make([]string, 0, len(d.metadata))
	for k := range d.metadata {
		if isDLTHeader(k) || k == wmkafka.UUIDHeaderKey {
			// прежние x-dlt-* (повторная обработка из DLT) заменяются актуальными
			continue
		}

		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		headers = append(headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(d.metadata[k])})
	}

	reason := "unknown error"
	if cause != nil {
		reason = cause.Error()
	}

	if len(reason) > maxDLTErrorLen {
		reason = reason[:maxDLTErrorLen]
	}

	add := func(k, v string) {
		headers = append(headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}

	add(DLTHeaderOriginalTopic, d.topic)

	if d.hasPartition {
		add(DLTHeaderOriginalPartition, strconv.FormatInt(int64(d.partition), 10))
	}

	if d.hasOffset {
		add(DLTHeaderOriginalOffset, strconv.FormatInt(d.offset, 10))
	}

	if d.consumerGroup != "" {
		add(DLTHeaderConsumerGroup, d.consumerGroup)
	}

	add(DLTHeaderError, reason)
	add(DLTHeaderAttempts, strconv.Itoa(attempts))
	add(DLTHeaderFailedAt, now.UTC().Format(time.RFC3339Nano))

	pm := &sarama.ProducerMessage{
		Topic:   DLTTopic(d.topic),
		Value:   sarama.ByteEncoder(d.payload),
		Headers: headers,
	}

	if d.key != nil {
		pm.Key = sarama.ByteEncoder(d.key)
	}

	return pm
}

func isDLTHeader(k string) bool {
	switch k {
	case DLTHeaderOriginalTopic, DLTHeaderOriginalPartition, DLTHeaderOriginalOffset,
		DLTHeaderConsumerGroup, DLTHeaderError, DLTHeaderAttempts, DLTHeaderFailedAt:
		return true
	}

	return false
}

// dltSender публикует сообщение в DLT.
type dltSender interface {
	Send(msg *sarama.ProducerMessage) error
	Close() error
}

// lazyDLTProducer — sync-продюсер DLT на брокерах и сетевом конфиге subscriber-а.
// Создаётся при первом сообщении в DLT (без ядовитых сообщений лишних соединений нет);
// неудачное создание повторяется при следующей отправке. Нет топика DLT
// (UNKNOWN_TOPIC_OR_PARTITION) - одна попытка создать его на отправку, затем повтор публикации.
type lazyDLTProducer struct {
	brokers []string
	cfg     *sarama.Config

	// newProducer и declare - подменяются в тестах.
	newProducer func() (sarama.SyncProducer, error)
	declare     func(dltTopic string) error

	mu       sync.Mutex
	producer sarama.SyncProducer
	closed   bool
}

func newLazyDLTProducer(brokers []string, subscriberCfg *sarama.Config) *lazyDLTProducer {
	p := &lazyDLTProducer{
		brokers: append([]string(nil), brokers...),
		cfg:     dltProducerConfig(subscriberCfg),
	}

	p.newProducer = func() (sarama.SyncProducer, error) { return sarama.NewSyncProducer(p.brokers, p.cfg) }
	p.declare = p.declareDLT

	return p
}

func (p *lazyDLTProducer) Send(msg *sarama.ProducerMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("dlt producer is closed")
	}

	if p.producer == nil {
		producer, err := p.newProducer()
		if err != nil {
			return err
		}

		p.producer = producer
	}

	_, _, err := p.producer.SendMessage(msg)
	if !errors.Is(err, sarama.ErrUnknownTopicOrPartition) || p.declare == nil {
		return err
	}

	if declErr := p.declare(msg.Topic); declErr != nil {
		return fmt.Errorf("%w (declare %s: %v)", err, msg.Topic, declErr)
	}

	_, _, err = p.producer.SendMessage(msg)

	return err
}

// declareDLT Создаёт топик DLT с числом партиций и фактором репликации исходного топика.
func (p *lazyDLTProducer) declareDLT(dltTopic string) error {
	source := strings.TrimSuffix(dltTopic, DLTSuffix)

	client, err := sarama.NewClient(p.brokers, p.cfg)
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(source)
	if err != nil {
		return fmt.Errorf("partitions of %s: %w", source, err)
	}

	if len(partitions) == 0 {
		return fmt.Errorf("source topic %s has no partitions", source)
	}

	replicas, err := client.Replicas(source, partitions[0])
	if err != nil {
		return fmt.Errorf("replicas of %s: %w", source, err)
	}

	rf := int16(len(replicas))
	if rf < 1 {
		rf = 1
	}

	return DeclareTopics(p.brokers, p.cfg, TopicSpec{
		Name:              dltTopic,
		NumPartitions:     int32(len(partitions)),
		ReplicationFactor: rf,
	})
}

func (p *lazyDLTProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true

	if p.producer == nil {
		return nil
	}

	err := p.producer.Close()
	p.producer = nil

	return err
}

// position — partition/offset для логов (ключ и payload в лог не пишутся).
func (d deliveryInfo) position() string {
	p, o := "?", "?"
	if d.hasPartition {
		p = strconv.FormatInt(int64(d.partition), 10)
	}

	if d.hasOffset {
		o = strconv.FormatInt(d.offset, 10)
	}

	return "partition=" + p + " offset=" + o
}
