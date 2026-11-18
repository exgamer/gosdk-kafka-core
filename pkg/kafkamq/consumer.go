package kafkamq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmkafka "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-core/pkg/errorreporter"
	"github.com/exgamer/gosdk-core/pkg/logger"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

// Consumer — helper для запуска нескольких подписчиков (handlers).
type Consumer struct {
	kafkaCfg *config.KafkaConfig

	mu       sync.RWMutex
	handlers []registration

	// closing — Close вызван: закрытие канала сообщений после него — штатная остановка.
	closing atomic.Bool

	logger watermill.LoggerAdapter

	// настройки поведения
	onError ErrorAction
	onPanic ErrorAction
	// sentryPayload — сколько байт payload класть в событие трекера ошибок: > 0 — не больше
	// стольких байт, 0 и меньше — не класть (по умолчанию). Меняется только явным WithSentryPayloadLimit.
	sentryPayload int
}

// registration — зарегистрированный handler и всё, что нужно для повторов и DLT.
type registration struct {
	cc            config.ConsumeConfig
	consumerGroup string
	dlt           dltSender
}

// ErrorAction определяет, что делать с сообщением при ошибке/панике.
type ErrorAction int

const (
	// ActionAck — подтвердить сообщение сразу (сообщение будет потеряно, повторов и DLT нет)
	ActionAck ErrorAction = iota
	// ActionNack — повторить обработку внутри консюмера (MaxHandleAttempts попыток с растущей
	// паузой 1с, 5с, 15с, 15с), затем опубликовать сообщение в <topic>.dlt и подтвердить —
	// партиция не стоит, сообщение не теряется. Пока DLT недоступен, сообщение не
	// подтверждается (публикация повторяется). Политика по умолчанию.
	ActionNack
)

func NewConsumer(kafkaCfg *config.KafkaConfig) (*Consumer, error) {
	if kafkaCfg == nil {
		return nil, errors.New("nil kafka config")
	}

	return &Consumer{
		kafkaCfg: kafkaCfg,
		logger:   newWatermillLogger(),
		onError:  ActionNack,
		onPanic:  ActionNack,
	}, nil
}

// WithErrorAction — политика при ошибке handler-а.
func (a *Consumer) WithErrorAction(action ErrorAction) *Consumer {
	a.onError = action

	return a
}

// WithPanicAction — политика при panic в handler-е.
func (a *Consumer) WithPanicAction(action ErrorAction) *Consumer {
	a.onPanic = action

	return a
}

// WithSentryPayloadLimit — явно включает payload сообщения в событие трекера ошибок:
// первые maxBytes байт. 0 и меньше — не отправлять (как по умолчанию: в payload бывают
// персональные данные и секреты). Смысл тот же, что в gosdk-rabbit-core.
func (a *Consumer) WithSentryPayloadLimit(maxBytes int) *Consumer {
	a.sentryPayload = maxBytes

	return a
}

// RegisterMultipleHandler — регистрация набора консьюмеров.
func (a *Consumer) RegisterMultipleHandler(ctx context.Context, handlers []config.HandlerRegister) error {
	for _, h := range handlers {
		if err := a.RegisterHandler(ctx, h.Handler, h.Topic, h.Config...); err != nil {
			logger.Error(ctx, "failed to register kafka handler: "+err.Error())

			return err
		}
	}

	return nil
}

// RegisterHandler — регистрация одного консьюмера на topic.
func (a *Consumer) RegisterHandler(ctx context.Context, handler config.Handler, topic string, cfg ...config.ConsumerOption) error {
	if topic == "" {
		return errors.New("kafka topic is empty")
	}

	// KAFKA_CLIENT_ID применяется после опций: помощники (NewConsumerGroupConfig и др.)
	// подставляют свой sarama.Config с ClientID="watermill" и раньше затирали его.
	sc := resolveSubscriberConfig(a.kafkaCfg, cfg)

	if sc.ConsumerGroup == "" {
		// Без consumer group Kafka не хранит committed offset для этого подписчика:
		// msg.Ack()/Nack() ничего не коммитят (sess == nil в watermill-kafka), и после
		// каждого реконнекта/рестарта сообщения будут вычитаны заново с offset-а,
		// заданного OverwriteSaramaConfig.Consumer.Offsets.Initial. Это осознанный режим
		// для отдельных сценариев (полное вычитывание compacted-топика и т.п.), но чаще
		// всего означает забытый config.WithConsumerGroup/NewConsumerFromOldestConfig.
		logger.Warning(ctx, "kafka consumer group is empty for topic="+topic+": Ack/Nack will not commit any offset, messages will be replayed on every reconnect")
	}

	subscriber, err := wmkafka.NewSubscriber(sc, a.logger)
	if err != nil {
		logger.Error(ctx, "failed to create kafka subscriber: "+err.Error())

		return err
	}

	a.mu.Lock()
	a.handlers = append(a.handlers, registration{
		cc: config.ConsumeConfig{
			Consumer: subscriber,
			Handler:  handler,
			Topic:    topic,
		},
		consumerGroup: sc.ConsumerGroup,
		dlt:           newLazyDLTProducer(sc.Brokers, sc.OverwriteSaramaConfig),
	})
	a.mu.Unlock()

	return nil
}

// snapshotHandlers возвращает копию зарегистрированных handlers под RLock — Consume и Close
// не должны читать поле handlers напрямую, чтобы не гонять его конкурентно с RegisterHandler.
func (a *Consumer) snapshotHandlers() []registration {
	a.mu.RLock()
	defer a.mu.RUnlock()

	out := make([]registration, len(a.handlers))
	copy(out, a.handlers)

	return out
}

// RunConsumers — удобный метод: регистрирует и запускает Consume.
// ВАЖНО: передавайте ctx, чтобы можно было остановить обработку и корректно закрыть подписчиков.
func (a *Consumer) RunConsumers(ctx context.Context, handlers []config.HandlerRegister) error {
	if err := a.RegisterMultipleHandler(ctx, handlers); err != nil {
		return err
	}

	return a.Consume(ctx)
}

// Consume — запускает все зарегистрированные handlers и блокируется до остановки.
//
// Остановка — отмена ctx: новые сообщения не берутся, текущий handler дорабатывает (его ctx
// не отменяется остановкой), его Ack успевает отметить offset; пауза между повторами
// прерывается без Ack — сообщение придёт снова после рестарта. Consume возвращается, когда
// все handler-ы завершились; nil — штатная остановка.
//
// Ошибка подписки, неожиданное закрытие канала сообщений или паника вне handler-а
// останавливают всех и возвращаются ошибкой (kernel в этом случае останавливает приложение).
//
// Обработка сообщения: handler ok => Ack; handler err/panic => по политике (см. ErrorAction).
func (a *Consumer) Consume(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}

	handlers := a.snapshotHandlers()
	if len(handlers) == 0 {
		return errors.New("no handlers registered")
	}

	// runCtx — приём сообщений: отменяется снаружи (остановка) или при первой ошибке.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// subCtx — подписка watermill-kafka. Живёт дольше runCtx: Ack текущего сообщения после
	// остановки должен дойти до сессии consumer group. Отменяется (defer), когда все
	// handler-ы уже завершились.
	subCtx, cancelSub := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSub()

	errCh := make(chan error, len(handlers))
	report := func(err error) {
		select {
		case errCh <- err:
		default:
		}
		cancel()
	}

	var wg sync.WaitGroup

	for _, reg := range handlers {
		wg.Add(1)

		go func(reg registration) {
			defer wg.Done()

			cc := reg.cc
			handlerName := getHandlerName(cc.Handler)

			// Защита от паники на уровне консьюмера (вне обработки конкретного msg)
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("consumer goroutine panic (handler=%s, topic=%s): %v", handlerName, cc.Topic, r)
					logger.Error(ctx, err.Error()+"\n"+string(debug.Stack()))
					a.captureSentry(ctx, err, handlerName, cc.Topic, nil, 0, true)
					report(err)
				}
			}()

			logger.Info(ctx, "Try to subscribe handler="+handlerName+" topic="+cc.Topic)

			msgChannel, err := cc.Consumer.Subscribe(subCtx, cc.Topic)
			if err != nil {
				wrapped := fmt.Errorf("failed to subscribe (handler=%s, topic=%s): %w", handlerName, cc.Topic, err)
				a.captureSentry(ctx, wrapped, handlerName, cc.Topic, nil, 0, false)
				report(wrapped)

				return
			}

			logger.Info(ctx, "Successfully subscribed handler="+handlerName+" topic="+cc.Topic)

			for {
				select {
				case <-runCtx.Done():
					logger.Info(ctx, "Context cancelled, stopping consumer handler="+handlerName)

					return

				case msg, ok := <-msgChannel:
					if !ok {
						if runCtx.Err() != nil || a.closing.Load() {
							logger.Info(ctx, "Message channel closed handler="+handlerName)

							return
						}

						err := fmt.Errorf("kafka message channel closed unexpectedly (handler=%s, topic=%s)", handlerName, cc.Topic)
						logger.Error(ctx, err.Error())
						report(err)

						return
					}

					if runCtx.Err() != nil {
						// остановка пришла одновременно с сообщением: не берём его в работу,
						// без Ack оно будет доставлено снова
						return
					}

					a.handleMessage(runCtx, reg, msg)
				}
			}
		}(reg)
	}

	// закрываем errCh, когда все горутины завершились
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// ждём завершения всех горутин; возвращаем первую ошибку (nil — штатная остановка)
	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// handleMessage обрабатывает одно сообщение с повторами. ctx — контекст приёма (отменяется
// остановкой): им прерываются паузы между попытками; сам handler получает ctx без отмены.
// ctx обработчика несёт id корреляции из заголовка correlation_id сообщения (нет — новый id; он же уходит в DLT).
func (a *Consumer) handleMessage(ctx context.Context, reg registration, msg *message.Message) {
	handlerName := getHandlerName(reg.cc.Handler)
	topic := reg.cc.Topic
	ctx = consumeCorrelation(ctx, msg)
	handlerCtx := context.WithoutCancel(ctx)
	delivery := newDeliveryInfo(topic, reg.consumerGroup, msg)

	for attempt := 1; ; attempt++ {
		start := time.Now()
		panicked, err := runHandler(handlerCtx, reg.cc.Handler, handlerName, msg)

		if err == nil {
			msg.Ack()

			return
		}

		elapsed := time.Since(start)

		action := a.onError
		if panicked {
			action = a.onPanic
		}

		if action == ActionAck {
			logger.Error(ctx, fmt.Sprintf("kafka handler failed, message acked by policy without retry: handler=%s topic=%s %s elapsed=%s err=%v",
				handlerName, topic, delivery.position(), elapsed, err))
			a.captureSentry(ctx, err, handlerName, topic, &delivery, attempt, panicked)
			msg.Ack()

			return
		}

		if attempt >= MaxHandleAttempts {
			logger.Error(ctx, fmt.Sprintf("kafka handler failed %d times, moving message to %s: handler=%s topic=%s %s err=%v",
				attempt, DLTTopic(topic), handlerName, topic, delivery.position(), err))
			a.captureSentry(ctx, err, handlerName, topic, &delivery, attempt, panicked)

			if !a.publishToDLT(ctx, reg, delivery, err, attempt) {
				return
			}

			msg.Ack()

			return
		}

		delay := retryDelay(attempt)
		logger.Warning(ctx, fmt.Sprintf("kafka handler failed, retry in %s: handler=%s topic=%s %s attempt=%d/%d elapsed=%s err=%v",
			delay, handlerName, topic, delivery.position(), attempt, MaxHandleAttempts, elapsed, err))

		if !sleepCtx(ctx, delay) {
			logger.Info(ctx, "kafka consumer is stopping during retry pause, message is not acked and will be redelivered: handler="+
				handlerName+" topic="+topic+" "+delivery.position())

			return
		}
	}
}

// runHandler вызывает handler, превращая панику в ошибку.
func runHandler(ctx context.Context, handler config.Handler, handlerName string, msg *message.Message) (panicked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			err = fmt.Errorf("panic in handler=%s: %v", handlerName, r)
			logger.Error(ctx, err.Error()+"\n"+string(debug.Stack()))
		}
	}()

	return false, handler(ctx, msg)
}

// publishToDLT публикует исходное сообщение в <topic>.dlt. Пока публикация не удалась —
// ERROR в лог и повтор с паузой; false — остановка во время повторов (сообщение не
// подтверждается и будет доставлено снова).
func (a *Consumer) publishToDLT(ctx context.Context, reg registration, d deliveryInfo, cause error, attempts int) bool {
	dltTopic := DLTTopic(d.topic)

	for try := 1; ; try++ {
		var err error
		if reg.dlt == nil {
			err = errors.New("dlt publisher is not configured")
		} else {
			// новое сообщение на каждую попытку: sarama меняет служебные поля ProducerMessage
			err = reg.dlt.Send(buildDLTMessage(d, cause, attempts, time.Now()))
		}

		if err == nil {
			logger.Warning(ctx, fmt.Sprintf("kafka message moved to %s: topic=%s %s attempts=%d", dltTopic, d.topic, d.position(), attempts))

			return true
		}

		logger.Error(ctx, fmt.Sprintf("failed to publish kafka message to %s, message is NOT acked, retrying: topic=%s %s try=%d err=%v",
			dltTopic, d.topic, d.position(), try, err))

		if !sleepCtx(ctx, retryDelay(try)) {
			logger.Error(ctx, fmt.Sprintf("kafka consumer is stopping, message was not published to %s and is not acked, it will be redelivered: topic=%s %s",
				dltTopic, d.topic, d.position()))

			return false
		}
	}
}

// captureSentry отправляет ошибку в error-трекер через errorreporter.Capture.
// Consumer ничего не знает про Sentry - реальную отправку делает адаптер,
// зарегистрированный через errorreporter.SetReporter (см. gosdk-sentry-core).
// Без него вызов безопасен и просто ничего не отправляет.
//
// Отправляются только безопасные метаданные: topic, partition, offset, длина и хеш ключа,
// номер попытки. Payload — только при явном WithSentryPayloadLimit; заголовки — никогда.
func (a *Consumer) captureSentry(ctx context.Context, err error, handlerName, topic string, d *deliveryInfo, attempt int, isPanic bool) {
	consumerCtx := map[string]any{
		"handler": handlerName,
		"panic":   isPanic,
		"topic":   topic,
	}

	if attempt > 0 {
		consumerCtx["attempt"] = attempt
	}

	if d != nil {
		consumerCtx["message_uuid"] = d.uuid

		if d.hasPartition {
			consumerCtx["partition"] = d.partition
		}

		if d.hasOffset {
			consumerCtx["offset"] = d.offset
		}

		consumerCtx["key_len"] = len(d.key)
		if len(d.key) > 0 {
			sum := sha256.Sum256(d.key)
			consumerCtx["key_sha256"] = hex.EncodeToString(sum[:8])
		}

		consumerCtx["payload_size"] = len(d.payload)

		if a.sentryPayload > 0 {
			payload := d.payload
			truncated := len(payload) > a.sentryPayload
			if truncated {
				payload = payload[:a.sentryPayload]
			}

			consumerCtx["payload"] = string(payload)
			consumerCtx["payload_truncated"] = truncated
		}
	}

	// Level: error для всех случаев - как и раньше (в исходном коде
	// scope.SetLevel не звался, sentry-go по умолчанию шлёт LevelError).
	errorreporter.Capture(ctx, err, errorreporter.Options{
		Level: errorreporter.LevelError,
		// handler тегом (не только в Extra) - чтобы фильтровать/группировать
		// issues в Sentry по конкретному консьюмеру, как endpoint для HTTP.
		Tags: map[string]string{
			"component": "kafka_consumer",
			"handler":   handlerName,
			"topic":     topic,
		},
		Extra: map[string]any{"consumer": consumerCtx},
	})
}

// Close закрывает всех зарегистрированных subscriber-ов и DLT-продюсеры. В отличие от Rabbit
// (где subscriber-ы делят общий connection, закрываемый kernel-ом), в Kafka каждый
// subscriber держит собственный sarama-клиент и должен закрываться явно.
//
// Close не ждёт текущий handler: для штатной остановки сначала отмените ctx Consume и
// дождитесь его возврата (так делает kernel), затем вызывайте Close.
func (a *Consumer) Close() error {
	a.closing.Store(true)

	var firstErr error

	regs := a.snapshotHandlers()

	for _, r := range regs {
		if r.cc.Consumer == nil {
			continue
		}

		if err := r.cc.Consumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, r := range regs {
		if r.dlt == nil {
			continue
		}

		if err := r.dlt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func getHandlerName(handler config.Handler) string {
	handlerName := reflect.TypeOf(handler).String()

	if reflect.TypeOf(handler).Kind() == reflect.Func {
		pc := reflect.ValueOf(handler).Pointer()
		if fn := runtime.FuncForPC(pc); fn != nil {
			handlerName = fn.Name()
		}
	}

	return handlerName
}
