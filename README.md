# gosdk-kafka-core

`gosdk-kafka-core` — Kafka kernel для приложений, построенных на базе `gosdk-core`.
Пакет инкапсулирует инициализацию consumer-а и publisher-а Kafka и подключается к
приложению как **kernel**. Архитектурно зеркалит `gosdk-rabbit-core` (те же
Kernel/Registry/DI паттерны), но поверх Watermill Kafka (sarama) вместо Watermill AMQP —
см. [отличия от gosdk-rabbit-core](#-отличия-от-gosdk-rabbit-core) ниже.

Основная цель — дать единый и предсказуемый способ поднятия Kafka-слоя поверх core SDK.

---

## 📦 Возможности

- 🌐 kafka kernel для `gosdk-core`
- 🚀 Инициализация kafka consumer (consumer group, offset policy)
- 🚀 Инициализация kafka publisher (с опциональным partition key)
- ⚙️ Конфигурация через config
- ♻️ Корректный shutdown
- 🧠 Интеграция с DI контейнером

---

## 🚀 Установка

```bash
go get github.com/exgamer/gosdk-kafka-core
```

---

[Что доступно в DI из коробки](pkg/di/container.go)

---

## 🧠 Концепция Kafka Kernel

Kafka kernel — это kernel приложения, который:
- регистрирует kafka зависимости в DI
- инициализирует kafka consumer
- инициализирует kafka publisher
- корректно завершает работу при shutdown

Kernel реализует интерфейс `KernelInterface` из `gosdk-core`.

---

## 🔌 Регистрация Kafka Kernel

```go
app.RegisterKernel(app.NewKafkaKernel().EnableConsumer().EnablePublisher()) // включаем консьюмер или паблишер в зависимости от необходимости
```

`EnableConsumer()`/`EnablePublisher()` — это не просто пометка "буду использовать": без
соответствующего вызова `ConsumersRegistry`/`PublisherRegistry` не попадают в DI, и
`di.GetKafkaConsumersRegistry`/`di.GetKafkaPublishersRegistry` вернут ошибку "not found".
`*config.KafkaConfig` при этом доступен в DI всегда — он нужен для `kafkamq.DeclareTopics`
даже когда ни consumer, ни publisher не подняты.

---

## ⚙️ Конфигурация

Kafka kernel использует конфигурацию из `pkg/config`.

Пример env-переменных:

```env
KAFKA_BROKERS=127.0.0.1:9092,127.0.0.1:9093
KAFKA_CONSUMER_GROUP=my-service
KAFKA_CLIENT_ID=my-service
```

`KAFKA_BROKERS` — список брокеров через запятую (см. `config.KafkaConfig.BrokersList`).
`KAFKA_CONSUMER_GROUP` — дефолтная consumer group для всех handler-ов, которые не задают
свою через `config.WithConsumerGroup`/`NewConsumerFromOldestConfig`. `KAFKA_CLIENT_ID` —
дефолтный sarama `ClientID` для publisher-а и consumer-а, если не переопределён через
`WithPublisherSaramaConfig`/`WithConsumerSaramaConfig`.

Переменные `KAFKA_*` (`config.KafkaConfig`) читаются из окружения процесса и из `.env`;
окружение главнее `.env`, пустая переменная в окружении перекрывает значение из `.env`.
Требуется `gosdk-core` v1.0.27+.

`KAFKA_CLIENT_ID` применяется после опций handler-а/publisher-а: помощники
`NewConsumerGroupConfig`/`NewConsumerFromOldestConfig`/`NewConsumerFromNewestConfig` и
`WithPublisherSaramaConfig`/`WithConsumerSaramaConfig` с `ClientID` из `""`, `"watermill"` или
`"sarama"` (дефолты библиотек) его больше не затирают. Свой `ClientID` в `sarama.Config` сервиса
не перебивается.

⚠️ Начальный offset без помощника: если handler не передаёт `NewConsumerFromOldestConfig`/
`NewConsumerFromNewestConfig`/`NewConsumerGroupConfig` (или свой `sarama.Config`), действует
дефолт sarama — `OffsetNewest`: новая группа (или группа, чей offset истёк по retention) начинает
с конца топика, и сообщения, записанные до первого запуска, не читаются. Чтобы не терять историю —
`NewConsumerFromOldestConfig`.

⚠️ Без consumer group (ни из `KAFKA_CONSUMER_GROUP`, ни из явной опции) Kafka не хранит
committed offset для подписчика: `msg.Ack()`/`msg.Nack()` ничего не коммитят, и после
каждого реконнекта сообщения будут вычитаны заново. `Consumer.RegisterHandler` в этом
случае пишет предупреждение в лог — но не запрещает такой режим (иногда он осознанно нужен,
например полное вычитывание compacted-топика).

---

## 🔧 Кастомный Marshaler и Sarama-конфиг

- `config.WithCustomMarshaler(m kafka.Marshaler)` / `config.WithCustomUnmarshaler(u kafka.Unmarshaler)` — своя реализация маршалинга (в отличие от `WithMarshaler`/`WithUnmarshaler`, принимающих только `kafka.DefaultMarshaler`).
- `config.WithPublisherSaramaConfig` / `config.WithConsumerSaramaConfig` — полная замена `*sarama.Config` (TLS, acks, retries, compression, session timeout и т.п.).
- Publisher без своего `sarama.Config` публикует надёжно: `RequiredAcks=WaitForAll`,
  `Producer.Idempotent=true`, `Net.MaxOpenRequests=1`. Свой `sarama.Config` из
  `WithPublisherSaramaConfig` берётся как есть — эти значения в нём SDK не меняет.
- `config.WithPartitioningMarshaler` — публикация с явным partition key, чтобы сохранить порядок сообщений в рамках одного ключа (см. [PUBLISHERREADME](pkg/kafkamq/PUBLISHERREADME.md)).

---

## 🗺️ Декларация топиков без consumer-а

Для топиков, у которых намеренно нет читателя в этом сервисе (например топик,
который только продюсируется, но требует нестандартных партиций/retention), не
нужно поднимать отдельный `sarama.NewClusterAdmin` в обход SDK:

```go
kafkaCfg, err := kafkaDi.GetKafkaConfig(a.Container)
if err != nil {
    return err
}

err = kafkamq.DeclareTopics(kafkaCfg.BrokersList(), nil, kafkamq.TopicSpec{
    Name:              "outbox-retry",
    NumPartitions:     6,
    ReplicationFactor: 3,
})
```

`kafkaDi.GetKafkaConfig` отдаёт тот же самый конфиг kernel-а (список брокеров), что
используется под consumer/publisher.

---

## 🔁 Повторы и DLT

Ошибка или паника handler-а (политика по умолчанию `ActionNack`) больше не приводит к
бесконечной переотправке того же сообщения. Консюмер SDK:

1. повторяет вызов handler-а до `kafkamq.MaxHandleAttempts` (5) раз с паузами 1 с, 5 с, 15 с, 15 с
   (партиция в это время ждёт — порядок сообщений сохраняется);
2. затем публикует исходное сообщение в топик `<topic>.dlt` (`kafkamq.DLTTopic(topic)`) на тех же
   брокерах и с тем же сетевым конфигом, что у subscriber-а, и подтверждает его — offset
   двигается, следующее сообщение партиции обрабатывается;
3. если топика `<topic>.dlt` нет (`UNKNOWN_TOPIC_OR_PARTITION`) — SDK один раз пытается создать его
   (`DeclareTopics`, число партиций и фактор репликации исходного топика) и повторяет публикацию;
4. если публикация в DLT не удалась — сообщение **не** подтверждается (не теряется), в лог пишется
   ERROR, публикация повторяется с паузой, пока не пройдёт или сервис не остановят.

Сообщение в DLT: исходные ключ, payload и заголовки (включая `_watermill_message_uuid`) плюс
`x-dlt-original-topic`, `x-dlt-original-partition`, `x-dlt-original-offset`,
`x-dlt-consumer-group`, `x-dlt-error` (текст ошибки, до 1024 байт), `x-dlt-attempts`,
`x-dlt-failed-at` (константы `kafkamq.DLTHeader*`).

Что нужно сделать сервису:
- создать `<topic>.dlt` заранее (`kafkamq.DeclareTopics`), если у сервиса нет права создавать топики
  (ACL `CREATE`) и на брокере выключено `auto.create.topics.enable` — иначе ядовитое сообщение
  остановит партицию с ERROR в логе «failed to publish kafka message to <topic>.dlt»;
- делать handler идемпотентным (см. ребаланс ниже);
- решить, кто читает DLT (алерт, ручной разбор, повторная отправка в исходный топик).

Детали:
- Ошибку в Sentry шлёт одно событие на сообщение — при уходе в DLT (или при `ActionAck`), с
  номером попытки; промежуточные неудачи — WARN в лог.
- Остановка сервиса во время паузы между попытками прерывает паузу без подтверждения: после
  рестарта сообщение придёт снова, счёт попыток начнётся заново.
- `kafkamq.ActionAck` (`WithErrorAction`/`WithPanicAction`) — как раньше: сразу подтвердить,
  без повторов и DLT.
- `config.WithNackResendSleep` больше ни на что не влияет: SDK не делает Nack.
- Пауза между попытками держит партицию. Ребаланс группы ждёт всю серию попыток, а не одну:
  watermill ждёт Ack сообщения без ctx сессии. Паузы (1 + 5 + 15 + 15 = 36 с) плюс время handler-а
  рассчитаны с запасом меньше `Consumer.Group.Rebalance.Timeout` sarama по умолчанию (60 с). Если
  серия дольше (медленный handler, свой меньший таймаут) — партиция уходит другому участнику,
  сообщение обрабатывается повторно, возможны дубли в DLT. Handler должен быть идемпотентным.

---

## 🧩 Работа с консьюмером

[Работа с консьюмером](pkg/kafkamq/CONSUMERREADME.md)


## 🧩 Работа с паблишером

---
[Работа с паблишером](pkg/kafkamq/PUBLISHERREADME.md)

---

## ♻️ Graceful Shutdown

Kafka kernel автоматически:
- прекращает приём новых сообщений;
- дожидается текущего handler-а в пределах таймаута остановки приложения (ctx handler-а
  остановкой не отменяется, его Ack успевает закоммитить offset); не уложился — закрытие всё
  равно выполняется, неподтверждённое сообщение придёт снова после рестарта;
- закрывает все publisher-ы (после handler-ов — те могли публиковать);
- закрывает всех зарегистрированных subscriber-ов (sarama-клиентов) и DLT-продюсеры.

Если `Consume` упал (ошибка подписки, неожиданное закрытие канала сообщений, паника вне
handler-а), kernel пишет ERROR и останавливает приложение через `app.Fail` — сервис без
консюмеров не остаётся работать «зомби».

---

## 🔄 Отличия от gosdk-rabbit-core

Kafka — не AMQP-брокер: нет exchange/queue/routing key, вместо них — topic +
partition + consumer group. Это меняет форму SDK в нескольких местах:

| gosdk-rabbit-core | gosdk-kafka-core | Причина |
|---|---|---|
| `pkg/rabbitmq` | `pkg/kafkamq` | имя пакета не пересекается с импортируемым `watermill-kafka/pkg/kafka`, как rabbit-core избегает конфликта с `pkg/amqp` |
| единый `config.Config` для consumer и publisher | раздельные `config.PublisherOption` / `config.ConsumerOption` | watermill-kafka использует разные структуры `PublisherConfig`/`SubscriberConfig`, в отличие от единого `amqp.Config` |
| `*amqp.ConnectionWrapper` — общее соединение в DI | `*config.KafkaConfig` — общий конфиг (брокеры) в DI | у Kafka нет постоянного соединения: каждый publisher/subscriber держит свой sarama-клиент |
| `enums.TopicExchange/DirectExchange/FanoutExchange` | `enums.OffsetOldest/OffsetNewest` | в Kafka нет типов exchange — есть начальный offset консьюмера |
| `rabbitmq.DeclareTopology` (declare на канале) | `kafkamq.DeclareTopics` (создание топиков через ClusterAdmin) | разные примитивы администрирования брокера |
| `Subscribe(ctx, "")` — topic не используется (задан в exchange/queue) | `Subscribe(ctx, topic)` — topic обязателен | в Kafka topic передаётся в Subscribe явно, поэтому `config.HandlerRegister`/`config.ConsumeConfig` хранят поле `Topic` |
| `Consumer` не требует явного `Close()` (шарит connection) | `Consumer.Close()` закрывает все subscriber-ы | каждый Kafka-subscriber держит собственный sarama-клиент и должен закрываться явно |
| `EnableConsumer()`/`EnablePublisher()` не влияют на регистрацию реестров в `Init()` (реестры создаются всегда) | `EnableConsumer()`/`EnablePublisher()` реально гейтят регистрацию `ConsumersRegistry`/`PublisherRegistry` в DI | в rabbit-core эти флаги нигде не читаются в `Init()` — оставлено как есть в исходном SDK, но в kafka-core исправлено, чтобы флаги не вводили в заблуждение |

---

## 📌 Используется вместе с

- `gosdk-core`
- internal business modules

---

## 📝 License

MIT или внутренняя лицензия компании
