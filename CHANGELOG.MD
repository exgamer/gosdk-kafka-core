# Changelog

## v1.2.0

Ядовитое сообщение больше не останавливает партицию, остановка дожидается текущего handler-а,
публикация по умолчанию надёжная. Публичный API не сломан (код сервисов компилируется без
правок), новых переменных окружения и опций-переключателей нет. Из зависимостей — только gosdk-core v1.3.0
(CorrelationID).

### Добавлено
- Повторы обработки внутри консюмера и DLT: `kafkamq.MaxHandleAttempts`, `kafkamq.DLTSuffix`,
  `kafkamq.DLTTopic`, заголовки `kafkamq.DLTHeader*`.
- Сквозной CorrelationID: `Publisher.PublishCtx`, `PublishWithMetaDataCtx`, `PublishBatchCtx`,
  `PublishBatchWithMetaDataCtx` — публикация, ограниченная `ctx`; заголовок `correlation_id`
  (`CorrelationIDMetadataKey`) из `sdkctx.CorrelationID(ctx)` (gosdk-core), если вызывающий не задал свой; методы без
  `ctx` id не ставят. Обработчик получает `ctx` с id из заголовка `correlation_id` (нет — новый id); повторы и DLT
  несут тот же id. Требуется gosdk-core v1.3.0.

### Исправлено
- Ошибка/паника handler-а (`ActionNack`) вызывала бесконечную переотправку того же сообщения
  каждые 100 мс — партиция стояла. Теперь — до 5 попыток с паузой 1 с, 5 с, 15 с, 15 с, затем
  публикация в `<topic>.dlt` и Ack.
- `KAFKA_CLIENT_ID` затирался помощниками `NewConsumerGroupConfig`/`NewConsumerFromOldestConfig`/
  `NewConsumerFromNewestConfig` и `With*SaramaConfig` с `ClientID="watermill"` — теперь
  применяется после опций, если `ClientID` в итоговом конфиге пустой, `"watermill"` или `"sarama"`.
- Остановка отменяла ctx handler-а посреди обработки и закрывала subscriber-ов, не дожидаясь его:
  Ack после остановки не коммитился. Теперь Stop прекращает приём, ждёт текущий handler в
  пределах ctx остановки, затем закрывает publisher-ы и subscriber-ов.
- Падение `Consume` оставляло сервис работать без консюмеров («зомби») — теперь kernel
  останавливает приложение через `app.Fail`.
- `DeclareTopics` распознаёт существующий топик через `errors.Is(err, sarama.ErrTopicAlreadyExists)`,
  а не по тексту ошибки.

### ⚠️ Изменения поведения
- **Ядовитое сообщение уходит в `<topic>.dlt`** после 5 попыток (36 с пауз плюс время handler-а; всё
  это время партиция ждёт) и подтверждается. Ребаланс группы ждёт всю серию попыток (watermill ждёт
  Ack без ctx сессии); паузы рассчитаны с запасом меньше `Rebalance.Timeout` sarama по умолчанию (60 с).
  При превышении — повторная обработка сообщения и возможные дубли в DLT: handler должен быть
  идемпотентным. Нет топика `<topic>.dlt` (`UNKNOWN_TOPIC_OR_PARTITION`) — SDK один раз пытается
  создать его (число партиций и фактор репликации исходного топика; нужно право `CREATE`) и повторяет
  публикацию. Не удалось — сообщение не подтверждается, партиция стоит, в лог идёт ERROR
  `failed to publish kafka message to <topic>.dlt`; создайте топик заранее (`kafkamq.DeclareTopics`).
  DLT-продюсер — на брокерах и сетевом конфиге subscriber-а (TLS/SASL из `WithConsumerSaramaConfig`).
- `config.WithNackResendSleep` больше ни на что не влияет — SDK не вызывает Nack.
- Событие в Sentry — одно на сообщение (при уходе в DLT или при `ActionAck`), а не на каждую
  неудачную доставку; промежуточные неудачи — WARN в лог.
- **Sentry: payload и заголовки сообщения по умолчанию не отправляются**; в событии — handler,
  topic, partition, offset, `key_len`, `key_sha256` (первые 8 байт хеша), attempt, `payload_size`.
  Payload — только при явном `WithSentryPayloadLimit(n)` с `n > 0` (первые `n` байт). `0` и меньше —
  не отправлять (раньше `0` — целиком, без лимита; по умолчанию было 2 КБ), как в gosdk-rabbit-core.
- **Publisher без своего `sarama.Config`**: `RequiredAcks=WaitForAll` (было `WaitForLocal`),
  `Producer.Idempotent=true`, `Net.MaxOpenRequests=1`. Публикация чуть медленнее (ждёт все ISR);
  при `min.insync.replicas` больше числа живых реплик публикация падает ошибкой вместо тихой потери.
  На кластере с ACL-авторизацией и брокерами старше 2.8 продюсеру нужно право `IdempotentWrite`.
  Явный `WithPublisherSaramaConfig` не меняется.
- Handler получает ctx, который не отменяется остановкой (значения сохранены). Остановка ждёт
  handler в пределах таймаута остановки приложения; пауза между повторами прерывается сразу, без Ack.
- Publisher-ы закрываются после завершения handler-ов (раньше — сразу при Stop).
- Неожиданное закрытие канала сообщений subscriber-а (не после Stop/Close) — теперь ошибка
  `Consume`, а не тихий выход горутины.
- Ошибки handler-а пишутся через логгер `gosdk-core` (`[WARN]`/`[ERROR]`, со стеком для паники)
  вместо `log.Printf`.

## v1.1.1

Зависимость `gosdk-core` поднята до v1.2.0. API пакета не изменился.

## v1.1.0

Зависимость `gosdk-core` поднята до v1.1.0. API пакета не изменился.

### Изменено
- `gosdk-core` v1.0.29 → v1.1.0.

### ⚠️ Изменения поведения
- Требует `gosdk-core` v1.1.0: поднимать вместе с `gosdk-http-core` ≥ v1.2.0, иначе сервис теряет
  AppInfo (имя сервиса) в логах запросов и событиях Sentry. AppInfo читать через
  `context.GetAppInfoFromContext`.
- Логгер экранирует управляющие символы в сообщениях (многострочное сообщение - одна строка с `\n`),
  `logger.Dump` пишет значение одной строкой.

## v1.0.3

Повтор v1.0.2 под новым именем репозитория: `github.com/exgamer/gosdk-kafka-core` (было
`exgamer/github.com-exgamer-gosdk-kafka-core`, путь модуля не совпадал с репозиторием — `go get` не работал).
Код, API и зависимости — как в v1.0.2.

## v1.0.2

Минимальная версия Go поднята до 1.27.3. API и поведение пакета не изменились.

### Изменено
- `go.mod`: `go 1.26.0` → `go 1.27.3`. Причина - уязвимости стандартной библиотеки go1.26.0
  (`govulncheck`), исправлены в новых версиях Go. На go1.27.3 `govulncheck` уязвимостей стандартной библиотеки не находит.
- `gosdk-core` v1.0.28 → v1.0.29 (тоже `go 1.27.3`).

### ⚠️ Изменения поведения
- Потребителю нужен Go ≥ 1.27.3: после `go get` этой версии в его `go.mod` станет `go 1.27.3`.
  С `GOTOOLCHAIN=auto` toolchain скачается сам; с `GOTOOLCHAIN=local` на Go < 1.27.3 или в образе
  без доступа к `proxy.golang.org` сборка упадёт - образ сборки `golang:1.27.3`.

## v1.0.1

`KAFKA_*` из переменных окружения процесса (Docker, k8s, `docker run -e`) читаются
и без `.env`. Типы, поля и сигнатуры не изменились.

### Исправлено
- У полей `config.KafkaConfig` есть `json`-теги с теми же именами, что `mapstructure`
  (`json:"KAFKA_BROKERS"` и т. д.). Без них `InitConfig` из `gosdk-core` находил переменную
  в окружении, но не мог положить её в поле: применялись только ключи, которые есть в `.env`.

### Изменено
- `gosdk-core` поднят с v1.0.26 до v1.0.28: v1.0.27 - `InitConfig` приводит числа и флаги
  из окружения процесса к типу поля, v1.0.28 - `logger.Error` пишет на уровне Error.

### ⚠️ Изменения поведения
- Переменная `KAFKA_*`, которой нет в `.env`, но которая задана в окружении процесса,
  теперь применяется (раньше молча игнорировалась, поле оставалось пустым). Если `.env` нет совсем,
  конфиг целиком берётся из окружения. Для ключей, которые есть в `.env`, окружение было главнее
  и раньше.
- Пустая переменная в окружении (`KAFKA_CLIENT_ID=`) теперь перекрывает значение из `.env`
  пустой строкой (раньше бралось значение из `.env`). Не задавайте пустые `KAFKA_*`
  в окружении, если значение должно прийти из `.env`.
- `json.Marshal(KafkaConfig)` выдаёт ключи `KAFKA_*` вместо имён полей (`Brokers`, `ClientID`…).
  В SDK конфиг в JSON не сериализуется.
- Затронуты все места, где конфиг читается через `InitKafkaConfig`: `app.KafkaKernel`
  (`Init`) и сервисы, которые подключают этот kernel.
- Сообщения `logger.Error` (`gosdk-core` v1.0.28) видны при `LOG_LEVEL` `info` и выше с меткой
  `[ERROR]` (раньше - только при `trace`/`debug` с меткой `[DEBUG]`): остановка consumer
  в `KafkaKernel`, ошибка регистрации обработчика и создания подписчика в `kafkamq.Consumer`.
- Числовые и булевы поля других конфигов сервиса из окружения процесса разбираются по правилам
  `gosdk-core` v1.0.27; неверное значение - ошибка с именем переменной. У `KafkaConfig`
  все поля строковые, для него ничего не меняется.

## v0.1.0

- Первая версия `gosdk-kafka-core`: Kafka kernel по образцу `gosdk-rabbit-core`, поверх `ThreeDotsLabs/watermill-kafka` (sarama).
- `pkg/app`: `KafkaKernel`, `ConsumersRegistry`, `PublisherRegistry`.
- `pkg/kafkamq`: `Consumer` (consumer group, Ack/Nack, panic-safe, sentry-репортинг через `errorreporter`), `Publisher` (Publish/PublishBatch/PublishWithMetaData), `DeclareTopics`.
- `pkg/config`: `KafkaConfig` (env), функциональные опции `PublisherOption`/`ConsumerOption`, хелперы `NewConsumerFromOldestConfig`/`NewConsumerFromNewestConfig`, `NewPublisherConfig`/`NewPublisherWithPartitioningConfig`.
- `pkg/di`: хелперы `GetKafkaClient`, `GetKafkaConfig`, `GetKafkaConsumersRegistry`, `GetKafkaPublishersRegistry`.

### Правки по итогам ревью (до релиза)

- `KafkaConfig.ConsumerGroup`/`ClientID` теперь реально используются как дефолты в `kafkamq.Consumer.RegisterHandler`/`kafkamq.NewPublisher` (раньше были мёртвыми полями, только читались из env и нигде не применялись).
- `Consumer.RegisterHandler` пишет предупреждение в лог, если у handler-а нет consumer group (ни из env, ни из опции) — без группы `Ack`/`Nack` не коммитят offset.
- `KafkaKernel.EnableConsumer()`/`EnablePublisher()` теперь реально гейтят регистрацию `ConsumersRegistry`/`PublisherRegistry` в DI (раньше флаги ни на что не влияли в `Init()`, как и в `gosdk-rabbit-core`, откуда они унаследованы).
- `kafkamq.Consumer.handlers` защищён мьютексом (`RegisterHandler`/`Consume`/`Close` больше не гоняют один и тот же срез без синхронизации).
- `config.WithMarshaller` переименован в `config.WithMarshaler` для консистентности с `WithUnmarshaler`/`WithCustomMarshaler`/`WithCustomUnmarshaler`.
- `kafkamq.DeclareTopics` валидирует `TopicSpec` (`Name`/`NumPartitions`/`ReplicationFactor`) до открытия соединения с брокером, вместо того чтобы возвращать сырую ошибку брокера.
