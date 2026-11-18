## 🧩 Работа с консьюмером

Консьюмеры регистрируются в DI и доступны в бизнес-модулях.

```go
package consumers

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
)

func NewCityConsumer() *CityConsumer {
	return &CityConsumer{}
}

type CityConsumer struct {
}

func (c *CityConsumer) Consume(ctx context.Context, msg *message.Message) error {
	fmt.Println(string(msg.Payload))

	return nil
}
```

```go
package city

import (
	"github.com/exgamer/kafka-consumer-template/internal/domains/handbbok/modules/city/factories"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

func GetConsumers(
	consumersFactory *factories.CityConsumersFactory,
) []config.HandlerRegister {
	return []config.HandlerRegister{
		{
			Handler: consumersFactory.CityConsumer.Consume,
			Topic:   "cities",
			Config:  config.NewConsumerFromOldestConfig("city-service"),
		},
	}
}
```

```go
consumersFactory := factories.NewCityConsumersFactory()

consumers := GetConsumers(consumersFactory)

reg, err := di.GetKafkaConsumersRegistry(a.Container) // твой helper для DI
if err != nil {
    return err
}

reg.RegisterMultipleHandler(consumers)
```

---

## Consumer Group и offset

В отличие от Rabbit (где логика доставки определяется exchange/queue), в Kafka
поведение консьюмера определяется двумя вещами:

- **Consumer Group** (`config.WithConsumerGroup`) — несколько инстансов сервиса
  с одинаковой группой делят партиции топика между собой (горизонтальное
  масштабирование, каждое сообщение обрабатывается ровно одним инстансом).
- **Initial offset** (`config.NewConsumerFromOldestConfig` / `NewConsumerFromNewestConfig`) —
  откуда читать при первом запуске группы (когда ещё нет закоммиченного offset).

```go
config.NewConsumerFromOldestConfig("city-service") // не терять историю при первом деплое
config.NewConsumerFromNewestConfig("live-metrics")  // забирать только новые события
```

Если конкретный handler не передаёт свою группу через `Config`, используется дефолтная
`KAFKA_CONSUMER_GROUP` из env (`KafkaConfig.ConsumerGroup`). Если группа не задана ни там,
ни там — `RegisterHandler` всё равно зарегистрирует handler, но напишет в лог
предупреждение: **без consumer group `msg.Ack()`/`msg.Nack()` не коммитят offset**, и
сообщения будут вычитываться заново при каждом реконнекте/рестарте. Это осознанный режим
для отдельных сценариев (полное вычитывание compacted-топика и т.п.), но почти всегда
означает забытую группу.

---

## Id корреляции

`ctx` handler-а несёт сквозной id корреляции (`sdkctx.CorrelationID(ctx)`, gosdk-core) из заголовка `correlation_id`
сообщения; нет — новый id, он же уходит в DLT. Все попытки одного сообщения — с одним id; публикации handler-а через
`...Ctx(ctx, ...)` передают его дальше.

## Ошибки handler-а: повторы и DLT

Handler вернул ошибку или запаниковал — по умолчанию (`ActionNack`) SDK повторяет вызов до 5 раз
с паузами 1 с, 5 с, 15 с, 15 с, затем публикует сообщение в `<topic>.dlt` (нет топика — один раз
пытается создать) и подтверждает его. Пока DLT недоступен, сообщение не подтверждается. Ребаланс
ждёт всю серию попыток; при превышении `Rebalance.Timeout` возможна повторная обработка и дубли
в DLT — handler должен быть идемпотентным. Подробно — [README: Повторы и DLT](../../README.md#-повторы-и-dlt).

```go
consumer.WithErrorAction(kafkamq.ActionAck) // бизнес-ошибки не повторять и не слать в DLT (сообщение теряется)
consumer.WithPanicAction(kafkamq.ActionNack) // по умолчанию: повторы, затем DLT
```

Handler получает ctx, который не отменяется остановкой сервиса посреди обработки (значения
контекста сохраняются). Долгие операции ограничивайте своими таймаутами.

Payload сообщения в Sentry по умолчанию не уходит (только topic, partition, offset, длина и хеш
ключа, номер попытки); включить явно — `consumer.WithSentryPayloadLimit(n)` с `n > 0` (первые `n` байт),
`0` и меньше — не отправлять.
