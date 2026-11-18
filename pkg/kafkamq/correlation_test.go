package kafkamq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	sdkctx "github.com/exgamer/gosdk-core/pkg/context"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

func TestSetCorrelationID(t *testing.T) {
	ctx := sdkctx.WithCorrelationID(context.Background(), "corr-1")

	plain := message.NewMessage("1", nil)
	own := message.NewMessage("2", nil)
	own.Metadata.Set(CorrelationIDMetadataKey, "explicit")

	setCorrelationID(ctx, []*message.Message{plain, own})

	if got := plain.Metadata.Get(CorrelationIDMetadataKey); got != "corr-1" {
		t.Errorf("plain: correlation_id = %q, want corr-1", got)
	}

	if got := own.Metadata.Get(CorrelationIDMetadataKey); got != "explicit" {
		t.Errorf("explicit metadata: correlation_id = %q, want explicit (caller wins)", got)
	}

	none := message.NewMessage("3", nil)
	setCorrelationID(context.Background(), []*message.Message{none})

	if _, ok := none.Metadata[CorrelationIDMetadataKey]; ok {
		t.Errorf("correlation_id set without an id in ctx: %v", none.Metadata)
	}
}

// failingReg is a registration whose handler records the correlation id of every attempt and always fails.
func failingReg(ids *[]string, dlt *fakeDLT) registration {
	return registration{
		cc: config.ConsumeConfig{
			Topic: "orders",
			Handler: func(ctx context.Context, _ *message.Message) error {
				*ids = append(*ids, sdkctx.CorrelationID(ctx))

				return errors.New("bad payload")
			},
		},
		dlt: dlt,
	}
}

// The message's correlation_id reaches the handler in every attempt and the DLT copy.
func TestHandleMessage_CorrelationIDFromHeaderToDLT(t *testing.T) {
	withFakeReporter(t)
	setRetryBackoff(t, time.Millisecond)

	var ids []string

	dlt := &fakeDLT{}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`poison`))
	msg.Metadata.Set(CorrelationIDMetadataKey, "from-msg")

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	c.handleMessage(sdkctx.WithCorrelationID(context.Background(), "consumer-ctx"), failingReg(&ids, dlt), msg)

	for i, id := range ids {
		if id != "from-msg" {
			t.Fatalf("attempt %d: correlation id = %q, want from-msg", i+1, id)
		}
	}

	if dlt.sentCount() != 1 || headerMap(dlt.sent[0])[CorrelationIDMetadataKey] != "from-msg" {
		t.Fatalf("DLT correlation_id = %q, want from-msg", headerMap(dlt.sent[0])[CorrelationIDMetadataKey])
	}
}

// Without correlation_id one new id serves every attempt and goes to DLT.
func TestHandleMessage_CorrelationIDGeneratedKeptInDLT(t *testing.T) {
	withFakeReporter(t)
	setRetryBackoff(t, time.Millisecond)

	var ids []string

	dlt := &fakeDLT{}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`poison`))

	c := &Consumer{onError: ActionNack, onPanic: ActionNack}
	c.handleMessage(sdkctx.WithCorrelationID(context.Background(), "consumer-ctx"), failingReg(&ids, dlt), msg)

	if len(ids) != MaxHandleAttempts || ids[0] == "" || ids[0] == "consumer-ctx" {
		t.Fatalf("ids = %q, want %d attempts with a new id", ids, MaxHandleAttempts)
	}

	for i, id := range ids {
		if id != ids[0] {
			t.Fatalf("attempt %d: correlation id = %q, want %q", i+1, id, ids[0])
		}
	}

	if got := headerMap(dlt.sent[0])[CorrelationIDMetadataKey]; got != ids[0] {
		t.Fatalf("DLT correlation_id = %q, want %q", got, ids[0])
	}
}

// PublishCtx with a cancelled ctx returns the ctx error before publishing.
func TestPublishCtx_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := &Publisher{}

	if err := p.PublishCtx(ctx, "t", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishCtx = %v, want context.Canceled", err)
	}
}
