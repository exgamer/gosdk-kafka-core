package app

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	coreApp "github.com/exgamer/gosdk-core/pkg/app"
	coreDi "github.com/exgamer/gosdk-core/pkg/di"
	"github.com/exgamer/gosdk-kafka-core/pkg/config"
)

// TestKafkaKernel_Init_RegistriesGatedByEnableFlags — без EnableConsumer()/EnablePublisher()
// ни один из реестров не должен попадать в DI: иначе флаги ничего не переключают.
func TestKafkaKernel_Init_RegistriesGatedByEnableFlags(t *testing.T) {
	a := &coreApp.App{Container: coreDi.NewContainer()}

	k := NewKafkaKernel()

	if err := k.Init(a); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if _, err := coreDi.Resolve[*PublisherRegistry](a.Container); err == nil {
		t.Fatal("expected PublisherRegistry to NOT be registered without EnablePublisher()")
	}

	if _, err := coreDi.Resolve[*ConsumersRegistry](a.Container); err == nil {
		t.Fatal("expected ConsumersRegistry to NOT be registered without EnableConsumer()")
	}

	// Конфиг должен быть доступен всегда — нужен для kafkamq.DeclareTopics без consumer/publisher.
	if _, err := coreDi.Resolve[*config.KafkaConfig](a.Container); err != nil {
		t.Fatalf("expected KafkaConfig to always be registered: %v", err)
	}
}

// TestKafkaKernel_Init_EnabledRegistriesAreRegistered — с обоими флагами оба реестра
// должны быть резолвимы из DI.
func TestKafkaKernel_Init_EnabledRegistriesAreRegistered(t *testing.T) {
	a := &coreApp.App{Container: coreDi.NewContainer()}

	k := NewKafkaKernel().EnableConsumer().EnablePublisher()

	if err := k.Init(a); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if _, err := coreDi.Resolve[*PublisherRegistry](a.Container); err != nil {
		t.Fatalf("expected PublisherRegistry to be registered: %v", err)
	}

	if _, err := coreDi.Resolve[*ConsumersRegistry](a.Container); err != nil {
		t.Fatalf("expected ConsumersRegistry to be registered: %v", err)
	}
}

// TestKafkaKernel_Init_OnlyConsumerEnabled — включение одного флага не должно тянуть за
// собой реестр другого.
func TestKafkaKernel_Init_OnlyConsumerEnabled(t *testing.T) {
	a := &coreApp.App{Container: coreDi.NewContainer()}

	k := NewKafkaKernel().EnableConsumer()

	if err := k.Init(a); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if _, err := coreDi.Resolve[*ConsumersRegistry](a.Container); err != nil {
		t.Fatalf("expected ConsumersRegistry to be registered: %v", err)
	}

	if _, err := coreDi.Resolve[*PublisherRegistry](a.Container); err == nil {
		t.Fatal("expected PublisherRegistry to NOT be registered without EnablePublisher()")
	}
}

// TestKafkaKernel_Stop_WaitsForConsumeWithinCtx — Stop отменяет приём и ждёт завершения
// Consume (текущего handler-а), прежде чем закрывать publisher-ы и subscriber-ов.
func TestKafkaKernel_Stop_WaitsForConsumeWithinCtx(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	k := &KafkaKernel{ctx: runCtx, cancel: cancel, consumeDone: done}

	var finished atomic.Bool
	go func() {
		<-runCtx.Done()
		time.Sleep(100 * time.Millisecond) // текущий handler дорабатывает
		finished.Store(true)
		close(done)
	}()

	ctx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()

	if err := k.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if !finished.Load() {
		t.Fatal("Stop returned before consume finished")
	}
}

// TestKafkaKernel_Stop_TimeoutReturnsCtxErr — handler не уложился в ctx остановки:
// Stop не висит дольше ctx и возвращает его ошибку.
func TestKafkaKernel_Stop_TimeoutReturnsCtxErr(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	k := &KafkaKernel{cancel: cancel, consumeDone: make(chan struct{})} // никогда не закроется

	ctx, stopCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stopCancel()

	start := time.Now()
	err := k.Stop(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want DeadlineExceeded", err)
	}

	if time.Since(start) > 2*time.Second {
		t.Fatal("Stop exceeded its ctx")
	}
}
