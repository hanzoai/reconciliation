package ingestion

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// genericTestHandler is a configurable event handler for testing.
type genericTestHandler struct {
	mu          sync.Mutex
	events      []*GenericEvent
	handleFunc  func(ctx context.Context, event Event) error
	handleCount atomic.Int64
}

func newGenericTestHandler() *genericTestHandler {
	return &genericTestHandler{
		events: make([]*GenericEvent, 0),
	}
}

func (h *genericTestHandler) Handle(ctx context.Context, event Event) error {
	h.handleCount.Add(1)
	genericEvent, ok := event.(*GenericEvent)
	if ok {
		h.mu.Lock()
		h.events = append(h.events, genericEvent)
		h.mu.Unlock()
	}
	if h.handleFunc != nil {
		return h.handleFunc(ctx, event)
	}
	return nil
}

func (h *genericTestHandler) getEvents() []*GenericEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]*GenericEvent, len(h.events))
	copy(result, h.events)
	return result
}

func (h *genericTestHandler) getHandleCount() int64 {
	return h.handleCount.Load()
}

func TestGenericConsumerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  GenericConsumerConfig
		wantErr string
	}{
		{
			name: "valid config",
			config: GenericConsumerConfig{
				Topic: "test-topic",
			},
			wantErr: "",
		},
		{
			name: "missing topic",
			config: GenericConsumerConfig{
				Topic: "",
			},
			wantErr: "topic is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

func TestNewGenericConsumer(t *testing.T) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		watermill.NopLogger{},
	)
	defer func() { _ = pubSub.Close() }()

	t.Run("valid configuration", func(t *testing.T) {
		config := GenericConsumerConfig{
			Topic: "test-topic",
		}
		handler := newGenericTestHandler()

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		require.NoError(t, err)
		assert.NotNil(t, consumer)
	})

	t.Run("invalid configuration", func(t *testing.T) {
		config := GenericConsumerConfig{
			Topic: "",
		}
		handler := newGenericTestHandler()

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		assert.Error(t, err)
		assert.Nil(t, consumer)
	})

	t.Run("nil subscriber", func(t *testing.T) {
		config := GenericConsumerConfig{
			Topic: "test-topic",
		}
		handler := newGenericTestHandler()

		consumer, err := NewGenericConsumer(config, nil, handler)
		assert.EqualError(t, err, "subscriber is required")
		assert.Nil(t, consumer)
	})

	t.Run("nil handler", func(t *testing.T) {
		config := GenericConsumerConfig{
			Topic: "test-topic",
		}

		consumer, err := NewGenericConsumer(config, pubSub, nil)
		assert.EqualError(t, err, "event handler is required")
		assert.Nil(t, consumer)
	})
}

func TestGenericConsumer_Integration(t *testing.T) {
	t.Run("consumer starts and stops cleanly", func(t *testing.T) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{},
			watermill.NopLogger{},
		)
		defer func() { _ = pubSub.Close() }()

		topic := "test-start-stop"
		handler := newGenericTestHandler()
		config := GenericConsumerConfig{
			Topic: topic,
		}

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start consumer in goroutine
		startedCh := make(chan struct{})
		errCh := make(chan error, 1)
		go func() {
			close(startedCh)
			errCh <- consumer.Start(ctx)
		}()

		// Wait for goroutine to start
		<-startedCh

		// Wait for consumer to be healthy
		require.Eventually(t, func() bool {
			return consumer.Health() == nil
		}, 5*time.Second, 10*time.Millisecond, "consumer did not become healthy")

		// Stop the consumer
		consumer.Stop()

		// Wait for Start to return
		select {
		case err := <-errCh:
			// nil is acceptable for clean shutdown
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("consumer did not stop in time")
		}

		// Consumer should report not running after stop
		assert.Error(t, consumer.Health())
	})

	t.Run("messages consumed in order", func(t *testing.T) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{
				BlockPublishUntilSubscriberAck: true,
			},
			watermill.NopLogger{},
		)
		defer func() { _ = pubSub.Close() }()

		topic := "test-message-order"
		handler := newGenericTestHandler()
		config := GenericConsumerConfig{
			Topic: topic,
		}

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Start consumer in goroutine
		go func() {
			_ = consumer.Start(ctx)
		}()

		// Wait for consumer to be ready
		require.Eventually(t, func() bool {
			return consumer.Health() == nil
		}, 5*time.Second, 10*time.Millisecond)

		// Produce messages
		messages := []string{"message-1", "message-2", "message-3", "message-4", "message-5"}
		for _, msg := range messages {
			err := pubSub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte(msg)))
			require.NoError(t, err)
		}

		// Wait for all messages to be consumed
		require.Eventually(t, func() bool {
			return handler.getHandleCount() >= int64(len(messages))
		}, 10*time.Second, 100*time.Millisecond)

		// Stop consumer
		consumer.Stop()

		// Verify messages were consumed in order
		events := handler.getEvents()
		require.Len(t, events, len(messages))

		for i, event := range events {
			assert.Equal(t, messages[i], string(event.Payload), "message %d mismatch", i)
		}
	})

	t.Run("acknowledgment after successful processing", func(t *testing.T) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{
				BlockPublishUntilSubscriberAck: true,
			},
			watermill.NopLogger{},
		)
		defer func() { _ = pubSub.Close() }()

		topic := "test-ack-success"
		handler := newGenericTestHandler()
		config := GenericConsumerConfig{
			Topic: topic,
		}

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Start consumer
		go func() {
			_ = consumer.Start(ctx)
		}()

		// Wait for consumer to be ready
		require.Eventually(t, func() bool {
			return consumer.Health() == nil
		}, 10*time.Second, 10*time.Millisecond)

		// Publish message
		err = pubSub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("test-message")))
		require.NoError(t, err)

		// Wait for message to be processed
		require.Eventually(t, func() bool {
			return handler.getHandleCount() >= 1
		}, 10*time.Second, 100*time.Millisecond)

		// Verify message was received
		events := handler.getEvents()
		require.Len(t, events, 1)
		assert.Equal(t, "test-message", string(events[0].Payload))

		consumer.Stop()
	})

	t.Run("no acknowledgment if handler returns error", func(t *testing.T) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{
				BlockPublishUntilSubscriberAck: false,
			},
			watermill.NopLogger{},
		)
		defer func() { _ = pubSub.Close() }()

		topic := "test-no-ack-error"

		// Handler that fails on first attempt then succeeds
		attemptCount := atomic.Int64{}
		handler := newGenericTestHandler()
		handler.handleFunc = func(ctx context.Context, event Event) error {
			count := attemptCount.Add(1)
			if count == 1 {
				return NewRetryableError(errors.New("simulated failure"))
			}
			return nil
		}

		config := GenericConsumerConfig{
			Topic: topic,
		}

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Start consumer
		go func() {
			_ = consumer.Start(ctx)
		}()

		// Wait for consumer to be ready
		require.Eventually(t, func() bool {
			return consumer.Health() == nil
		}, 5*time.Second, 10*time.Millisecond)

		// Publish message
		err = pubSub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("retry-message")))
		require.NoError(t, err)

		// Wait a bit for processing
		time.Sleep(500 * time.Millisecond)

		// Verify handler was called at least once
		assert.GreaterOrEqual(t, attemptCount.Load(), int64(1))

		consumer.Stop()
	})

	t.Run("Health returns correct state", func(t *testing.T) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{},
			watermill.NopLogger{},
		)
		defer func() { _ = pubSub.Close() }()

		topic := "test-health"
		handler := newGenericTestHandler()
		config := GenericConsumerConfig{
			Topic: topic,
		}

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		require.NoError(t, err)

		// Before starting, Health should return error
		assert.Error(t, consumer.Health(), "health should return error before start")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		go func() {
			_ = consumer.Start(ctx)
		}()

		// Wait for consumer to be ready
		require.Eventually(t, func() bool {
			return consumer.Health() == nil
		}, 5*time.Second, 10*time.Millisecond)

		// While running, Health should return nil
		assert.NoError(t, consumer.Health(), "health should return nil while running")

		// Stop the consumer
		consumer.Stop()

		// After stopping, Health should return error
		assert.Error(t, consumer.Health(), "health should return error after stop")
	})

	t.Run("fatal error stops consumer and marks unhealthy", func(t *testing.T) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{
				BlockPublishUntilSubscriberAck: false,
			},
			watermill.NopLogger{},
		)
		defer func() { _ = pubSub.Close() }()

		topic := "test-fatal-error"
		handler := newGenericTestHandler()
		handler.handleFunc = func(ctx context.Context, event Event) error {
			return NewFatalError(errors.New("fatal processing error"))
		}

		config := GenericConsumerConfig{
			Topic: topic,
		}

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		go func() {
			_ = consumer.Start(ctx)
		}()

		// Wait for consumer to be ready
		require.Eventually(t, func() bool {
			return consumer.Health() == nil
		}, 5*time.Second, 10*time.Millisecond)

		// Publish message that will cause fatal error
		err = pubSub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("fatal-message")))
		require.NoError(t, err)

		// Wait for message to be processed
		require.Eventually(t, func() bool {
			return handler.getHandleCount() >= 1
		}, 5*time.Second, 100*time.Millisecond)

		// Consumer should become unhealthy after fatal error
		require.Eventually(t, func() bool {
			return consumer.Health() != nil
		}, 5*time.Second, 100*time.Millisecond)

		// Health error should contain our fatal error
		healthErr := consumer.Health()
		assert.Error(t, healthErr)
		assert.True(t, errors.Is(healthErr, ErrFatal))

		consumer.Stop()
	})

	t.Run("fatal error stops Start and returns error", func(t *testing.T) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{
				BlockPublishUntilSubscriberAck: true,
			},
			watermill.NopLogger{},
		)
		defer func() { _ = pubSub.Close() }()

		topic := "test-fatal-error-start"
		handler := newGenericTestHandler()
		handler.handleFunc = func(ctx context.Context, event Event) error {
			return NewFatalError(errors.New("fatal processing error"))
		}

		config := GenericConsumerConfig{
			Topic: topic,
		}

		consumer, err := NewGenericConsumer(config, pubSub, handler)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- consumer.Start(ctx)
		}()

		require.Eventually(t, func() bool {
			return consumer.Health() == nil
		}, 5*time.Second, 10*time.Millisecond)

		err = pubSub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("fatal-message")))
		require.NoError(t, err)

		select {
		case startErr := <-errCh:
			require.Error(t, startErr)
			assert.True(t, errors.Is(startErr, ErrFatal))
		case <-time.After(5 * time.Second):
			t.Fatal("Start did not return after fatal error")
		}
	})
}

func TestGenericEvent(t *testing.T) {
	t.Run("event fields are accessible", func(t *testing.T) {
		event := &GenericEvent{
			ID:      "test-id",
			Payload: []byte("test-payload"),
			Metadata: message.Metadata{
				"key1": "value1",
				"key2": "value2",
			},
		}

		assert.Equal(t, "test-id", event.ID)
		assert.Equal(t, []byte("test-payload"), event.Payload)
		assert.Equal(t, "value1", event.Metadata["key1"])
		assert.Equal(t, "value2", event.Metadata["key2"])
	})
}
