package ingestion

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

// testHandler is a configurable event handler for testing.
type testHandler struct {
	mu          sync.Mutex
	events      []*KafkaEvent
	handleFunc  func(ctx context.Context, event Event) error
	handleCount atomic.Int64
}

func newTestHandler() *testHandler {
	return &testHandler{
		events: make([]*KafkaEvent, 0),
	}
}

func (h *testHandler) Handle(ctx context.Context, event Event) error {
	h.handleCount.Add(1)
	kafkaEvent, ok := event.(*KafkaEvent)
	if ok {
		h.mu.Lock()
		h.events = append(h.events, kafkaEvent)
		h.mu.Unlock()
	}
	if h.handleFunc != nil {
		return h.handleFunc(ctx, event)
	}
	return nil
}

func (h *testHandler) getEvents() []*KafkaEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]*KafkaEvent, len(h.events))
	copy(result, h.events)
	return result
}

func (h *testHandler) getHandleCount() int64 {
	return h.handleCount.Load()
}

func TestKafkaConsumerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  KafkaConsumerConfig
		wantErr string
	}{
		{
			name: "valid config",
			config: KafkaConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
				GroupID: "test-group",
			},
			wantErr: "",
		},
		{
			name: "missing brokers",
			config: KafkaConsumerConfig{
				Brokers: []string{},
				Topic:   "test-topic",
				GroupID: "test-group",
			},
			wantErr: "at least one broker is required",
		},
		{
			name: "missing topic",
			config: KafkaConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "",
				GroupID: "test-group",
			},
			wantErr: "topic is required",
		},
		{
			name: "missing group ID",
			config: KafkaConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
				GroupID: "",
			},
			wantErr: "group ID is required",
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

func TestNewKafkaConsumer(t *testing.T) {
	t.Run("valid configuration", func(t *testing.T) {
		config := KafkaConsumerConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
			GroupID: "test-group",
		}
		handler := newTestHandler()

		consumer, err := NewKafkaConsumer(config, handler)
		require.NoError(t, err)
		assert.NotNil(t, consumer)
	})

	t.Run("invalid configuration", func(t *testing.T) {
		config := KafkaConsumerConfig{
			Brokers: []string{},
			Topic:   "test-topic",
			GroupID: "test-group",
		}
		handler := newTestHandler()

		consumer, err := NewKafkaConsumer(config, handler)
		assert.Error(t, err)
		assert.Nil(t, consumer)
	})

	t.Run("nil handler", func(t *testing.T) {
		config := KafkaConsumerConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
			GroupID: "test-group",
		}

		consumer, err := NewKafkaConsumer(config, nil)
		assert.EqualError(t, err, "event handler is required")
		assert.Nil(t, consumer)
	})
}

func setupKafkaContainer(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)

	cleanup := func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %v", err)
		}
	}

	return brokers[0], cleanup
}

func createTopic(t *testing.T, brokers []string, topic string, numPartitions int32) {
	t.Helper()

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin(brokers, config)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	}, false)
	require.NoError(t, err)
}

func produceMessages(t *testing.T, brokers []string, topic string, messages []string) {
	t.Helper()

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer(brokers, config)
	require.NoError(t, err)
	defer func() { _ = producer.Close() }()

	for _, msg := range messages {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		})
		require.NoError(t, err)
	}
}

func TestKafkaConsumer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	broker, cleanup := setupKafkaContainer(t)
	defer cleanup()

	brokers := []string{broker}

	t.Run("consumer starts and stops cleanly", func(t *testing.T) {
		topic := "test-start-stop"
		createTopic(t, brokers, topic, 1)

		handler := newTestHandler()
		config := KafkaConsumerConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: "test-group-start-stop",
		}

		consumer, err := NewKafkaConsumer(config, handler)
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
		}, 10*time.Second, 100*time.Millisecond, "consumer did not become healthy")

		// Stop the consumer
		consumer.Stop()

		// Wait for Start to return
		select {
		case err := <-errCh:
			// Error could be nil or ErrClosedConsumerGroup, both are acceptable
			if err != nil && !errors.Is(err, sarama.ErrClosedConsumerGroup) {
				t.Errorf("unexpected error: %v", err)
			}
		case <-time.After(10*time.Second):
			t.Fatal("consumer did not stop in time")
		}

		// Consumer should report not running after stop
		assert.Error(t, consumer.Health())
	})

	t.Run("messages consumed in order", func(t *testing.T) {
		topic := "test-message-order"
		createTopic(t, brokers, topic, 1)

		// Produce messages before starting consumer
		messages := []string{"message-1", "message-2", "message-3", "message-4", "message-5"}
		produceMessages(t, brokers, topic, messages)

		handler := newTestHandler()
		config := KafkaConsumerConfig{
			Brokers:       brokers,
			Topic:         topic,
			GroupID:       "test-group-order",
			FromBeginning: true,
		}

		consumer, err := NewKafkaConsumer(config, handler)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Start consumer in goroutine
		go func() {
			_ = consumer.Start(ctx)
		}()

		// Wait for all messages to be consumed
		require.Eventually(t, func() bool {
			return handler.getHandleCount() >= int64(len(messages))
		}, 20*time.Second, 100*time.Millisecond)

		// Stop consumer
		consumer.Stop()

		// Verify messages were consumed in order
		events := handler.getEvents()
		require.Len(t, events, len(messages))

		for i, event := range events {
			assert.Equal(t, messages[i], string(event.Value), "message %d mismatch", i)
		}

		// Verify offsets are sequential
		for i := 1; i < len(events); i++ {
			assert.Equal(t, events[i-1].Offset+1, events[i].Offset, "offsets not sequential")
		}
	})

	t.Run("offset committed after successful processing", func(t *testing.T) {
		topic := "test-offset-commit"
		createTopic(t, brokers, topic, 1)

		// Produce messages
		messages := []string{"msg-a", "msg-b", "msg-c"}
		produceMessages(t, brokers, topic, messages)

		groupID := "test-group-offset-commit"

		// First consumer consumes all messages
		handler1 := newTestHandler()
		config := KafkaConsumerConfig{
			Brokers:       brokers,
			Topic:         topic,
			GroupID:       groupID,
			FromBeginning: true,
		}

		consumer1, err := NewKafkaConsumer(config, handler1)
		require.NoError(t, err)

		ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel1()

		go func() {
			_ = consumer1.Start(ctx1)
		}()

		// Wait for all messages
		require.Eventually(t, func() bool {
			return handler1.getHandleCount() >= int64(len(messages))
		}, 20*time.Second, 100*time.Millisecond)

		// Stop first consumer (offsets should be committed)
		consumer1.Stop()
		time.Sleep(1 * time.Second) // Allow time for cleanup

		// Start second consumer with same group ID
		handler2 := newTestHandler()
		consumer2, err := NewKafkaConsumer(config, handler2)
		require.NoError(t, err)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel2()

		go func() {
			_ = consumer2.Start(ctx2)
		}()

		// Wait a bit
		time.Sleep(3 * time.Second)
		consumer2.Stop()

		// Second consumer should not have received any messages
		// because offsets were committed by first consumer
		events2 := handler2.getEvents()
		assert.Empty(t, events2, "second consumer should not receive already committed messages")
	})

	t.Run("offset not committed if handler returns error", func(t *testing.T) {
		topic := "test-offset-no-commit-error"
		createTopic(t, brokers, topic, 1)

		// Produce messages
		messages := []string{"error-msg-1", "error-msg-2"}
		produceMessages(t, brokers, topic, messages)

		groupID := "test-group-offset-no-commit"

		// First consumer fails on all messages
		failCount := atomic.Int64{}
		handler1 := newTestHandler()
		handler1.handleFunc = func(ctx context.Context, event Event) error {
			failCount.Add(1)
			return NewRetryableError(errors.New("simulated failure"))
		}

		config := KafkaConsumerConfig{
			Brokers:       brokers,
			Topic:         topic,
			GroupID:       groupID,
			FromBeginning: true,
		}

		consumer1, err := NewKafkaConsumer(config, handler1)
		require.NoError(t, err)

		ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel1()

		go func() {
			_ = consumer1.Start(ctx1)
		}()

		// Wait for some processing attempts
		time.Sleep(3 * time.Second)
		consumer1.Stop()

		// Start second consumer - should receive the same messages
		// because offsets were not committed due to errors
		handler2 := newTestHandler()
		consumer2, err := NewKafkaConsumer(config, handler2)
		require.NoError(t, err)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel2()

		go func() {
			_ = consumer2.Start(ctx2)
		}()

		// Wait for messages
		require.Eventually(t, func() bool {
			return handler2.getHandleCount() >= int64(len(messages))
		}, 20*time.Second, 100*time.Millisecond)

		consumer2.Stop()

		// Second consumer should have received messages
		events2 := handler2.getEvents()
		assert.GreaterOrEqual(t, len(events2), len(messages), "second consumer should receive uncommitted messages")
	})

	t.Run("Health returns correct state", func(t *testing.T) {
		topic := "test-health"
		createTopic(t, brokers, topic, 1)

		handler := newTestHandler()
		config := KafkaConsumerConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: "test-group-health",
		}

		consumer, err := NewKafkaConsumer(config, handler)
		require.NoError(t, err)

		// Before starting, Health should return error
		assert.Error(t, consumer.Health(), "health should return error before start")

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		go func() {
			_ = consumer.Start(ctx)
		}()

		// Wait for consumer to be ready
		time.Sleep(3 * time.Second)

		// While running, Health should return nil
		assert.NoError(t, consumer.Health(), "health should return nil while running")

		// Stop the consumer
		consumer.Stop()

		// After stopping, Health should return error
		assert.Error(t, consumer.Health(), "health should return error after stop")
	})
}

func TestConvertHeaders(t *testing.T) {
	t.Run("empty headers", func(t *testing.T) {
		result := convertHeaders(nil)
		assert.Empty(t, result)
	})

	t.Run("multiple headers", func(t *testing.T) {
		headers := []*sarama.RecordHeader{
			{Key: []byte("content-type"), Value: []byte("application/json")},
			{Key: []byte("correlation-id"), Value: []byte("12345")},
		}
		result := convertHeaders(headers)
		assert.Len(t, result, 2)
		assert.Equal(t, []byte("application/json"), result["content-type"])
		assert.Equal(t, []byte("12345"), result["correlation-id"])
	})
}
