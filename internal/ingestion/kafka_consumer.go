package ingestion

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// KafkaConsumerConfig holds the configuration for a Kafka consumer.
type KafkaConsumerConfig struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string
	// Topic is the Kafka topic to consume from.
	Topic string
	// GroupID is the consumer group ID.
	GroupID string
	// FromBeginning determines whether to start consuming from the beginning
	// of the topic (true) or from the latest offset (false).
	FromBeginning bool
}

// Validate validates the Kafka consumer configuration.
func (c *KafkaConsumerConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("at least one broker is required")
	}
	if c.Topic == "" {
		return errors.New("topic is required")
	}
	if c.GroupID == "" {
		return errors.New("group ID is required")
	}
	return nil
}

// KafkaConsumer implements the Consumer interface for Kafka.
type KafkaConsumer struct {
	config  KafkaConsumerConfig
	handler EventHandler

	client sarama.ConsumerGroup
	ready  chan bool

	mu            sync.RWMutex
	running       atomic.Bool
	healthy       atomic.Bool
	lastErr       error
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	wgInitialized atomic.Bool

	// Metrics
	eventsReceived  metric.Int64Counter
	eventsProcessed metric.Int64Counter
	eventsErrors    metric.Int64Counter
}

// NewKafkaConsumer creates a new Kafka consumer with the given configuration.
func NewKafkaConsumer(config KafkaConsumerConfig, handler EventHandler) (*KafkaConsumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if handler == nil {
		return nil, errors.New("event handler is required")
	}

	consumer := &KafkaConsumer{
		config:  config,
		handler: handler,
		ready:   make(chan bool),
	}

	if err := consumer.initMetrics(); err != nil {
		return nil, err
	}

	return consumer, nil
}

// initMetrics initializes the OpenTelemetry metrics for the consumer.
func (c *KafkaConsumer) initMetrics() error {
	meter := otel.Meter("github.com/formancehq/reconciliation/internal/ingestion")

	var err error
	c.eventsReceived, err = meter.Int64Counter(
		"kafka_consumer_events_received_total",
		metric.WithDescription("Total number of events received from Kafka"),
	)
	if err != nil {
		return err
	}

	c.eventsProcessed, err = meter.Int64Counter(
		"kafka_consumer_events_processed_total",
		metric.WithDescription("Total number of events successfully processed"),
	)
	if err != nil {
		return err
	}

	c.eventsErrors, err = meter.Int64Counter(
		"kafka_consumer_events_errors_total",
		metric.WithDescription("Total number of events that failed processing"),
	)
	if err != nil {
		return err
	}

	return nil
}

// Start begins consuming events from Kafka.
// It blocks until the context is cancelled or an error occurs.
func (c *KafkaConsumer) Start(ctx context.Context) error {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_6_0_0
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	if c.config.FromBeginning {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// Manual offset commit after successful processing
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = false

	client, err := sarama.NewConsumerGroup(c.config.Brokers, c.config.GroupID, saramaConfig)
	if err != nil {
		return err
	}
	c.client = client

	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()
	c.running.Store(true)
	c.healthy.Store(true)

	consumeErrCh := make(chan error, 1)
	c.wg.Add(1)
	c.wgInitialized.Store(true)
	go func() {
		defer c.wg.Done()
		for {
			// Reset ready channel for each rebalance
			c.mu.Lock()
			c.ready = make(chan bool)
			c.mu.Unlock()

			if err := c.client.Consume(ctx, []string{c.config.Topic}, c); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				c.mu.Lock()
				c.lastErr = err
				c.healthy.Store(false)
				c.mu.Unlock()
				select {
				case consumeErrCh <- err:
				default:
				}
			}

			// Check if context was cancelled
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Wait for consumer to be ready or context to be cancelled
	c.mu.RLock()
	ready := c.ready
	c.mu.RUnlock()
	select {
	case <-ready:
	case <-ctx.Done():
		c.running.Store(false)
		return c.client.Close()
	case err := <-consumeErrCh:
		c.running.Store(false)
		_ = c.client.Close()
		return err
	}

	// Block until context is done
	<-ctx.Done()

	c.running.Store(false)
	return c.client.Close()
}

// Stop gracefully stops the consumer.
func (c *KafkaConsumer) Stop() {
	c.running.Store(false)
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if c.wgInitialized.Load() {
		c.wg.Wait()
	}
}

// Health returns nil if the consumer is healthy, or an error describing
// the health issue.
func (c *KafkaConsumer) Health() error {
	if !c.running.Load() {
		return errors.New("consumer is not running")
	}
	if !c.healthy.Load() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.lastErr != nil {
			return c.lastErr
		}
		return errors.New("consumer is unhealthy")
	}
	return nil
}

// Setup is called at the beginning of a new session, before ConsumeClaim.
// Implements sarama.ConsumerGroupHandler.
func (c *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

// Cleanup is called at the end of a session, once all ConsumeClaim goroutines have exited.
// Implements sarama.ConsumerGroupHandler.
func (c *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes messages from a single partition.
// Implements sarama.ConsumerGroupHandler.
func (c *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			c.eventsReceived.Add(session.Context(), 1)

			// Create event from message
			event := &KafkaEvent{
				Topic:     message.Topic,
				Partition: message.Partition,
				Offset:    message.Offset,
				Key:       message.Key,
				Value:     message.Value,
				Timestamp: message.Timestamp,
				Headers:   convertHeaders(message.Headers),
			}

			// Handle the event
			err := c.handler.Handle(session.Context(), event)
			if err != nil {
				c.eventsErrors.Add(session.Context(), 1)

				// Check if error is fatal
				if errors.Is(err, ErrFatal) {
					c.mu.Lock()
					c.lastErr = err
					c.healthy.Store(false)
					c.mu.Unlock()
					return err
				}

				// For retryable errors, don't commit and continue
				// The message will be reprocessed after rebalance
				if errors.Is(err, ErrRetryable) {
					continue
				}

				// For other errors, treat as retryable (don't commit)
				continue
			}

			// Commit offset only after successful processing
			session.MarkMessage(message, "")
			session.Commit()
			c.eventsProcessed.Add(session.Context(), 1)

		case <-session.Context().Done():
			return nil
		}
	}
}

// KafkaEvent represents an event received from Kafka.
type KafkaEvent struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp interface{}
	Headers   map[string][]byte
}

// convertHeaders converts Sarama record headers to a map.
func convertHeaders(headers []*sarama.RecordHeader) map[string][]byte {
	result := make(map[string][]byte, len(headers))
	for _, h := range headers {
		result[string(h.Key)] = h.Value
	}
	return result
}

// Ensure KafkaConsumer implements Consumer interface.
var _ Consumer = (*KafkaConsumer)(nil)
