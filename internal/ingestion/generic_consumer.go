package ingestion

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// GenericConsumerConfig holds the configuration for a generic consumer
// that supports both NATS and Kafka via the go-libs publish package.
type GenericConsumerConfig struct {
	// Topic is the topic to consume from.
	Topic string
	// BatchMode controls whether the handler manages Ack/Nack.
	// When true, the consumer does not auto-Ack/Nack messages.
	// This is used with BatchBuffer for hybrid batch ingestion.
	BatchMode bool
}

// Validate validates the generic consumer configuration.
func (c *GenericConsumerConfig) Validate() error {
	if c.Topic == "" {
		return errors.New("topic is required")
	}
	return nil
}

// GenericConsumer implements the Consumer interface using Watermill's
// message.Subscriber, which supports both NATS and Kafka via go-libs publish.
type GenericConsumer struct {
	config     GenericConsumerConfig
	subscriber message.Subscriber
	handler    EventHandler

	mu                 sync.RWMutex
	running            atomic.Bool
	healthy            atomic.Bool
	lastErr            error
	lastEventTimestamp time.Time
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	wgInitialized      atomic.Bool

	// Metrics
	eventsReceived  metric.Int64Counter
	eventsProcessed metric.Int64Counter
	eventsErrors    metric.Int64Counter
}

// NewGenericConsumer creates a new generic consumer with the given configuration.
// The subscriber can be created using go-libs publish package for either NATS or Kafka.
func NewGenericConsumer(config GenericConsumerConfig, subscriber message.Subscriber, handler EventHandler) (*GenericConsumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if subscriber == nil {
		return nil, errors.New("subscriber is required")
	}

	if handler == nil {
		return nil, errors.New("event handler is required")
	}

	consumer := &GenericConsumer{
		config:     config,
		subscriber: subscriber,
		handler:    handler,
	}

	if err := consumer.initMetrics(); err != nil {
		return nil, err
	}

	return consumer, nil
}

// initMetrics initializes the OpenTelemetry metrics for the consumer.
func (c *GenericConsumer) initMetrics() error {
	meter := otel.Meter("github.com/formancehq/reconciliation/internal/ingestion")

	var err error
	c.eventsReceived, err = meter.Int64Counter(
		"events_received_total",
		metric.WithDescription("Total number of events received"),
	)
	if err != nil {
		return err
	}

	c.eventsProcessed, err = meter.Int64Counter(
		"events_processed_total",
		metric.WithDescription("Total number of events successfully processed"),
	)
	if err != nil {
		return err
	}

	c.eventsErrors, err = meter.Int64Counter(
		"events_errors_total",
		metric.WithDescription("Total number of events that failed processing"),
	)
	if err != nil {
		return err
	}

	return nil
}

// Start begins consuming events from the configured topic.
// It blocks until the context is cancelled or an error occurs.
func (c *GenericConsumer) Start(ctx context.Context) error {
	logger := logging.FromContext(ctx)

	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()
	c.running.Store(true)
	c.healthy.Store(true)

	logger.WithFields(map[string]interface{}{
		"topic": c.config.Topic,
	}).Info("Subscribing to topic")

	messages, err := c.subscriber.Subscribe(ctx, c.config.Topic)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"topic": c.config.Topic,
			"error": err.Error(),
		}).Error("Failed to subscribe to topic")

		c.running.Store(false)
		c.healthy.Store(false)
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}

	logger.WithFields(map[string]interface{}{
		"topic": c.config.Topic,
	}).Info("Successfully subscribed to topic, starting to consume messages")

	c.wg.Add(1)
	c.wgInitialized.Store(true)
	go func() {
		defer c.wg.Done()
		c.consumeMessages(ctx, messages)
	}()

	// Block until context is done
	<-ctx.Done()

	logger.WithFields(map[string]interface{}{
		"topic": c.config.Topic,
	}).Info("Consumer stopped")

	c.running.Store(false)
	return nil
}

// consumeMessages processes messages from the channel.
func (c *GenericConsumer) consumeMessages(ctx context.Context, messages <-chan *message.Message) {
	logger := logging.FromContext(ctx)

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return
			}

			c.eventsReceived.Add(ctx, 1)

			logger.WithFields(map[string]interface{}{
				"topic":      c.config.Topic,
				"message_id": msg.UUID,
			}).Debug("Received message from broker")

			// Create event from message
			event := &GenericEvent{
				ID:       msg.UUID,
				Payload:  msg.Payload,
				Metadata: msg.Metadata,
				Message:  msg, // Include the original message for batch mode
			}

			// Handle the event
			err := c.handler.Handle(ctx, event)

			// In batch mode, the handler (BatchBuffer) manages Ack/Nack
			if c.config.BatchMode {
				if err != nil {
					c.eventsErrors.Add(ctx, 1)
					// Log but don't Ack/Nack - the batch buffer handles this
					logger.WithFields(map[string]interface{}{
						"topic":      c.config.Topic,
						"message_id": msg.UUID,
						"error":      err.Error(),
					}).Debug("Error in batch mode handler")
				} else {
					c.eventsProcessed.Add(ctx, 1)
				}
				continue
			}

			// Non-batch mode: consumer manages Ack/Nack
			if err != nil {
				c.eventsErrors.Add(ctx, 1)

				// Check if error is fatal
				if errors.Is(err, ErrFatal) {
					logger.WithFields(map[string]interface{}{
						"topic":      c.config.Topic,
						"message_id": msg.UUID,
						"error":      err.Error(),
					}).Error("Fatal error processing message")

					c.mu.Lock()
					c.lastErr = err
					c.healthy.Store(false)
					c.mu.Unlock()
					// Nack the message - it will not be redelivered for fatal errors
					msg.Nack()
					return
				}

				// For retryable errors, Nack the message so it can be reprocessed
				if errors.Is(err, ErrRetryable) {
					logger.WithFields(map[string]interface{}{
						"topic":      c.config.Topic,
						"message_id": msg.UUID,
						"error":      err.Error(),
					}).Info("Retryable error processing message, will retry")
					msg.Nack()
					continue
				}

				// For other errors, treat as retryable (Nack)
				logger.WithFields(map[string]interface{}{
					"topic":      c.config.Topic,
					"message_id": msg.UUID,
					"error":      err.Error(),
				}).Info("Error processing message, will retry")
				msg.Nack()
				continue
			}

			// Acknowledge message only after successful processing
			msg.Ack()
			c.eventsProcessed.Add(ctx, 1)

			logger.WithFields(map[string]interface{}{
				"topic":      c.config.Topic,
				"message_id": msg.UUID,
			}).Debug("Message processed successfully")

		case <-ctx.Done():
			return
		}
	}
}

// Stop gracefully stops the consumer.
func (c *GenericConsumer) Stop() {
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
func (c *GenericConsumer) Health() error {
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

// SetLastEventTimestamp updates the last processed event timestamp.
func (c *GenericConsumer) SetLastEventTimestamp(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastEventTimestamp = t
}

// GetLastEventTimestamp returns the last processed event timestamp.
func (c *GenericConsumer) GetLastEventTimestamp() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastEventTimestamp
}

// GetLagSeconds returns the lag in seconds since the last event was processed.
// Returns 0 if no events have been processed yet.
func (c *GenericConsumer) GetLagSeconds() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lastEventTimestamp.IsZero() {
		return 0
	}
	lag := time.Since(c.lastEventTimestamp).Seconds()
	if lag < 0 {
		return 0
	}
	return lag
}

// IsRunning returns true if the consumer is currently running.
func (c *GenericConsumer) IsRunning() bool {
	return c.running.Load()
}

// GenericEvent represents an event received from the message broker.
type GenericEvent struct {
	ID       string
	Payload  []byte
	Metadata message.Metadata
	Message  *message.Message // Original message for batch mode Ack/Nack
}

// Ensure GenericConsumer implements Consumer interface.
var _ Consumer = (*GenericConsumer)(nil)
