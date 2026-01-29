package ingestion

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/events"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// UnifiedIngestionService orchestrates the ingestion of both Ledger and Payments events.
// It uses a single consumer to receive all events and routes them internally based on event type.
// This solves the NATS queue group message distribution issue where messages were being
// delivered to the wrong handler when using separate consumers.
type UnifiedIngestionService struct {
	consumer Consumer
	store    storage.TransactionStore
}

// UnifiedIngestionServiceConfig holds the configuration for UnifiedIngestionService.
type UnifiedIngestionServiceConfig struct{}

// NewUnifiedIngestionService creates a new UnifiedIngestionService.
// The consumer should be configured to consume from all relevant topics.
func NewUnifiedIngestionService(
	consumer Consumer,
	store storage.TransactionStore,
	config UnifiedIngestionServiceConfig,
) *UnifiedIngestionService {
	return &UnifiedIngestionService{
		consumer: consumer,
		store:    store,
	}
}

// Start begins the ingestion service. It blocks until the context is cancelled
// or a fatal error occurs.
func (s *UnifiedIngestionService) Start(ctx context.Context) error {
	return s.consumer.Start(ctx)
}

// Stop gracefully stops the ingestion service.
func (s *UnifiedIngestionService) Stop() {
	s.consumer.Stop()
}

// Health returns nil if the service is healthy, or an error describing the issue.
func (s *UnifiedIngestionService) Health() error {
	return s.consumer.Health()
}

// getConsumer returns the underlying GenericConsumer for health status reporting.
func (s *UnifiedIngestionService) getConsumer() *GenericConsumer {
	if gc, ok := s.consumer.(*GenericConsumer); ok {
		return gc
	}
	return nil
}

// UnifiedEventHandler implements EventHandler for processing both Ledger and Payments events.
// It routes events internally based on the event type field.
// All transactions are stored in OpenSearch via BatchBuffer.
type UnifiedEventHandler struct {
	store            storage.TransactionStore
	metrics          *IngestionMetrics
	consumer         *GenericConsumer
	batchBuffer      *BatchBuffer
	publisher        message.Publisher
	topic            string
	backfillExecutor *BackfillExecutor
}

// NewUnifiedEventHandler creates a new handler for both Ledger and Payments events.
// Note: This handler requires BatchBuffer to be set for proper operation.
func NewUnifiedEventHandler(
	store storage.TransactionStore,
	metrics *IngestionMetrics,
) *UnifiedEventHandler {
	return &UnifiedEventHandler{
		store:   store,
		metrics: metrics,
	}
}

// NewUnifiedEventHandlerWithBatchBuffer creates a new handler with batch buffer support.
func NewUnifiedEventHandlerWithBatchBuffer(
	store storage.TransactionStore,
	metrics *IngestionMetrics,
	batchBuffer *BatchBuffer,
	publisher message.Publisher,
	topic string,
) *UnifiedEventHandler {
	return &UnifiedEventHandler{
		store:       store,
		metrics:     metrics,
		batchBuffer: batchBuffer,
		publisher:   publisher,
		topic:       topic,
	}
}

// SetBackfillExecutor sets the backfill executor for the handler.
func (h *UnifiedEventHandler) SetBackfillExecutor(executor *BackfillExecutor) {
	h.backfillExecutor = executor
}

// SetBatchBuffer sets the batch buffer for the handler.
func (h *UnifiedEventHandler) SetBatchBuffer(buffer *BatchBuffer) {
	h.batchBuffer = buffer
}

// SetConsumer sets the consumer reference for updating lag timestamps.
func (h *UnifiedEventHandler) SetConsumer(consumer *GenericConsumer) {
	h.consumer = consumer
}

// rawEvent is used to peek at the event type before full parsing.
type rawEvent struct {
	Type string `json:"type"`
}

// Handle processes a single event by routing it to the appropriate handler based on event type.
func (h *UnifiedEventHandler) Handle(ctx context.Context, event Event) error {
	genericEvent, ok := event.(*GenericEvent)
	if !ok {
		logging.FromContext(ctx).Errorf("unexpected event type: %T", event)
		return nil // Skip non-generic events
	}

	// Peek at the event type to determine routing
	var raw rawEvent
	if err := json.Unmarshal(genericEvent.Payload, &raw); err != nil {
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"event_id": genericEvent.ID,
			"error":    err.Error(),
		}).Debug("failed to parse event type, skipping")
		return nil // Skip malformed events
	}

	// Route based on event type
	switch raw.Type {
	case LedgerEventTypeTransactionCreated:
		return h.handleLedgerEvent(ctx, genericEvent)
	case PaymentsEventTypeSavedPayment:
		return h.handlePaymentEvent(ctx, genericEvent)
	case events.EventTypeBackfill:
		return h.handleBackfillEvent(ctx, genericEvent)
	default:
		// Ignore other event types (AUDIT, etc.) - this is expected behavior
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"event_id":   genericEvent.ID,
			"event_type": raw.Type,
		}).Debug("ignoring non-relevant event type")
		return nil
	}
}

// handleLedgerEvent processes a ledger event (COMMITTED_TRANSACTIONS).
func (h *UnifiedEventHandler) handleLedgerEvent(ctx context.Context, genericEvent *GenericEvent) error {
	startTime := time.Now()

	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"event_id": genericEvent.ID,
		"source":   "ledger",
	})

	// Parse the raw event payload into a LedgerEvent
	var ledgerEvent LedgerEvent
	if err := json.Unmarshal(genericEvent.Payload, &ledgerEvent); err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("failed to unmarshal ledger event, skipping")
		h.recordMetrics(ctx, SideLedger, StatusError, startTime, time.Time{})
		return nil // Skip malformed events, don't retry
	}

	// Extract ledger name: try root level first, fallback to payload.ledger
	ledgerName := ledgerEvent.Ledger
	if ledgerName == "" {
		if ln, ok := ledgerEvent.Payload["ledger"].(string); ok {
			ledgerName = ln
			ledgerEvent.Ledger = ledgerName
		}
	}

	// Normalize the event into a transaction
	tx, err := NormalizeLedgerEvent(ctx, ledgerEvent)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error":       err.Error(),
			"event_type":  ledgerEvent.Type,
			"ledger_name": ledgerName,
		}).Error("failed to normalize ledger event, skipping")
		h.recordMetrics(ctx, SideLedger, StatusError, startTime, time.Time{})
		return nil
	}

	// If normalizer returns nil, the event is not relevant
	if tx == nil {
		logger.Debug("ledger event not relevant, skipping")
		h.recordMetrics(ctx, SideLedger, StatusSkipped, startTime, time.Time{})
		return nil
	}

	// Submit to batch buffer (handles both batch and single mode)
	if h.batchBuffer != nil && genericEvent.Message != nil {
		h.batchBuffer.Submit(ctx, genericEvent.Message, tx)
		return nil // Buffer will handle Ack/Nack
	}

	// Fallback: direct store (should not happen in normal operation)
	return h.persistTransactionDirect(ctx, logger, tx, SideLedger, startTime)
}

// handlePaymentEvent processes a payment event (SAVED_PAYMENT).
func (h *UnifiedEventHandler) handlePaymentEvent(ctx context.Context, genericEvent *GenericEvent) error {
	startTime := time.Now()

	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"event_id": genericEvent.ID,
		"source":   "payments",
	})

	// Parse the raw event payload into a PaymentsEvent
	var paymentsEvent PaymentsEvent
	if err := json.Unmarshal(genericEvent.Payload, &paymentsEvent); err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("failed to unmarshal payments event, skipping")
		h.recordMetrics(ctx, SidePayment, StatusError, startTime, time.Time{})
		return nil
	}

	// Normalize the event into a transaction
	tx, err := NormalizePaymentsEvent(ctx, paymentsEvent)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error":      err.Error(),
			"event_type": paymentsEvent.Type,
		}).Error("failed to normalize payments event, skipping")
		h.recordMetrics(ctx, SidePayment, StatusError, startTime, time.Time{})
		return nil
	}

	// If normalizer returns nil, the event is not relevant
	if tx == nil {
		logger.Debug("payments event not relevant, skipping")
		h.recordMetrics(ctx, SidePayment, StatusSkipped, startTime, time.Time{})
		return nil
	}

	// Submit to batch buffer (handles both batch and single mode)
	if h.batchBuffer != nil && genericEvent.Message != nil {
		h.batchBuffer.Submit(ctx, genericEvent.Message, tx)
		return nil // Buffer will handle Ack/Nack
	}

	// Fallback: direct store (should not happen in normal operation)
	return h.persistTransactionDirect(ctx, logger, tx, SidePayment, startTime)
}

// persistTransactionDirect handles direct persistence without batch buffer.
// This is a fallback for when batch buffer is not available.
func (h *UnifiedEventHandler) persistTransactionDirect(
	ctx context.Context,
	logger logging.Logger,
	tx *models.Transaction,
	metricsSide string,
	startTime time.Time,
) error {
	// Update logger with transaction details
	logger = logger.WithFields(map[string]interface{}{
		"transaction_id": tx.ID.String(),
		"external_id":    tx.ExternalID,
		"provider":       tx.Provider,
	})

	// Check for idempotence: skip if external_id already exists
	existing, err := h.store.ExistsByExternalIDs(ctx, tx.Side, []string{tx.ExternalID})
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("failed to check for existing transaction")
		h.recordMetrics(ctx, metricsSide, StatusError, startTime, tx.OccurredAt)
		return NewRetryableError(err)
	}

	if existing[tx.ExternalID] {
		logger.Debug("transaction already exists, skipping")
		h.recordMetrics(ctx, metricsSide, StatusSkipped, startTime, tx.OccurredAt)
		return nil
	}

	// Create the transaction in OpenSearch
	if err := h.store.Create(ctx, tx); err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("failed to create transaction")
		h.recordMetrics(ctx, metricsSide, StatusError, startTime, tx.OccurredAt)
		return NewRetryableError(err)
	}

	logger.Info("transaction created successfully")
	h.recordMetrics(ctx, metricsSide, StatusSuccess, startTime, tx.OccurredAt)
	h.publishTransactionIngested(ctx, tx)

	// Update last event timestamp for health check lag calculation
	if h.consumer != nil {
		h.consumer.SetLastEventTimestamp(time.Now())
	}

	return nil
}

// handleBackfillEvent processes a BACKFILL event by delegating to the BackfillExecutor.
func (h *UnifiedEventHandler) handleBackfillEvent(ctx context.Context, genericEvent *GenericEvent) error {
	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"event_id": genericEvent.ID,
		"source":   "backfill",
	})

	if h.backfillExecutor == nil {
		logger.Error("backfill executor not configured, skipping backfill event")
		return nil
	}

	// Parse the backfill event payload
	var backfillEvent struct {
		Type    string `json:"type"`
		Payload struct {
			BackfillID string `json:"backfillId"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(genericEvent.Payload, &backfillEvent); err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("failed to unmarshal backfill event, skipping")
		return nil
	}

	backfillID, err := uuid.Parse(backfillEvent.Payload.BackfillID)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"backfill_id": backfillEvent.Payload.BackfillID,
			"error":       err.Error(),
		}).Error("invalid backfill ID, skipping")
		return nil
	}

	logger.WithFields(map[string]interface{}{
		"backfill_id": backfillID.String(),
	}).Info("Processing backfill event")

	// Execute backfill in a goroutine to avoid blocking the consumer
	go func() {
		if err := h.backfillExecutor.Execute(ctx, backfillID); err != nil {
			logging.FromContext(ctx).WithFields(map[string]interface{}{
				"backfill_id": backfillID.String(),
				"error":       err.Error(),
			}).Error("Backfill execution failed")
		}
	}()

	return nil
}

// publishTransactionIngested publishes a TRANSACTION_INGESTED event for a successfully stored transaction.
func (h *UnifiedEventHandler) publishTransactionIngested(ctx context.Context, tx *models.Transaction) {
	if h.publisher == nil || h.topic == "" {
		return
	}

	event := events.NewTransactionIngestedEvent(tx)
	data, err := event.Marshal()
	if err != nil {
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"error":         err.Error(),
			"transactionId": tx.ID.String(),
		}).Error("Failed to marshal TRANSACTION_INGESTED event")
		return
	}

	msg := message.NewMessage(uuid.New().String(), data)
	msg.SetContext(ctx)

	if err := h.publisher.Publish(h.topic, msg); err != nil {
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"error":         err.Error(),
			"transactionId": tx.ID.String(),
			"topic":         h.topic,
		}).Error("Failed to publish TRANSACTION_INGESTED event")
	}
}

// recordMetrics records the ingestion metrics.
func (h *UnifiedEventHandler) recordMetrics(ctx context.Context, side, status string, startTime time.Time, eventTimestamp time.Time) {
	if h.metrics == nil {
		return
	}

	h.metrics.RecordEvent(ctx, side, status)
	h.metrics.RecordDuration(ctx, side, time.Since(startTime))

	if !eventTimestamp.IsZero() {
		h.metrics.RecordLag(ctx, side, eventTimestamp)
	}
}

// Ensure UnifiedEventHandler implements EventHandler interface.
var _ EventHandler = (*UnifiedEventHandler)(nil)
