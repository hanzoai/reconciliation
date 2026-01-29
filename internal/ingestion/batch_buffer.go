package ingestion

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/events"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// BatchBufferMeterName is the name of the meter used for batch buffer metrics.
	BatchBufferMeterName = "github.com/formancehq/reconciliation/internal/ingestion/batch_buffer"

	// Environment variable names for batch buffer configuration.
	BatchBufferSizeEnvVar      = "BATCH_BUFFER_SIZE"
	BatchFlushIntervalMSEnvVar = "BATCH_FLUSH_INTERVAL_MS"
	BatchEnabledEnvVar         = "BATCH_ENABLED"

	// Default values for batch buffer configuration.
	DefaultBatchBufferSize      = 100
	DefaultBatchFlushIntervalMS = 200
	DefaultBatchEnabled         = true

	// Flush trigger constants.
	FlushTriggerSize     = "size"
	FlushTriggerTimer    = "timer"
	FlushTriggerShutdown = "shutdown"
)

// BatchBufferConfig holds the configuration for the batch buffer.
type BatchBufferConfig struct {
	// BufferSize is the maximum number of items to accumulate before flushing.
	BufferSize int
	// FlushInterval is the maximum time to wait before flushing.
	FlushInterval time.Duration
	// Enabled controls whether batch mode is active.
	Enabled bool
}

// BatchBufferConfigFromEnv creates a BatchBufferConfig from environment variables.
func BatchBufferConfigFromEnv() BatchBufferConfig {
	config := BatchBufferConfig{
		BufferSize:    DefaultBatchBufferSize,
		FlushInterval: time.Duration(DefaultBatchFlushIntervalMS) * time.Millisecond,
		Enabled:       DefaultBatchEnabled,
	}

	if sizeStr := os.Getenv(BatchBufferSizeEnvVar); sizeStr != "" {
		if size, err := strconv.Atoi(sizeStr); err == nil && size > 0 {
			config.BufferSize = size
		}
	}

	if intervalStr := os.Getenv(BatchFlushIntervalMSEnvVar); intervalStr != "" {
		if intervalMS, err := strconv.Atoi(intervalStr); err == nil && intervalMS > 0 {
			config.FlushInterval = time.Duration(intervalMS) * time.Millisecond
		}
	}

	if enabledStr := os.Getenv(BatchEnabledEnvVar); enabledStr != "" {
		config.Enabled = enabledStr == "true" || enabledStr == "1"
	}

	return config
}

// BatchItem represents a single item in the batch buffer.
// It pairs a message with its normalized transaction for Ack/Nack handling.
type BatchItem struct {
	Message     *message.Message
	Transaction *models.Transaction
}

// BatchBufferMetrics holds the OpenTelemetry metrics for the batch buffer.
type BatchBufferMetrics struct {
	// flushTotal counts the total number of flush operations.
	flushTotal metric.Int64Counter
	// flushSize tracks the size of batches when flushed.
	flushSize metric.Int64Histogram
	// flushDuration tracks the duration of flush operations by stage.
	flushDuration metric.Float64Histogram
	// ackTotal counts the number of messages Acked.
	ackTotal metric.Int64Counter
	// nackTotal counts the number of messages Nacked.
	nackTotal metric.Int64Counter
	// bufferSize tracks the current buffer size.
	bufferSize metric.Int64Gauge
}

// NewBatchBufferMetrics creates a new BatchBufferMetrics instance.
func NewBatchBufferMetrics() (*BatchBufferMetrics, error) {
	meter := otel.Meter(BatchBufferMeterName)

	flushTotal, err := meter.Int64Counter(
		"reconciliation_batch_flush_total",
		metric.WithDescription("Total number of batch flush operations"),
		metric.WithUnit("{flush}"),
	)
	if err != nil {
		return nil, err
	}

	flushSize, err := meter.Int64Histogram(
		"reconciliation_batch_flush_size",
		metric.WithDescription("Size of batches when flushed"),
		metric.WithUnit("{item}"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 25, 50, 100, 250, 500),
	)
	if err != nil {
		return nil, err
	}

	flushDuration, err := meter.Float64Histogram(
		"reconciliation_batch_flush_duration_seconds",
		metric.WithDescription("Duration of batch flush operations by stage"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
	)
	if err != nil {
		return nil, err
	}

	ackTotal, err := meter.Int64Counter(
		"reconciliation_batch_ack_total",
		metric.WithDescription("Total number of messages Acked"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	nackTotal, err := meter.Int64Counter(
		"reconciliation_batch_nack_total",
		metric.WithDescription("Total number of messages Nacked"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	bufferSize, err := meter.Int64Gauge(
		"reconciliation_batch_buffer_size",
		metric.WithDescription("Current number of items in the batch buffer"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	return &BatchBufferMetrics{
		flushTotal:    flushTotal,
		flushSize:     flushSize,
		flushDuration: flushDuration,
		ackTotal:      ackTotal,
		nackTotal:     nackTotal,
		bufferSize:    bufferSize,
	}, nil
}

// BatchBuffer accumulates transactions and flushes them in batches.
// It supports flushing by size threshold or time interval.
// Transactions are stored directly in OpenSearch via TransactionStore.
type BatchBuffer struct {
	config     BatchBufferConfig
	store      storage.TransactionStore
	metrics    *BatchBufferMetrics
	ingMetrics *IngestionMetrics
	publisher  message.Publisher
	topic      string

	mu        sync.Mutex
	items     []*BatchItem
	flushChan chan struct{}
	stopChan  chan struct{}
	stopped   bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBatchBuffer creates a new BatchBuffer.
// The store parameter is the TransactionStore (OpenSearch) for persisting transactions.
func NewBatchBuffer(
	config BatchBufferConfig,
	store storage.TransactionStore,
	metrics *BatchBufferMetrics,
	ingMetrics *IngestionMetrics,
	publisher message.Publisher,
	topic string,
) *BatchBuffer {
	return &BatchBuffer{
		config:     config,
		store:      store,
		metrics:    metrics,
		ingMetrics: ingMetrics,
		publisher:  publisher,
		topic:      topic,
		items:      make([]*BatchItem, 0, config.BufferSize),
		flushChan:  make(chan struct{}, 1),
		stopChan:   make(chan struct{}),
	}
}

// Start begins the background flush timer goroutine.
func (b *BatchBuffer) Start(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)

	b.wg.Add(1)
	go b.flushLoop()

	logging.FromContext(ctx).WithFields(map[string]interface{}{
		"buffer_size":    b.config.BufferSize,
		"flush_interval": b.config.FlushInterval.String(),
		"enabled":        b.config.Enabled,
	}).Info("Batch buffer started")

	return nil
}

// Stop gracefully stops the batch buffer with a final flush.
func (b *BatchBuffer) Stop() error {
	b.mu.Lock()
	b.stopped = true
	b.mu.Unlock()

	// Signal the flush loop to perform final flush and exit
	close(b.stopChan)

	// Wait for flush loop to complete (it will do the final flush)
	b.wg.Wait()

	if b.cancel != nil {
		b.cancel()
	}

	return nil
}

// Submit adds a message and its transaction to the buffer.
// If the buffer is full, it triggers an immediate flush.
// This method is non-blocking and safe for concurrent use.
func (b *BatchBuffer) Submit(ctx context.Context, msg *message.Message, tx *models.Transaction) {
	if !b.config.Enabled {
		// Fallback to synchronous processing when batch mode is disabled
		b.processSingle(ctx, msg, tx)
		return
	}

	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		msg.Nack()
		return
	}
	b.items = append(b.items, &BatchItem{
		Message:     msg,
		Transaction: tx,
	})
	currentSize := len(b.items)
	b.mu.Unlock()

	// Update buffer size metric
	if b.metrics != nil {
		b.metrics.bufferSize.Record(ctx, int64(currentSize))
	}

	// Trigger flush if buffer is full
	if currentSize >= b.config.BufferSize {
		select {
		case b.flushChan <- struct{}{}:
		default:
			// Flush already pending
		}
	}
}

// flushLoop runs the background flush timer.
func (b *BatchBuffer) flushLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopChan:
			// Perform final flush before exiting (context is still valid)
			b.flush(FlushTriggerShutdown)
			return
		case <-ticker.C:
			b.flush(FlushTriggerTimer)
		case <-b.flushChan:
			b.flush(FlushTriggerSize)
		}
	}
}

// flush processes all buffered items.
func (b *BatchBuffer) flush(trigger string) {
	b.mu.Lock()
	if len(b.items) == 0 {
		b.mu.Unlock()
		return
	}

	// Take ownership of current items
	toFlush := b.items
	b.items = make([]*BatchItem, 0, b.config.BufferSize)
	b.mu.Unlock()

	ctx := context.Background()
	if b.ctx != nil {
		ctx = b.ctx
	}

	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"trigger":    trigger,
		"batch_size": len(toFlush),
	})

	logger.Debug("Starting batch flush")

	startTime := time.Now()

	// Record flush metrics
	if b.metrics != nil {
		b.metrics.flushTotal.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("trigger", trigger),
			),
		)
		b.metrics.flushSize.Record(ctx, int64(len(toFlush)))
	}

	// Separate items by side
	ledgerItems, paymentItems, unknownItems := b.splitBySide(ctx, toFlush)

	// Nack unknown items so they are not silently lost
	for _, item := range unknownItems {
		if item.Message != nil {
			item.Message.Nack()
		}
	}

	// Process each side
	b.processSide(ctx, ledgerItems, models.TransactionSideLedger, SideLedger)
	b.processSide(ctx, paymentItems, models.TransactionSidePayments, SidePayment)

	// Record total flush duration
	if b.metrics != nil {
		b.metrics.flushDuration.Record(ctx, time.Since(startTime).Seconds(),
			metric.WithAttributes(
				attribute.String("trigger", trigger),
				attribute.String("stage", "total"),
			),
		)
	}

	// Update buffer size metric (now empty)
	if b.metrics != nil {
		b.metrics.bufferSize.Record(ctx, 0)
	}

	logger.WithFields(map[string]interface{}{
		"duration":       time.Since(startTime).String(),
		"ledger_count":   len(ledgerItems),
		"payments_count": len(paymentItems),
	}).Debug("Batch flush completed")
}

// splitBySide separates items by transaction side.
// Items with unknown sides are logged as warnings and collected in a third slice.
func (b *BatchBuffer) splitBySide(ctx context.Context, items []*BatchItem) ([]*BatchItem, []*BatchItem, []*BatchItem) {
	var ledgerItems, paymentItems, unknownItems []*BatchItem
	logger := logging.FromContext(ctx)

	for _, item := range items {
		switch item.Transaction.Side {
		case models.TransactionSideLedger:
			ledgerItems = append(ledgerItems, item)
		case models.TransactionSidePayments:
			paymentItems = append(paymentItems, item)
		default:
			logger.WithFields(map[string]interface{}{
				"transaction_id":   item.Transaction.ID,
				"external_id":     item.Transaction.ExternalID,
				"unknown_side":    string(item.Transaction.Side),
			}).Error("Dropping item with unknown transaction side")
			unknownItems = append(unknownItems, item)
		}
	}

	return ledgerItems, paymentItems, unknownItems
}

// processSide handles a batch of items for a specific side.
// Transactions are stored directly in OpenSearch via TransactionStore.
func (b *BatchBuffer) processSide(ctx context.Context, items []*BatchItem, side models.TransactionSide, metricsSide string) {
	if len(items) == 0 {
		return
	}

	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"side":  side,
		"count": len(items),
	})

	// 1. Check for duplicates in OpenSearch
	externalIDs := make([]string, len(items))
	for i, item := range items {
		externalIDs[i] = item.Transaction.ExternalID
	}

	checkStart := time.Now()
	existing, err := b.store.ExistsByExternalIDs(ctx, side, externalIDs)
	if b.metrics != nil {
		b.metrics.flushDuration.Record(ctx, time.Since(checkStart).Seconds(),
			metric.WithAttributes(
				attribute.String("side", metricsSide),
				attribute.String("stage", "duplicate_check"),
			),
		)
	}

	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("Failed to check existing external IDs, Nacking all items")
		b.nackAll(ctx, items, metricsSide, "store_error")
		return
	}

	// 2. Filter out duplicates and Ack them
	var newItems []*BatchItem
	for _, item := range items {
		if existing[item.Transaction.ExternalID] {
			// Already exists - Ack as duplicate
			item.Message.Ack()
			b.recordAck(ctx, metricsSide, "duplicate")
			b.recordIngestionMetrics(ctx, metricsSide, StatusSkipped, item.Transaction.OccurredAt)
		} else {
			newItems = append(newItems, item)
		}
	}

	if len(newItems) == 0 {
		logger.Debug("All items were duplicates")
		return
	}

	// 3. Deduplicate within the batch itself
	newItems = b.deduplicateInBatch(ctx, newItems, metricsSide)
	if len(newItems) == 0 {
		return
	}

	// 4. Batch INSERT to OpenSearch
	txs := make([]*models.Transaction, len(newItems))
	for i, item := range newItems {
		txs[i] = item.Transaction
	}

	insertStart := time.Now()
	if err := b.store.CreateBatch(ctx, txs); err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("Failed to batch insert transactions, Nacking all items")
		if b.metrics != nil {
			b.metrics.flushDuration.Record(ctx, time.Since(insertStart).Seconds(),
				metric.WithAttributes(
					attribute.String("side", metricsSide),
					attribute.String("stage", "insert"),
				),
			)
		}
		b.nackAll(ctx, newItems, metricsSide, "insert_error")
		return
	}

	if b.metrics != nil {
		b.metrics.flushDuration.Record(ctx, time.Since(insertStart).Seconds(),
			metric.WithAttributes(
				attribute.String("side", metricsSide),
				attribute.String("stage", "insert"),
			),
		)
	}

	// 5. Ack all successfully inserted and publish events
	for _, item := range newItems {
		item.Message.Ack()
		b.recordAck(ctx, metricsSide, "success")
		b.recordIngestionMetrics(ctx, metricsSide, StatusSuccess, item.Transaction.OccurredAt)
		b.publishTransactionIngested(ctx, item.Transaction)
	}

	logger.WithFields(map[string]interface{}{
		"inserted": len(newItems),
	}).Debug("Successfully inserted batch to OpenSearch")
}

// deduplicateInBatch removes duplicate external_ids within the same batch.
// Only the first occurrence is kept; subsequent duplicates are Acked.
func (b *BatchBuffer) deduplicateInBatch(ctx context.Context, items []*BatchItem, metricsSide string) []*BatchItem {
	seen := make(map[string]bool)
	var unique []*BatchItem

	for _, item := range items {
		if seen[item.Transaction.ExternalID] {
			// Duplicate within batch - Ack
			item.Message.Ack()
			b.recordAck(ctx, metricsSide, "duplicate_in_batch")
			b.recordIngestionMetrics(ctx, metricsSide, StatusSkipped, item.Transaction.OccurredAt)
		} else {
			seen[item.Transaction.ExternalID] = true
			unique = append(unique, item)
		}
	}

	return unique
}

// nackAll Nacks all items in the list.
func (b *BatchBuffer) nackAll(ctx context.Context, items []*BatchItem, metricsSide, reason string) {
	for _, item := range items {
		item.Message.Nack()
		b.recordNack(ctx, metricsSide, reason)
		b.recordIngestionMetrics(ctx, metricsSide, StatusError, item.Transaction.OccurredAt)
	}
}

// recordAck records an Ack metric.
func (b *BatchBuffer) recordAck(ctx context.Context, side, reason string) {
	if b.metrics != nil {
		b.metrics.ackTotal.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("side", side),
				attribute.String("reason", reason),
			),
		)
	}
}

// recordNack records a Nack metric.
func (b *BatchBuffer) recordNack(ctx context.Context, side, reason string) {
	if b.metrics != nil {
		b.metrics.nackTotal.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("side", side),
				attribute.String("reason", reason),
			),
		)
	}
}

// recordIngestionMetrics records ingestion-related metrics.
func (b *BatchBuffer) recordIngestionMetrics(ctx context.Context, side, status string, occurredAt time.Time) {
	if b.ingMetrics != nil {
		b.ingMetrics.RecordEvent(ctx, side, status)
		if !occurredAt.IsZero() {
			b.ingMetrics.RecordLag(ctx, side, occurredAt)
		}
	}
}

// processSingle handles a single transaction without batching.
// Used when batch mode is disabled.
func (b *BatchBuffer) processSingle(ctx context.Context, msg *message.Message, tx *models.Transaction) {
	side := tx.Side
	metricsSide := SideLedger
	if side == models.TransactionSidePayments {
		metricsSide = SidePayment
	}

	// Check for duplicate in OpenSearch
	existing, err := b.store.ExistsByExternalIDs(ctx, side, []string{tx.ExternalID})
	if err != nil {
		msg.Nack()
		b.recordNack(ctx, metricsSide, "store_error")
		b.recordIngestionMetrics(ctx, metricsSide, StatusError, tx.OccurredAt)
		return
	}

	if existing[tx.ExternalID] {
		msg.Ack()
		b.recordAck(ctx, metricsSide, "duplicate")
		b.recordIngestionMetrics(ctx, metricsSide, StatusSkipped, tx.OccurredAt)
		return
	}

	// Create transaction in OpenSearch
	if err := b.store.Create(ctx, tx); err != nil {
		msg.Nack()
		b.recordNack(ctx, metricsSide, "insert_error")
		b.recordIngestionMetrics(ctx, metricsSide, StatusError, tx.OccurredAt)
		return
	}

	msg.Ack()
	b.recordAck(ctx, metricsSide, "success")
	b.recordIngestionMetrics(ctx, metricsSide, StatusSuccess, tx.OccurredAt)
	b.publishTransactionIngested(ctx, tx)
}

// publishTransactionIngested publishes a TRANSACTION_INGESTED event for a successfully stored transaction.
func (b *BatchBuffer) publishTransactionIngested(ctx context.Context, tx *models.Transaction) {
	if b.publisher == nil || b.topic == "" {
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

	if err := b.publisher.Publish(b.topic, msg); err != nil {
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"error":         err.Error(),
			"transactionId": tx.ID.String(),
			"topic":         b.topic,
		}).Error("Failed to publish TRANSACTION_INGESTED event")
	}
}

// IsEnabled returns whether batch mode is enabled.
func (b *BatchBuffer) IsEnabled() bool {
	return b.config.Enabled
}

// Config returns the batch buffer configuration.
func (b *BatchBuffer) Config() BatchBufferConfig {
	return b.config
}
