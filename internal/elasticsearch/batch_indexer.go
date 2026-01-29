package elasticsearch

import (
	"context"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// BatchIndexerMeterName is the name of the meter used for batch indexer metrics.
	BatchIndexerMeterName = "github.com/formancehq/reconciliation/internal/elasticsearch/batch_indexer"
)

// BatchIndexerMetrics holds the OpenTelemetry metrics instruments for the batch indexer.
type BatchIndexerMetrics struct {
	// bulkIndexTotal counts the total number of bulk indexing operations.
	bulkIndexTotal metric.Int64Counter

	// bulkIndexSize tracks the size of bulk index batches.
	bulkIndexSize metric.Int64Histogram

	// bulkIndexDuration tracks the duration of bulk index operations.
	bulkIndexDuration metric.Float64Histogram

	// bulkIndexErrors counts the number of bulk index errors.
	bulkIndexErrors metric.Int64Counter
}

// NewBatchIndexerMetrics creates a new BatchIndexerMetrics instance.
func NewBatchIndexerMetrics() (*BatchIndexerMetrics, error) {
	meter := otel.Meter(BatchIndexerMeterName)

	bulkIndexTotal, err := meter.Int64Counter(
		"reconciliation_batch_bulk_index_total",
		metric.WithDescription("Total number of bulk indexing operations"),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}

	bulkIndexSize, err := meter.Int64Histogram(
		"reconciliation_batch_bulk_index_size",
		metric.WithDescription("Size of bulk index batches"),
		metric.WithUnit("{document}"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 25, 50, 100, 250, 500, 1000),
	)
	if err != nil {
		return nil, err
	}

	bulkIndexDuration, err := meter.Float64Histogram(
		"reconciliation_batch_bulk_index_duration_seconds",
		metric.WithDescription("Duration of bulk index operations in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	bulkIndexErrors, err := meter.Int64Counter(
		"reconciliation_batch_bulk_index_errors_total",
		metric.WithDescription("Total number of bulk index errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	return &BatchIndexerMetrics{
		bulkIndexTotal:    bulkIndexTotal,
		bulkIndexSize:     bulkIndexSize,
		bulkIndexDuration: bulkIndexDuration,
		bulkIndexErrors:   bulkIndexErrors,
	}, nil
}

// BatchIndexer provides batch indexing of transactions to OpenSearch.
// It wraps the Client's BulkIndex method with metrics and logging.
type BatchIndexer struct {
	client  *Client
	stack   string
	metrics *BatchIndexerMetrics
}

// NewBatchIndexer creates a new BatchIndexer.
func NewBatchIndexer(client *Client, stack string, metrics *BatchIndexerMetrics) *BatchIndexer {
	return &BatchIndexer{
		client:  client,
		stack:   stack,
		metrics: metrics,
	}
}

// IndexBatch indexes a batch of transactions in OpenSearch using the bulk API.
// This is a best-effort operation - errors are logged but not returned to the caller.
// This allows the ingestion pipeline to continue even if OpenSearch is temporarily unavailable.
func (b *BatchIndexer) IndexBatch(ctx context.Context, transactions []*models.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"batch_size": len(transactions),
		"stack":      b.stack,
	})

	startTime := time.Now()

	err := b.client.BulkIndex(ctx, b.stack, transactions)

	duration := time.Since(startTime)

	// Record metrics
	if b.metrics != nil {
		b.metrics.bulkIndexTotal.Add(ctx, 1)
		b.metrics.bulkIndexSize.Record(ctx, int64(len(transactions)))
		b.metrics.bulkIndexDuration.Record(ctx, duration.Seconds())

		if err != nil {
			b.metrics.bulkIndexErrors.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("error_type", "bulk_index_failed"),
				),
			)
		}
	}

	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error":    err.Error(),
			"duration": duration.String(),
		}).Error("Failed to bulk index transactions in OpenSearch")
		return err
	}

	logger.WithFields(map[string]interface{}{
		"duration": duration.String(),
	}).Debug("Successfully bulk indexed transactions in OpenSearch")

	return nil
}

// IndexBatchAsync indexes a batch of transactions asynchronously (fire-and-forget).
// Errors are logged but the caller is not blocked.
func (b *BatchIndexer) IndexBatchAsync(ctx context.Context, transactions []*models.Transaction) {
	if len(transactions) == 0 {
		return
	}

	go func() {
		// Create a new context that won't be cancelled when parent is done
		asyncCtx := context.Background()
		_ = b.IndexBatch(asyncCtx, transactions)
	}()
}

// Client returns the underlying OpenSearch client.
func (b *BatchIndexer) Client() *Client {
	return b.client
}

// Stack returns the stack name used for index naming.
func (b *BatchIndexer) Stack() string {
	return b.stack
}
