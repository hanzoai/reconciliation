package elasticsearch

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// IndexerMeterName is the name of the meter used for indexer metrics.
	IndexerMeterName = "github.com/formancehq/reconciliation/internal/elasticsearch"

	// IndexStatusSuccess indicates successful indexing.
	IndexStatusSuccess = "success"
	// IndexStatusError indicates failed indexing.
	IndexStatusError = "error"
)

// IndexerMetrics holds the OpenTelemetry metrics instruments for the indexer.
type IndexerMetrics struct {
	// indexTotal counts the total number of indexing operations.
	// Labels: status (success/error)
	indexTotal metric.Int64Counter
}

// NewIndexerMetrics creates a new IndexerMetrics instance.
func NewIndexerMetrics() (*IndexerMetrics, error) {
	meter := otel.Meter(IndexerMeterName)

	indexTotal, err := meter.Int64Counter(
		"reconciliation_opensearch_index_total",
		metric.WithDescription("Total number of OpenSearch indexing operations"),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}

	return &IndexerMetrics{
		indexTotal: indexTotal,
	}, nil
}

// RecordIndex records an indexing operation with the given status.
func (m *IndexerMetrics) RecordIndex(ctx context.Context, status string) {
	m.indexTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("status", status),
		),
	)
}

// TransactionIndexer provides synchronous transaction indexing to OpenSearch.
type TransactionIndexer struct {
	client    *Client
	metrics   *IndexerMetrics
	ismConfig ISMConfig
	stack     string
}

// NewTransactionIndexer creates a new TransactionIndexer.
func NewTransactionIndexer(client *Client, metrics *IndexerMetrics, ismConfig ISMConfig, stack string) *TransactionIndexer {
	return &TransactionIndexer{
		client:    client,
		metrics:   metrics,
		ismConfig: ismConfig,
		stack:     stack,
	}
}

// Stack returns the stack name used for index naming.
func (i *TransactionIndexer) Stack() string {
	return i.stack
}

// Index indexes a transaction in OpenSearch.
// This method is designed to be called synchronously after a transaction is persisted.
// The transaction is indexed into a monthly index based on its OccurredAt date.
// The document ID is set to the transaction ID to ensure idempotency.
func (i *TransactionIndexer) Index(ctx context.Context, tx *models.Transaction) error {
	indexName := MonthlyTransactionIndexName(i.stack, tx.OccurredAt)
	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"transaction_id": tx.ID.String(),
		"index":          indexName,
	})

	err := i.client.IndexTransaction(ctx, i.stack, tx)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("Failed to index transaction in OpenSearch")

		if i.metrics != nil {
			i.metrics.RecordIndex(ctx, IndexStatusError)
		}

		return err
	}

	logger.Debug("Transaction indexed in OpenSearch")

	if i.metrics != nil {
		i.metrics.RecordIndex(ctx, IndexStatusSuccess)
	}

	return nil
}

// EnsureTemplateExists ensures the transaction index template exists in OpenSearch.
// This should be called during application startup.
func (i *TransactionIndexer) EnsureTemplateExists(ctx context.Context) error {
	return i.client.EnsureTransactionIndexTemplate(ctx, i.stack, i.ismConfig.Enabled)
}

// EnsureISMPolicy ensures the ISM policy exists in OpenSearch.
// This should be called during application startup before EnsureTemplateExists.
func (i *TransactionIndexer) EnsureISMPolicy(ctx context.Context) error {
	return i.client.EnsureISMPolicy(ctx, i.stack, i.ismConfig)
}

// EnsureILMPolicy is an alias for EnsureISMPolicy for backward compatibility.
func (i *TransactionIndexer) EnsureILMPolicy(ctx context.Context) error {
	return i.EnsureISMPolicy(ctx)
}

// Client returns the underlying OpenSearch client.
// This allows other components to use the same connection.
func (i *TransactionIndexer) Client() *Client {
	return i.client
}
