package elasticsearch

import (
	"context"
	"testing"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchIndexerMetrics(t *testing.T) {
	metrics, err := NewBatchIndexerMetrics()
	require.NoError(t, err)
	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.bulkIndexTotal)
	assert.NotNil(t, metrics.bulkIndexSize)
	assert.NotNil(t, metrics.bulkIndexDuration)
	assert.NotNil(t, metrics.bulkIndexErrors)
}

func TestNewBatchIndexer(t *testing.T) {
	// Note: This test doesn't actually connect to OpenSearch,
	// it just verifies the constructor works correctly.
	t.Run("creates batch indexer with nil metrics", func(t *testing.T) {
		// We can't create a real client without OpenSearch,
		// but we can verify the struct is properly initialized
		indexer := &BatchIndexer{
			client:  nil,
			stack:   "test-stack",
			metrics: nil,
		}
		assert.Equal(t, "test-stack", indexer.Stack())
		assert.Nil(t, indexer.Client())
	})

	t.Run("creates batch indexer with metrics", func(t *testing.T) {
		metrics, err := NewBatchIndexerMetrics()
		require.NoError(t, err)

		indexer := &BatchIndexer{
			client:  nil,
			stack:   "test-stack",
			metrics: metrics,
		}
		assert.Equal(t, "test-stack", indexer.Stack())
		assert.NotNil(t, indexer.metrics)
	})
}

func TestBatchIndexer_IndexBatch_EmptyBatch(t *testing.T) {
	indexer := &BatchIndexer{
		client:  nil,
		stack:   "test-stack",
		metrics: nil,
	}

	// Empty batch should return immediately without error
	err := indexer.IndexBatch(context.Background(), nil)
	assert.NoError(t, err)

	err = indexer.IndexBatch(context.Background(), []*models.Transaction{})
	assert.NoError(t, err)
}

func TestBatchIndexer_IndexBatchAsync_EmptyBatch(t *testing.T) {
	indexer := &BatchIndexer{
		client:  nil,
		stack:   "test-stack",
		metrics: nil,
	}

	// Empty batch should return immediately without spawning goroutine
	indexer.IndexBatchAsync(context.Background(), nil)
	indexer.IndexBatchAsync(context.Background(), []*models.Transaction{})
	// No panic means success
}

// Integration tests that require a real OpenSearch instance
// are in batch_indexer_integration_test.go
