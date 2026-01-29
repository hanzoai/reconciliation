package ingestion

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/health"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/formancehq/reconciliation/internal/storage"
	"go.uber.org/fx"
)

// MetricsModule creates an fx module for ingestion metrics.
// This should be included when ingestion is enabled.
func MetricsModule() fx.Option {
	return fx.Provide(func() (*IngestionMetrics, error) {
		return NewIngestionMetrics()
	})
}

// SharedIngestionConfig holds common configuration for ingestion modules.
type SharedIngestionConfig struct {
	Stack string
}

// SharedIngestionModule creates an fx module for shared ingestion dependencies.
// This module should be included once when any ingestion is enabled.
// It provides: TransactionStore (OpenSearch), MatchRepository, AnomalyRepository.
// Note: Transactions are stored in OpenSearch only. Matching is done via the /v2/match API.
func SharedIngestionModule(config SharedIngestionConfig) fx.Option {
	return fx.Options(
		// Provide TransactionStore (OpenSearch-based)
		fx.Provide(func(esClient *elasticsearch.Client) storage.TransactionStore {
			return storage.NewOpenSearchTransactionStore(esClient, config.Stack)
		}),
		fx.Provide(func(store *storage.Storage) storage.MatchRepository {
			return storage.NewMatchRepository(store)
		}),
		fx.Provide(func(store *storage.Storage) storage.AnomalyRepository {
			return storage.NewAnomalyRepository(store)
		}),
	)
}

// UnifiedEventHandlerParams holds the dependencies for creating a UnifiedEventHandler.
type UnifiedEventHandlerParams struct {
	fx.In
	Store            storage.TransactionStore
	Metrics          *IngestionMetrics
	BatchBuffer      *BatchBuffer       `optional:"true"`
	Publisher        message.Publisher   `optional:"true"`
	BackfillExecutor *BackfillExecutor   `optional:"true"`
}

// BackfillExecutorParams holds the dependencies for creating a BackfillExecutor.
type BackfillExecutorParams struct {
	fx.In
	SDK       BackfillSDK             `optional:"true"`
	Store     *storage.Storage        `optional:"true"`
	TxStore   storage.TransactionStore
	Publisher message.Publisher       `optional:"true"`
}

// BatchBufferParams holds the dependencies for creating a BatchBuffer.
type BatchBufferParams struct {
	fx.In
	Store     storage.TransactionStore
	Metrics   *IngestionMetrics `optional:"true"`
	Publisher message.Publisher `optional:"true"`
}

// UnifiedIngestionConfig holds the configuration for the unified ingestion module.
type UnifiedIngestionConfig struct {
	// Topics is the list of topics to consume from (e.g., ["stacks.ledger", "stacks.payments"]).
	Topics []string
	// PublishTopic is the topic to publish TRANSACTION_INGESTED events to (e.g., "{stack}.reconciliation").
	PublishTopic string
}

// UnifiedConsumer wraps GenericConsumer for type distinction in fx.
type UnifiedConsumer struct {
	*GenericConsumer
}

// UnifiedIngestionModule creates an fx module for unified event ingestion.
// This is the primary ingestion module that handles both Ledger and Payments events.
// It uses a single consumer that routes events internally based on event type,
// solving the NATS queue group message distribution issue.
//
// The module requires SharedIngestionModule to be included first.
// Transactions are stored directly in OpenSearch via TransactionStore.
// When BatchBuffer is enabled (default), the module operates in batch mode for improved performance.
func UnifiedIngestionModule(config UnifiedIngestionConfig) fx.Option {
	if len(config.Topics) == 0 {
		return fx.Options()
	}

	// Use the first topic for the consumer subscription.
	// With NATS, the consumer filters on multiple subjects via the stream configuration.
	primaryTopic := config.Topics[0]

	// Check if batch mode is enabled via environment
	batchConfig := BatchBufferConfigFromEnv()

	return fx.Options(
		// Provide BatchBufferMetrics
		fx.Provide(func() (*BatchBufferMetrics, error) {
			return NewBatchBufferMetrics()
		}),
		// Provide BatchBuffer (uses TransactionStore for OpenSearch storage)
		fx.Provide(func(params BatchBufferParams, metrics *BatchBufferMetrics) *BatchBuffer {
			return NewBatchBuffer(
				BatchBufferConfigFromEnv(),
				params.Store,
				metrics,
				params.Metrics,
				params.Publisher,
				config.PublishTopic,
			)
		}),
		// Provide BackfillExecutor (requires SDK - available when stackClientModule is loaded)
		fx.Provide(func(params BackfillExecutorParams) *BackfillExecutor {
			if params.SDK == nil || params.Store == nil {
				return nil
			}
			return NewBackfillExecutor(params.SDK, params.Store, params.TxStore, params.Publisher, config.PublishTopic)
		}),
		fx.Provide(func(params UnifiedEventHandlerParams) *UnifiedEventHandler {
			var handler *UnifiedEventHandler
			if params.BatchBuffer != nil && params.BatchBuffer.IsEnabled() {
				handler = NewUnifiedEventHandlerWithBatchBuffer(params.Store, params.Metrics, params.BatchBuffer, params.Publisher, config.PublishTopic)
			} else {
				handler = NewUnifiedEventHandler(params.Store, params.Metrics)
			}
			if params.BackfillExecutor != nil {
				handler.SetBackfillExecutor(params.BackfillExecutor)
			}
			return handler
		}),
		fx.Provide(func(
			subscriber message.Subscriber,
			handler *UnifiedEventHandler,
			batchBuffer *BatchBuffer,
		) (*UnifiedConsumer, error) {
			consumerConfig := GenericConsumerConfig{
				Topic:     primaryTopic,
				BatchMode: batchBuffer != nil && batchBuffer.IsEnabled(),
			}
			consumer, err := NewGenericConsumer(consumerConfig, subscriber, handler)
			if err != nil {
				return nil, err
			}
			// Wire consumer to handler for lag tracking
			handler.SetConsumer(consumer)
			return &UnifiedConsumer{GenericConsumer: consumer}, nil
		}),
		fx.Provide(func(
			consumer *UnifiedConsumer,
			store storage.TransactionStore,
		) *UnifiedIngestionService {
			return NewUnifiedIngestionService(consumer.GenericConsumer, store, UnifiedIngestionServiceConfig{})
		}),
		// Provide health check for unified ingestion
		health.ProvideHealthCheck(func(service *UnifiedIngestionService) health.NamedCheck {
			return health.NewNamedCheck("ingestion.unified", health.CheckFn(func(ctx context.Context) error {
				status := GetStatus(service)
				if status.Status == HealthStatusUnhealthy {
					return &HealthCheckError{
						Status:     status.Status,
						LagSeconds: status.LagSeconds,
						Message:    status.Error,
					}
				}
				return nil
			}))
		}),
		// Start BatchBuffer if enabled
		fx.Invoke(func(lc fx.Lifecycle, buffer *BatchBuffer) {
			if buffer != nil && buffer.IsEnabled() {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						logging.FromContext(ctx).WithFields(map[string]interface{}{
							"buffer_size":    buffer.Config().BufferSize,
							"flush_interval": buffer.Config().FlushInterval.String(),
						}).Info("Starting batch buffer")
						return buffer.Start(ctx)
					},
					OnStop: func(ctx context.Context) error {
						logging.FromContext(ctx).Info("Stopping batch buffer")
						return buffer.Stop()
					},
				})
			}
		}),
		fx.Invoke(func(lc fx.Lifecycle, service *UnifiedIngestionService) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logging.FromContext(ctx).WithFields(map[string]interface{}{
						"topics":     config.Topics,
						"batch_mode": batchConfig.Enabled,
					}).Info("Starting unified ingestion service")
					go func() {
						if err := service.Start(ctx); err != nil {
							logging.FromContext(ctx).WithFields(map[string]interface{}{
								"error": err.Error(),
							}).Error("Unified ingestion service stopped with error")
						}
					}()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					logging.FromContext(ctx).Info("Stopping unified ingestion service")
					service.Stop()
					return nil
				},
			})
		}),
	)
}
