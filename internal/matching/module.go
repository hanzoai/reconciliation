package matching

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/storage"
	"go.uber.org/fx"
)

// MatchingConsumerConfig holds the configuration for the matching consumer module.
type MatchingConsumerConfig struct {
	// Topic is the NATS topic to consume from (e.g., "{stack}.reconciliation").
	Topic string
	// NumWorkers is the number of concurrent message handlers (default 10).
	NumWorkers int
}

// MatchingConsumerModule creates an fx module that wires the matching consumer
// with all dependencies needed for matching (TransactionStore, PolicyResolver, OrchestratorFactory).
func MatchingConsumerModule(config MatchingConsumerConfig) fx.Option {
	if config.Topic == "" {
		return fx.Options()
	}

	return fx.Options(
		// Provide PolicyResolver
		fx.Provide(func(store *storage.Storage) PolicyResolver {
			return NewStoragePolicyResolver(store)
		}),

		// Provide MatchingConsumer
		fx.Provide(func(
			subscriber message.Subscriber,
			txStore storage.TransactionStore,
			policyResolver PolicyResolver,
			orchestratorProvider OrchestratorProvider,
		) *MatchingConsumer {
			return NewMatchingConsumer(subscriber, config.Topic, txStore, policyResolver, orchestratorProvider, config.NumWorkers)
		}),

		// Start consumer lifecycle
		fx.Invoke(func(lc fx.Lifecycle, consumer *MatchingConsumer) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logging.FromContext(ctx).WithFields(map[string]interface{}{
						"topic":      config.Topic,
						"numWorkers": consumer.numWorkers,
					}).Info("Starting matching consumer")
					go func() {
						if err := consumer.Start(ctx); err != nil {
							logging.FromContext(ctx).WithFields(map[string]interface{}{
								"error": err.Error(),
							}).Error("Matching consumer stopped with error")
						}
					}()
					return nil
				},
			})
		}),
	)
}
