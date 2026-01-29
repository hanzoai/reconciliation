package elasticsearch

import (
	"context"
	"os"
	"strconv"

	"github.com/formancehq/go-libs/v3/health"
	"github.com/formancehq/go-libs/v3/logging"
	"go.uber.org/fx"
)

// TransactionIndexerParams holds the dependencies for creating a TransactionIndexer.
type TransactionIndexerParams struct {
	fx.In
	Client    *Client
	Metrics   *IndexerMetrics `optional:"true"`
	ISMConfig ISMConfig
	Stack     string `name:"stack"`
}

// BatchIndexerParams holds the dependencies for creating a BatchIndexer.
type BatchIndexerParams struct {
	fx.In
	Client  *Client
	Metrics *BatchIndexerMetrics `optional:"true"`
	Stack   string               `name:"stack"`
}

const (
	// StackEnvVar is the environment variable for the stack identifier.
	// Used as prefix for index names: $STACK-reconciliation-YYYY-MM
	StackEnvVar = "STACK"

	// OpenSearchURLEnvVar is the environment variable for OpenSearch URL.
	// Also supports ELASTICSEARCH_URL for backward compatibility.
	OpenSearchURLEnvVar = "OPENSEARCH_URL"
	// ElasticsearchURLEnvVar is kept for backward compatibility.
	ElasticsearchURLEnvVar = "ELASTICSEARCH_URL"
	// OpenSearchUsernameEnvVar is the environment variable for OpenSearch username.
	OpenSearchUsernameEnvVar = "OPENSEARCH_USERNAME"
	// ElasticsearchUsernameEnvVar is kept for backward compatibility.
	ElasticsearchUsernameEnvVar = "ELASTICSEARCH_USERNAME"
	// OpenSearchPasswordEnvVar is the environment variable for OpenSearch password.
	OpenSearchPasswordEnvVar = "OPENSEARCH_PASSWORD"
	// ElasticsearchPasswordEnvVar is kept for backward compatibility.
	ElasticsearchPasswordEnvVar = "ELASTICSEARCH_PASSWORD"
	// OpenSearchISMEnabledEnvVar is the environment variable to enable ISM.
	OpenSearchISMEnabledEnvVar = "OPENSEARCH_ISM_ENABLED"
	// ElasticsearchILMEnabledEnvVar is kept for backward compatibility.
	ElasticsearchILMEnabledEnvVar = "ELASTICSEARCH_ILM_ENABLED"
	// OpenSearchISMHotPhaseDaysEnvVar is the environment variable for hot phase duration in days.
	OpenSearchISMHotPhaseDaysEnvVar = "OPENSEARCH_ISM_HOT_PHASE_DAYS"
	// ElasticsearchILMHotPhaseDaysEnvVar is kept for backward compatibility.
	ElasticsearchILMHotPhaseDaysEnvVar = "ELASTICSEARCH_ILM_HOT_PHASE_DAYS"
	// OpenSearchISMWarmPhaseRolloverDaysEnvVar is the environment variable for warm phase rollover age in days.
	OpenSearchISMWarmPhaseRolloverDaysEnvVar = "OPENSEARCH_ISM_WARM_PHASE_ROLLOVER_DAYS"
	// ElasticsearchILMWarmPhaseRolloverDaysEnvVar is kept for backward compatibility.
	ElasticsearchILMWarmPhaseRolloverDaysEnvVar = "ELASTICSEARCH_ILM_WARM_PHASE_ROLLOVER_DAYS"
	// OpenSearchISMDeletePhaseEnabledEnvVar is the environment variable to enable the delete phase.
	OpenSearchISMDeletePhaseEnabledEnvVar = "OPENSEARCH_ISM_DELETE_PHASE_ENABLED"
	// ElasticsearchILMDeletePhaseEnabledEnvVar is kept for backward compatibility.
	ElasticsearchILMDeletePhaseEnabledEnvVar = "ELASTICSEARCH_ILM_DELETE_PHASE_ENABLED"
	// OpenSearchISMDeletePhaseDaysEnvVar is the environment variable for delete phase age in days.
	OpenSearchISMDeletePhaseDaysEnvVar = "OPENSEARCH_ISM_DELETE_PHASE_DAYS"
	// ElasticsearchILMDeletePhaseDaysEnvVar is kept for backward compatibility.
	ElasticsearchILMDeletePhaseDaysEnvVar = "ELASTICSEARCH_ILM_DELETE_PHASE_DAYS"
)

// getEnvWithFallback returns the value of the primary env var, or falls back to the secondary.
func getEnvWithFallback(primary, fallback string) string {
	if value := os.Getenv(primary); value != "" {
		return value
	}
	return os.Getenv(fallback)
}

// ConfigFromEnv creates a Config from environment variables.
// Supports both OPENSEARCH_* and ELASTICSEARCH_* variables for backward compatibility.
func ConfigFromEnv() Config {
	return Config{
		URL:      getEnvWithFallback(OpenSearchURLEnvVar, ElasticsearchURLEnvVar),
		Username: getEnvWithFallback(OpenSearchUsernameEnvVar, ElasticsearchUsernameEnvVar),
		Password: getEnvWithFallback(OpenSearchPasswordEnvVar, ElasticsearchPasswordEnvVar),
	}
}

// ISMConfigFromEnv creates an ISMConfig from environment variables.
// Supports both OPENSEARCH_ISM_* and ELASTICSEARCH_ILM_* variables for backward compatibility.
// If no environment variables are set, it returns the default configuration.
func ISMConfigFromEnv() ISMConfig {
	config := DefaultISMConfig()

	// Check if ISM is explicitly disabled
	if enabled := getEnvWithFallback(OpenSearchISMEnabledEnvVar, ElasticsearchILMEnabledEnvVar); enabled != "" {
		config.Enabled = enabled == "true" || enabled == "1"
	}

	// Hot phase days
	if days := getEnvWithFallback(OpenSearchISMHotPhaseDaysEnvVar, ElasticsearchILMHotPhaseDaysEnvVar); days != "" {
		if d, err := strconv.Atoi(days); err == nil && d > 0 {
			config.HotPhaseDays = d
		}
	}

	// Warm phase rollover days
	if days := getEnvWithFallback(OpenSearchISMWarmPhaseRolloverDaysEnvVar, ElasticsearchILMWarmPhaseRolloverDaysEnvVar); days != "" {
		if d, err := strconv.Atoi(days); err == nil && d > 0 {
			config.WarmPhaseRolloverDays = d
		}
	}

	// Delete phase enabled
	if enabled := getEnvWithFallback(OpenSearchISMDeletePhaseEnabledEnvVar, ElasticsearchILMDeletePhaseEnabledEnvVar); enabled != "" {
		config.DeletePhaseEnabled = enabled == "true" || enabled == "1"
	}

	// Delete phase days
	if days := getEnvWithFallback(OpenSearchISMDeletePhaseDaysEnvVar, ElasticsearchILMDeletePhaseDaysEnvVar); days != "" {
		if d, err := strconv.Atoi(days); err == nil && d > 0 {
			config.DeletePhaseDays = d
		}
	}

	return config
}

// ILMConfigFromEnv is an alias for ISMConfigFromEnv for backward compatibility.
func ILMConfigFromEnv() ILMConfig {
	return ISMConfigFromEnv()
}

// GetStackFromEnv returns the stack name from the STACK environment variable.
// Returns "default" if not set.
func GetStackFromEnv() string {
	if stack := os.Getenv(StackEnvVar); stack != "" {
		return stack
	}
	return "default"
}

// Module creates an fx module for OpenSearch.
// The module reads configuration from environment variables and provides:
// - *Client: the OpenSearch client
// - *TransactionIndexer: the transaction indexer service
// - *BatchIndexer: the batch indexer service for bulk operations
// - *IndexerMetrics: the indexer metrics
// - *BatchIndexerMetrics: the batch indexer metrics
// - ISMConfig: the ISM configuration
// - Health check named "opensearch"
func Module() fx.Option {
	return fx.Options(
		fx.Provide(func() (*Client, error) {
			config := ConfigFromEnv()
			return NewClient(config)
		}),
		fx.Provide(func() (*IndexerMetrics, error) {
			return NewIndexerMetrics()
		}),
		fx.Provide(func() (*BatchIndexerMetrics, error) {
			return NewBatchIndexerMetrics()
		}),
		fx.Provide(func() ISMConfig {
			return ISMConfigFromEnv()
		}),
		fx.Provide(fx.Annotate(
			func() string {
				return GetStackFromEnv()
			},
			fx.ResultTags(`name:"stack"`),
		)),
		fx.Provide(func(params TransactionIndexerParams) *TransactionIndexer {
			return NewTransactionIndexer(params.Client, params.Metrics, params.ISMConfig, params.Stack)
		}),
		fx.Provide(func(params BatchIndexerParams) *BatchIndexer {
			return NewBatchIndexer(params.Client, params.Stack, params.Metrics)
		}),
		health.ProvideHealthCheck(func(client *Client) health.NamedCheck {
			return health.NewNamedCheck("opensearch", health.CheckFn(func(ctx context.Context) error {
				return client.Health(ctx)
			}))
		}),
		fx.Invoke(func(lc fx.Lifecycle, client *Client, indexer *TransactionIndexer, ismConfig ISMConfig) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logging.FromContext(ctx).Debug("Verifying OpenSearch connection...")
					if err := client.Ping(ctx); err != nil {
						logging.FromContext(ctx).WithFields(map[string]interface{}{
							"error": err.Error(),
						}).Error("Failed to connect to OpenSearch")
						return err
					}
					logging.FromContext(ctx).Info("Successfully connected to OpenSearch")

					// Ensure the ISM policy exists (if enabled)
					if ismConfig.Enabled {
						logging.FromContext(ctx).Debug("Ensuring ISM policy exists...")
						if err := indexer.EnsureISMPolicy(ctx); err != nil {
							logging.FromContext(ctx).WithFields(map[string]interface{}{
								"error": err.Error(),
							}).Error("Failed to ensure ISM policy")
							return err
						}
						logging.FromContext(ctx).Info("ISM policy is ready")
					}

					// Ensure the transaction index template exists
					logging.FromContext(ctx).Debug("Ensuring transaction index template exists...")
					if err := indexer.EnsureTemplateExists(ctx); err != nil {
						logging.FromContext(ctx).WithFields(map[string]interface{}{
							"error": err.Error(),
						}).Error("Failed to ensure transaction index template")
						return err
					}
					logging.FromContext(ctx).Info("Transaction index template is ready")

					return nil
				},
			})
		}),
	)
}

// ModuleWithStack creates an fx module for OpenSearch with an explicit stack identifier.
// Configuration is still read from environment variables; only the stack is overridden.
// Use this when the stack comes from a CLI flag instead of the STACK environment variable.
func ModuleWithStack(stack string) fx.Option {
	return ModuleWithConfig(ConfigFromEnv(), ISMConfigFromEnv(), stack)
}

// ModuleWithConfig creates an fx module for OpenSearch with explicit configuration.
// This is useful for testing or when configuration comes from a source other than environment variables.
func ModuleWithConfig(config Config, ismConfig ISMConfig, stack string) fx.Option {
	return fx.Options(
		fx.Provide(func() (*Client, error) {
			return NewClient(config)
		}),
		fx.Provide(func() (*IndexerMetrics, error) {
			return NewIndexerMetrics()
		}),
		fx.Provide(func() (*BatchIndexerMetrics, error) {
			return NewBatchIndexerMetrics()
		}),
		fx.Provide(func() ISMConfig {
			return ismConfig
		}),
		fx.Provide(fx.Annotate(
			func() string {
				return stack
			},
			fx.ResultTags(`name:"stack"`),
		)),
		fx.Provide(func(params TransactionIndexerParams) *TransactionIndexer {
			return NewTransactionIndexer(params.Client, params.Metrics, params.ISMConfig, params.Stack)
		}),
		fx.Provide(func(params BatchIndexerParams) *BatchIndexer {
			return NewBatchIndexer(params.Client, params.Stack, params.Metrics)
		}),
		health.ProvideHealthCheck(func(client *Client) health.NamedCheck {
			return health.NewNamedCheck("opensearch", health.CheckFn(func(ctx context.Context) error {
				return client.Health(ctx)
			}))
		}),
		fx.Invoke(func(lc fx.Lifecycle, client *Client, indexer *TransactionIndexer, ismCfg ISMConfig) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logging.FromContext(ctx).Debug("Verifying OpenSearch connection...")
					if err := client.Ping(ctx); err != nil {
						logging.FromContext(ctx).WithFields(map[string]interface{}{
							"error": err.Error(),
						}).Error("Failed to connect to OpenSearch")
						return err
					}
					logging.FromContext(ctx).Info("Successfully connected to OpenSearch")

					// Ensure the ISM policy exists (if enabled)
					if ismCfg.Enabled {
						logging.FromContext(ctx).Debug("Ensuring ISM policy exists...")
						if err := indexer.EnsureISMPolicy(ctx); err != nil {
							logging.FromContext(ctx).WithFields(map[string]interface{}{
								"error": err.Error(),
							}).Error("Failed to ensure ISM policy")
							return err
						}
						logging.FromContext(ctx).Info("ISM policy is ready")
					}

					// Ensure the transaction index template exists
					logging.FromContext(ctx).Debug("Ensuring transaction index template exists...")
					if err := indexer.EnsureTemplateExists(ctx); err != nil {
						logging.FromContext(ctx).WithFields(map[string]interface{}{
							"error": err.Error(),
						}).Error("Failed to ensure transaction index template")
						return err
					}
					logging.FromContext(ctx).Info("Transaction index template is ready")

					return nil
				},
			})
		}),
	)
}
