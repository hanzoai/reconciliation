package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/formancehq/go-libs/v3/aws/iam"
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/licence"
	"github.com/formancehq/go-libs/v3/publish"

	sdk "github.com/formancehq/formance-sdk-go/v3"
	sharedapi "github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/reconciliation/internal/api"
	api_service "github.com/formancehq/reconciliation/internal/api/service"
	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/formancehq/reconciliation/internal/ingestion"
	"github.com/formancehq/reconciliation/internal/matching"
	"github.com/formancehq/reconciliation/internal/reporting"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func stackClientModule(cmd *cobra.Command) fx.Option {
	return fx.Options(
		fx.Provide(func() *sdk.Formance {
			stackClientID, _ := cmd.Flags().GetString(stackClientIDFlag)
			stackClientSecret, _ := cmd.Flags().GetString(stackClientSecretFlag)
			stackURL, _ := cmd.Flags().GetString(stackURLFlag)

			oauthConfig := clientcredentials.Config{
				ClientID:     stackClientID,
				ClientSecret: stackClientSecret,
				TokenURL:     fmt.Sprintf("%s/api/auth/oauth/token", stackURL),
				Scopes:       []string{"openid", "ledger:read", "ledger:write", "payments:read", "payments:write"},
			}
			underlyingHTTPClient := &http.Client{
				Transport: otlp.NewRoundTripper(http.DefaultTransport, service.IsDebug(cmd)),
				Timeout:   24 * time.Hour,
			}
			return sdk.New(
				sdk.WithClient(
					oauthConfig.Client(context.WithValue(context.Background(),
						oauth2.HTTPClient, underlyingHTTPClient)),
				),
				sdk.WithServerURL(stackURL),
			)
		}),
	)
}

// backfillSDKModule provides the BackfillSDK interface from the Formance SDK client.
func backfillSDKModule() fx.Option {
	return fx.Provide(func(client *sdk.Formance) ingestion.BackfillSDK {
		return api_service.NewSDKFormance(client)
	})
}

func newServeCommand(version string) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "serve",
		RunE: runServer(version),
	}
	cmd.Flags().String(listenFlag, ":8080", "Listening address")
	cmd.Flags().String(stackURLFlag, "", "Stack url")
	cmd.Flags().String(stackClientIDFlag, "", "Stack client ID")
	cmd.Flags().String(stackClientSecretFlag, "", "Stack client secret")
	cmd.Flags().StringSlice(kafkaTopicsFlag, []string{}, "Kafka topics to listen")
	cmd.Flags().String(stackFlag, "", "Stack identifier")
	cmd.Flags().Bool(workerFlag, false, "Enable worker mode (event listener)")
	cmd.Flags().Int(matchingWorkersFlag, matching.DefaultMatchingWorkers, "Number of concurrent matching workers")
	cmd.Flags().String(reportScheduleFlag, reporting.DefaultReportSchedule, "Cron schedule for daily report generation (e.g., '0 6 * * *' for 6:00 UTC)")

	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	auth.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	iam.AddFlags(cmd.Flags())
	service.AddFlags(cmd.Flags())
	licence.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName, cmd.Flags())

	return cmd
}

func runServer(version string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		databaseOptions, err := prepareDatabaseOptions(cmd)
		if err != nil {
			return err
		}

		options := make([]fx.Option, 0)
		options = append(options, databaseOptions)

		// Load OTEL resource (required by go-libs v3 for traces/metrics)
		serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
		resourceAttributes, _ := cmd.Flags().GetStringSlice(otlp.OtelResourceAttributesFlag)
		options = append(options,
			otlp.LoadResource(serviceName, resourceAttributes, version),
			otlptraces.FXModuleFromFlags(cmd),
			otlpmetrics.FXModuleFromFlags(cmd),
			auth.FXModuleFromFlags(cmd),
		)

		listen, _ := cmd.Flags().GetString(listenFlag)

		// Always include elasticsearch and shared ingestion modules
		// because the API service depends on TransactionStore (OpenSearch)
		stack, _ := cmd.Flags().GetString(stackFlag)
		options = append(options,
			elasticsearch.ModuleWithStack(stack),
			ingestion.SharedIngestionModule(ingestion.SharedIngestionConfig{Stack: stack}),
		)

		options = append(options,
			stackClientModule(cmd),
			licence.FXModuleFromFlags(cmd, ServiceName),
		)

		// Add worker (ingestion) module if --worker flag is set
		workerEnabled, _ := cmd.Flags().GetBool(workerFlag)
		if workerEnabled {
			options = append(options, backfillSDKModule())
			options = append(options, api.HTTPModuleWithPublisher(sharedapi.ServiceInfo{
				Version: version,
				Debug:   service.IsDebug(cmd),
			}, listen, stack+".reconciliation"))
		} else {
			options = append(options, api.HTTPModule(sharedapi.ServiceInfo{
				Version: version,
				Debug:   service.IsDebug(cmd),
			}, listen))
		}

		if workerEnabled {

			ingestionOptions, err := prepareIngestionOptions(cmd)
			if err != nil {
				return err
			}
			options = append(options, ingestionOptions)

			// Add report scheduler module (only with worker)
			schedulerOptions, err := prepareReportSchedulerOptions(cmd)
			if err != nil {
				return err
			}
			options = append(options, schedulerOptions)
		}

		return service.New(cmd.OutOrStdout(), options...).Run(cmd)
	}
}

func prepareDatabaseOptions(cmd *cobra.Command) (fx.Option, error) {
	connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
	if err != nil {
		return nil, err
	}

	return storage.Module(*connectionOptions, service.IsDebug(cmd)), nil
}

func prepareIngestionOptions(cmd *cobra.Command) (fx.Option, error) {
	topics, _ := cmd.Flags().GetStringSlice(kafkaTopicsFlag)

	if len(topics) == 0 {
		return nil, fmt.Errorf("--kafka-topics is required when worker mode is enabled")
	}

	stack, _ := cmd.Flags().GetString(stackFlag)

	// Note: elasticsearch.Module() and ingestion.SharedIngestionModule() are already
	// included in runServer() since the API service depends on TransactionStore.
	// Here we only add worker-specific modules.
	options := []fx.Option{
		publish.FXModuleFromFlags(cmd, service.IsDebug(cmd)),
		ingestion.MetricsModule(),
	}

	// Use unified ingestion module that handles all event types
	// This solves the NATS queue group message distribution issue
	unifiedConfig := ingestion.UnifiedIngestionConfig{
		Topics:       topics,
		PublishTopic: stack + ".reconciliation",
	}
	options = append(options, ingestion.UnifiedIngestionModule(unifiedConfig))

	return fx.Options(options...), nil
}

// getReportSchedule returns the report schedule cron expression.
// It checks the environment variable first, then falls back to the CLI flag.
func getReportSchedule(cmd *cobra.Command) string {
	// Check environment variable first
	envValue := os.Getenv(ReportScheduleEnvVar)
	if envValue != "" {
		return envValue
	}

	// Fall back to CLI flag
	flagValue, _ := cmd.Flags().GetString(reportScheduleFlag)
	if flagValue == "" {
		return reporting.DefaultReportSchedule
	}
	return flagValue
}

func prepareReportSchedulerOptions(cmd *cobra.Command) (fx.Option, error) {
	schedule := getReportSchedule(cmd)

	// Validate the cron schedule
	if err := reporting.ParseSchedule(schedule); err != nil {
		return nil, fmt.Errorf("invalid report schedule '%s': %w", schedule, err)
	}

	config := reporting.SchedulerConfig{
		Schedule: schedule,
	}

	return reporting.SchedulerModule(config), nil
}
