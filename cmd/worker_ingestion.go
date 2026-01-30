package cmd

import (
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/aws/iam"
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/health"
	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/licence"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/publish"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/formancehq/reconciliation/internal/ingestion"
	"github.com/formancehq/reconciliation/internal/reporting"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

func newWorkerIngestionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingestion",
		Short: "Run reconciliation ingestion worker (event listener)",
		RunE:  runWorkerIngestion,
	}

	cmd.Flags().String(listenFlag, ":8080", "Listening address")
	cmd.Flags().StringSlice(kafkaTopicsFlag, []string{}, "Kafka topics to listen")
	cmd.Flags().String(stackFlag, "", "Stack identifier")
	cmd.Flags().String(stackURLFlag, "", "Stack url")
	cmd.Flags().String(stackClientIDFlag, "", "Stack client ID")
	cmd.Flags().String(stackClientSecretFlag, "", "Stack client secret")
	cmd.Flags().String(reportScheduleFlag, reporting.DefaultReportSchedule, "Cron schedule for daily report generation (e.g., '0 6 * * *' for 6:00 UTC)")

	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	iam.AddFlags(cmd.Flags())
	service.AddFlags(cmd.Flags())
	licence.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName+"_ingestion", cmd.Flags())

	addAutoMigrateCommand(cmd)

	return cmd
}

func runWorkerIngestion(cmd *cobra.Command, _ []string) error {
	databaseOptions, err := prepareDatabaseOptions(cmd)
	if err != nil {
		return err
	}

	listen, _ := cmd.Flags().GetString(listenFlag)

	options := make([]fx.Option, 0)
	options = append(options, databaseOptions)

	// Load OTEL resource
	serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
	resourceAttributes, _ := cmd.Flags().GetStringSlice(otlp.OtelResourceAttributesFlag)
	options = append(options,
		otlp.LoadResource(serviceName, resourceAttributes, Version),
		otlptraces.FXModuleFromFlags(cmd),
		otlpmetrics.FXModuleFromFlags(cmd),
		licence.FXModuleFromFlags(cmd, ServiceName),
	)

	// Add elasticsearch and shared ingestion modules
	// Required for TransactionStore dependency
	stack, _ := cmd.Flags().GetString(stackFlag)
	if stack == "" {
		stack = elasticsearch.GetStackFromEnv()
	}
	if stack == "" {
		return fmt.Errorf("stack identifier is required for ingestion (use --%s or %s env var)", stackFlag, elasticsearch.StackEnvVar)
	}
	options = append(options,
		elasticsearch.ModuleWithStack(stack),
		ingestion.SharedIngestionModule(ingestion.SharedIngestionConfig{Stack: stack}),
	)

	// Add SDK client module for backfill support
	stackURL, _ := cmd.Flags().GetString(stackURLFlag)
	if stackURL != "" {
		options = append(options, stackClientModule(cmd))
		options = append(options, backfillSDKModule())
	}

	// Add HTTP server for health checks
	options = append(options, ingestionWorkerHTTPModule(listen))

	// Add ingestion module
	ingestionOptions, err := prepareIngestionOptions(cmd)
	if err != nil {
		return err
	}
	options = append(options, ingestionOptions)

	// Add report scheduler module
	schedulerOptions, err := prepareReportSchedulerOptions(cmd)
	if err != nil {
		return err
	}
	options = append(options, schedulerOptions)

	return service.New(cmd.OutOrStdout(), options...).Run(cmd)
}

// ingestionWorkerHTTPModule creates a minimal HTTP server for health checks.
func ingestionWorkerHTTPModule(bind string) fx.Option {
	return fx.Options(
		health.Module(),
		fx.Provide(newIngestionWorkerRouter),
		fx.Invoke(func(r *chi.Mux, lc fx.Lifecycle) {
			lc.Append(httpserver.NewHook(r, httpserver.WithAddress(bind)))
		}),
	)
}

func newIngestionWorkerRouter(healthController *health.HealthController) *chi.Mux {
	r := chi.NewRouter()
	r.Get("/_healthcheck", healthController.Check)
	r.Get("/_health", healthController.Check)
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return r
}
