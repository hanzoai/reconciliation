package cmd

import (
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
	"github.com/formancehq/reconciliation/internal/matching"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

func newWorkerMatchingCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "matching",
		Short: "Run reconciliation matching worker",
		RunE:  runWorkerMatching,
	}

	cmd.Flags().String(listenFlag, ":8080", "Listening address")
	cmd.Flags().StringSlice(kafkaTopicsFlag, []string{}, "Kafka topics to listen")
	cmd.Flags().String(stackFlag, "", "Stack identifier")
	cmd.Flags().Int(matchingWorkersFlag, matching.DefaultConsumerWorkers, "Number of concurrent matching workers")

	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	iam.AddFlags(cmd.Flags())
	service.AddFlags(cmd.Flags())
	licence.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName+"_matching", cmd.Flags())

	return cmd
}

func runWorkerMatching(cmd *cobra.Command, _ []string) error {
	databaseOptions, err := prepareDatabaseOptions(cmd)
	if err != nil {
		return err
	}

	listen, _ := cmd.Flags().GetString(listenFlag)
	stack, _ := cmd.Flags().GetString(stackFlag)
	matchingWorkers, _ := cmd.Flags().GetInt(matchingWorkersFlag)

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

	// Add publish module (provides message.Subscriber)
	options = append(options, publish.FXModuleFromFlags(cmd, service.IsDebug(cmd)))

	// Add elasticsearch + shared ingestion modules (provides TransactionStore)
	options = append(options,
		elasticsearch.Module(),
		ingestion.SharedIngestionModule(ingestion.SharedIngestionConfig{Stack: stack}),
	)

	// Provide OrchestratorFactory as matching.OrchestratorProvider
	options = append(options, fx.Provide(func(
		txStore storage.TransactionStore,
		matchRepo storage.MatchRepository,
		anomalyRepo storage.AnomalyRepository,
		esClient *elasticsearch.Client,
	) matching.OrchestratorProvider {
		return ingestion.NewOrchestratorFactory(txStore, matchRepo, anomalyRepo, esClient, stack)
	}))

	// Add matching consumer module
	topic := stack + ".reconciliation"
	options = append(options, matching.MatchingConsumerModule(matching.MatchingConsumerConfig{
		Topic:      topic,
		NumWorkers: matchingWorkers,
	}))

	// Add HTTP server for health checks
	options = append(options, matchingWorkerHTTPModule(listen))

	return service.New(cmd.OutOrStdout(), options...).Run(cmd)
}

// matchingWorkerHTTPModule creates a minimal HTTP server for health checks.
func matchingWorkerHTTPModule(bind string) fx.Option {
	return fx.Options(
		health.Module(),
		fx.Provide(newMatchingWorkerRouter),
		fx.Invoke(func(r *chi.Mux, lc fx.Lifecycle) {
			lc.Append(httpserver.NewHook(r, httpserver.WithAddress(bind)))
		}),
	)
}

func newMatchingWorkerRouter(healthController *health.HealthController) *chi.Mux {
	r := chi.NewRouter()
	r.Get("/_healthcheck", healthController.Check)
	r.Get("/_health", healthController.Check)
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return r
}
