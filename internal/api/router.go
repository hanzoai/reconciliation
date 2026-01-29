package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/service"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/health"
	"github.com/formancehq/reconciliation/internal/api/backend"
	v1 "github.com/formancehq/reconciliation/internal/api/v1"
	v2 "github.com/formancehq/reconciliation/internal/api/v2"
)

// NewRouter creates the main API router with V1 and V2 routes.
// Exported for use in tests.
func NewRouter(
	b backend.Backend,
	serviceInfo api.ServiceInfo,
	authenticator auth.Authenticator,
	healthController *health.HealthController) *chi.Mux {
	r := chi.NewRouter()
	r.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			handler.ServeHTTP(w, r)
		})
	})
	r.Get("/_healthcheck", healthController.Check)
	r.Get("/_info", api.InfoHandler(serviceInfo))

	r.Group(func(r chi.Router) {
		r.Use(auth.Middleware(authenticator))
		r.Use(service.OTLPMiddleware("reconciliation", serviceInfo.Debug))

		// V1 API routes
		r.Get("/reconciliations/{reconciliationID}", v1.GetReconciliationHandler(b))
		r.Get("/reconciliations", v1.ListReconciliationsHandler(b))

		r.Post("/policies", v1.CreatePolicyHandler(b))
		r.Get("/policies", v1.ListPoliciesHandler(b))
		r.Delete("/policies/{policyID}", v1.DeletePolicyHandler(b))
		r.Get("/policies/{policyID}", v1.GetPolicyHandler(b))
		r.Patch("/policies/{policyID}", v1.UpdatePolicyHandler(b))
		r.Post("/policies/{policyID}/reconciliation", v1.ReconciliationHandler(b))
		r.Get("/policies/{policyID}/reports/latest", v1.GetLatestPolicyReportHandler(b))

		// V2 API routes
		r.Route("/v2", func(r chi.Router) {
			r.Post("/policies", v2.CreatePolicyHandler(b))
			r.Get("/policies", v2.ListPoliciesHandler(b, v1.GetPaginatedQueryOptionsPolicies))
			r.Get("/policies/{policyID}", v2.GetPolicyHandler(b))
			r.Patch("/policies/{policyID}", v2.UpdatePolicyHandler(b))
			r.Delete("/policies/{policyID}", v2.DeletePolicyHandler(b))
			r.Get("/policies/{policyID}/anomalies", v2.ListPolicyAnomaliesHandler(b))
			r.Post("/policies/{policyID}/matches", v2.ForceMatchHandler(b))
			r.Post("/policies/{policyID}/match", v2.MatchHandler(b))
			r.Post("/policies/{policyID}/trigger-matching", v2.TriggerPolicyMatchHandler(b))

			r.Get("/anomalies", v2.ListAnomaliesHandler(b))
			r.Get("/anomalies/{anomalyID}", v2.GetAnomalyHandler(b))

			r.Post("/backfill", v2.CreateBackfillHandler(b))
			r.Get("/backfill/{backfillID}", v2.GetBackfillHandler(b))
		})
	})

	return r
}
