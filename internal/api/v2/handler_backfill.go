package v2

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/reconciliation/internal/api/backend"
	"github.com/formancehq/reconciliation/internal/api/service"
)

func CreateBackfillHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req service.CreateBackfillRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		backfill, err := b.GetService().CreateBackfill(r.Context(), &req)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.Created(w, backfill)
	}
}

func GetBackfillHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "backfillID")

		backfill, err := b.GetService().GetBackfill(r.Context(), id)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.Ok(w, backfill)
	}
}
