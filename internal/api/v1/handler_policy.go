package v1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/reconciliation/internal/api/backend"
	"github.com/formancehq/reconciliation/internal/api/service"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
)

type policyResponse struct {
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	CreatedAt      time.Time              `json:"createdAt"`
	LedgerName     string                 `json:"ledgerName"`
	LedgerQuery    map[string]interface{} `json:"ledgerQuery"`
	PaymentsPoolID string                 `json:"paymentsPoolID"`

	// Transactional reconciliation fields
	Mode                string                `json:"mode"`
	Topology            string                `json:"topology"`
	DeterministicFields []string              `json:"deterministicFields,omitempty"`
	ScoringConfig       *models.ScoringConfig `json:"scoringConfig,omitempty"`
}

func policyToResponse(policy *models.Policy) *policyResponse {
	return &policyResponse{
		ID:                  policy.ID.String(),
		Name:                policy.Name,
		CreatedAt:           policy.CreatedAt,
		LedgerName:          policy.LedgerName,
		LedgerQuery:         policy.LedgerQuery,
		PaymentsPoolID:      policy.PaymentsPoolID.String(),
		Mode:                policy.Mode,
		Topology:            policy.Topology,
		DeterministicFields: policy.DeterministicFields,
		ScoringConfig:       policy.ScoringConfig,
	}
}

func CreatePolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req service.CreatePolicyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		if err := req.Validate(); err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		policy, err := b.GetService().CreatePolicy(r.Context(), &req)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.Created(w, policyToResponse(policy))
	}
}

func DeletePolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "policyID")

		err := b.GetService().DeletePolicy(r.Context(), id)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.NoContent(w)
	}
}

func GetPolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "policyID")

		policy, err := b.GetService().GetPolicy(r.Context(), id)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.Ok(w, policyToResponse(policy))
	}
}

func UpdatePolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "policyID")

		var req service.UpdatePolicyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		if err := req.Validate(); err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		policy, err := b.GetService().UpdatePolicy(r.Context(), id, &req)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.Ok(w, policyToResponse(policy))
	}
}

func ListPoliciesHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := storage.GetPoliciesQuery{}

		if r.URL.Query().Get(QueryKeyCursor) != "" {
			err := bunpaginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &q)
			if err != nil {
				api.BadRequest(w, ErrValidation, fmt.Errorf("invalid '%s' query param", QueryKeyCursor))
				return
			}
		} else {
			options, err := GetPaginatedQueryOptionsPolicies(r)
			if err != nil {
				api.BadRequest(w, ErrValidation, err)
				return
			}
			q = storage.NewGetPoliciesQuery(*options)
		}

		cursor, err := b.GetService().ListPolicies(r.Context(), q)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.RenderCursor(w, *cursor)
	}
}
