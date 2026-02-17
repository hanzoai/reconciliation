package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/reconciliation/internal/api/backend"
	"github.com/formancehq/reconciliation/internal/api/service"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
)

type policyResponse struct {
	ID              string                 `json:"id"`
	Version         int64                  `json:"version"`
	Lifecycle       string                 `json:"lifecycle"`
	Name            string                 `json:"name"`
	CreatedAt       time.Time              `json:"createdAt"`
	LedgerName      string                 `json:"ledgerName"`
	LedgerQuery     map[string]interface{} `json:"ledgerQuery"`
	PaymentsPoolID  string                 `json:"paymentsPoolID"`
	Mode            string                 `json:"mode"`
	AssertionConfig map[string]interface{} `json:"assertionConfig"`
}

func createPolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req service.CreatePolicyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		policy, err := b.GetService().CreatePolicy(r.Context(), &req)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		data := &policyResponse{
			ID:              policy.PolicyID.String(),
			Version:         policy.Version,
			Lifecycle:       models.NormalizePolicyLifecycle(policy.Lifecycle).String(),
			Name:            policy.Name,
			CreatedAt:       policy.CreatedAt,
			LedgerName:      policy.LedgerName,
			LedgerQuery:     policy.LedgerQuery,
			PaymentsPoolID:  policy.PaymentsPoolID.String(),
			Mode:            models.NormalizeAssertionMode(policy.AssertionMode).String(),
			AssertionConfig: policy.AssertionConfig,
		}

		api.Created(w, data)
	}
}

func createPolicyVersionHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "policyID")

		var req service.CreatePolicyVersionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		policy, err := b.GetService().CreatePolicyVersion(r.Context(), id, &req)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		data := &policyResponse{
			ID:              policy.PolicyID.String(),
			Version:         policy.Version,
			Lifecycle:       models.NormalizePolicyLifecycle(policy.Lifecycle).String(),
			Name:            policy.Name,
			CreatedAt:       policy.CreatedAt,
			LedgerName:      policy.LedgerName,
			LedgerQuery:     policy.LedgerQuery,
			PaymentsPoolID:  policy.PaymentsPoolID.String(),
			Mode:            models.NormalizeAssertionMode(policy.AssertionMode).String(),
			AssertionConfig: policy.AssertionConfig,
		}

		api.Created(w, data)
	}
}

func archivePolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "policyID")

		err := b.GetService().ArchivePolicy(r.Context(), id)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.NoContent(w)
	}
}

func getPolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "policyID")

		policy, err := b.GetService().GetPolicy(r.Context(), id)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		data := &policyResponse{
			ID:              policy.PolicyID.String(),
			Version:         policy.Version,
			Lifecycle:       models.NormalizePolicyLifecycle(policy.Lifecycle).String(),
			Name:            policy.Name,
			CreatedAt:       policy.CreatedAt,
			LedgerName:      policy.LedgerName,
			LedgerQuery:     policy.LedgerQuery,
			PaymentsPoolID:  policy.PaymentsPoolID.String(),
			Mode:            models.NormalizeAssertionMode(policy.AssertionMode).String(),
			AssertionConfig: policy.AssertionConfig,
		}

		api.Ok(w, data)
	}
}

func listPoliciesHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := storage.GetPoliciesQuery{}

		if r.URL.Query().Get(QueryKeyCursor) != "" {
			err := bunpaginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &q)
			if err != nil {
				api.BadRequest(w, ErrValidation, fmt.Errorf("invalid '%s' query param", QueryKeyCursor))
				return
			}
		} else {
			options, err := getPaginatedQueryOptionsPolicies(r)
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

		api.RenderCursor(w, *bunpaginate.MapCursor(cursor, func(policy models.Policy) policyResponse {
			return policyResponse{
				ID:              policy.PolicyID.String(),
				Version:         policy.Version,
				Lifecycle:       models.NormalizePolicyLifecycle(policy.Lifecycle).String(),
				Name:            policy.Name,
				CreatedAt:       policy.CreatedAt,
				LedgerName:      policy.LedgerName,
				LedgerQuery:     policy.LedgerQuery,
				PaymentsPoolID:  policy.PaymentsPoolID.String(),
				Mode:            models.NormalizeAssertionMode(policy.AssertionMode).String(),
				AssertionConfig: policy.AssertionConfig,
			}
		}))
	}
}
