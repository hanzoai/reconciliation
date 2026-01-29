package v2

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/reconciliation/internal/api/backend"
	"github.com/formancehq/reconciliation/internal/api/service"
	"github.com/formancehq/reconciliation/internal/models"
)

type matchResponse struct {
	ID                     string             `json:"id"`
	PolicyID               string             `json:"policyID"`
	LedgerTransactionIDs   []string           `json:"ledgerTransactionIDs"`
	PaymentsTransactionIDs []string           `json:"paymentsTransactionIDs"`
	Score                  float64            `json:"score"`
	Decision               string             `json:"decision"`
	Explanation            models.Explanation `json:"explanation"`
	CreatedAt              time.Time          `json:"createdAt"`
}

func matchToResponse(m models.Match) matchResponse {
	ledgerTxIDs := make([]string, len(m.LedgerTransactionIDs))
	for i, id := range m.LedgerTransactionIDs {
		ledgerTxIDs[i] = id.String()
	}

	paymentTxIDs := make([]string, len(m.PaymentsTransactionIDs))
	for i, id := range m.PaymentsTransactionIDs {
		paymentTxIDs[i] = id.String()
	}

	return matchResponse{
		ID:                     m.ID.String(),
		PolicyID:               m.PolicyID.String(),
		LedgerTransactionIDs:   ledgerTxIDs,
		PaymentsTransactionIDs: paymentTxIDs,
		Score:                  m.Score,
		Decision:               string(m.Decision),
		Explanation:            m.Explanation,
		CreatedAt:              m.CreatedAt,
	}
}

const (
	// AuthUserHeader is the header containing the authenticated user identifier
	AuthUserHeader = "X-User-ID"
)

func ForceMatchHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		policyID := chi.URLParam(r, "policyID")
		if policyID == "" {
			api.BadRequest(w, ErrValidation, errors.New("missing policyID"))
			return
		}

		var req service.ForceMatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		// Get the authenticated user from the header
		resolvedBy := r.Header.Get(AuthUserHeader)
		if resolvedBy == "" {
			resolvedBy = "anonymous"
		}

		match, err := b.GetService().ForceMatch(r.Context(), policyID, &req, resolvedBy)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.Created(w, matchToResponse(*match))
	}
}

// TriggerPolicyMatchHandler handles POST /v2/policies/{policyID}/trigger-matching.
// It triggers matching for all transactions belonging to the policy.
func TriggerPolicyMatchHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		policyID := chi.URLParam(r, "policyID")
		if policyID == "" {
			api.BadRequest(w, ErrValidation, errors.New("missing policyID"))
			return
		}

		results, err := b.GetService().TriggerPolicyMatching(r.Context(), policyID)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.Ok(w, map[string]interface{}{
			"results": results,
		})
	}
}

// MatchHandler handles the POST /v2/policies/{policyID}/match endpoint.
// It runs matching for a list of transactions using the specified policy.
func MatchHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		policyID := chi.URLParam(r, "policyID")
		if policyID == "" {
			api.BadRequest(w, ErrValidation, errors.New("missing policyID"))
			return
		}

		var req MatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		if len(req.TransactionIDs) == 0 {
			api.BadRequest(w, ErrValidation, errors.New("missing transactionIds"))
			return
		}

		// Call service
		results, err := b.GetService().MatchTransactions(r.Context(), policyID, req.TransactionIDs)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		// Return results directly
		api.Ok(w, map[string]interface{}{
			"results": results,
		})
	}
}
