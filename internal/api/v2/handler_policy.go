package v2

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/reconciliation/internal/api/backend"
	"github.com/formancehq/reconciliation/internal/api/service"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
)

func CreatePolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// First, decode the raw request to determine the type
		var rawReq map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&rawReq); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		// Determine policy type
		policyTypeRaw, ok := rawReq["type"]
		if !ok {
			api.BadRequest(w, ErrValidation, errors.New("missing 'type' field"))
			return
		}
		policyType, ok := policyTypeRaw.(string)
		if !ok {
			api.BadRequest(w, ErrValidation, errors.New("'type' must be a string"))
			return
		}

		// Re-encode to handle specific type
		rawBytes, err := json.Marshal(rawReq)
		if err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		switch PolicyType(policyType) {
		case PolicyTypeBalance:
			var req CreateBalancePolicyV2Request
			if err := json.Unmarshal(rawBytes, &req); err != nil {
				api.BadRequest(w, ErrMissingOrInvalidBody, err)
				return
			}

			if err := validateBalancePolicyRequest(&req); err != nil {
				api.BadRequest(w, ErrValidation, err)
				return
			}

			// Convert to service request
			serviceReq := &service.CreatePolicyV2BalanceRequest{
				Name:        req.Name,
				LedgerName:  req.Ledger.Name,
				LedgerQuery: req.Ledger.Query,
				PoolID:      req.Payment.PoolID,
			}

			policy, err := b.GetService().CreatePolicyV2Balance(r.Context(), serviceReq)
			if err != nil {
				handleServiceErrors(w, r, err)
				return
			}

			api.Created(w, PolicyToV2Response(policy))

		case PolicyTypeTransaction:
			var req CreateTransactionPolicyV2Request
			if err := json.Unmarshal(rawBytes, &req); err != nil {
				api.BadRequest(w, ErrMissingOrInvalidBody, err)
				return
			}

			if err := validateTransactionPolicyRequest(&req); err != nil {
				api.BadRequest(w, ErrValidation, err)
				return
			}

			// Convert to service request
			serviceReq := &service.CreatePolicyV2TransactionRequest{
				Name:          req.Name,
				LedgerName:    req.Ledger.Name,
				LedgerQuery:   req.Ledger.Query,
				ConnectorType: req.Payment.ConnectorType,
				ConnectorID:   req.Payment.ConnectorID,
			}
			if req.Matcher != nil {
				serviceReq.DeterministicFields = req.Matcher.DeterministicFields
				serviceReq.ScoringConfig = req.Matcher.Scoring
			}

			policy, err := b.GetService().CreatePolicyV2Transaction(r.Context(), serviceReq)
			if err != nil {
				handleServiceErrors(w, r, err)
				return
			}

			api.Created(w, PolicyToV2Response(policy))

		default:
			api.BadRequest(w, ErrValidation, errors.New("invalid policy type: must be 'balance' or 'transaction'"))
			return
		}
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

		api.Ok(w, PolicyToV2Response(policy))
	}
}

func ListPoliciesHandler(b backend.Backend, getQueryOptions func(r *http.Request) (*storage.PaginatedQueryOptions[storage.PoliciesFilters], error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := storage.GetPoliciesQuery{}

		if r.URL.Query().Get("cursor") != "" {
			err := bunpaginate.UnmarshalCursor(r.URL.Query().Get("cursor"), &q)
			if err != nil {
				api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'cursor' query param"))
				return
			}
		} else {
			options, err := getQueryOptions(r)
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

		// Convert policies to v2 response format
		v2Cursor := convertToV2Cursor(cursor)
		api.RenderCursor(w, v2Cursor)
	}
}

func UpdatePolicyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "policyID")

		var rawReq map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&rawReq); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		rawBytes, err := json.Marshal(rawReq)
		if err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		var req UpdatePolicyV2Request
		if err := json.Unmarshal(rawBytes, &req); err != nil {
			api.BadRequest(w, ErrMissingOrInvalidBody, err)
			return
		}

		// Convert to service request
		serviceReq := &service.UpdatePolicyV2Request{}
		if req.Name != nil {
			serviceReq.Name = req.Name
		}
		if req.Ledger != nil {
			if req.Ledger.Name != "" {
				serviceReq.LedgerName = &req.Ledger.Name
			}
			if req.Ledger.Query != nil {
				serviceReq.LedgerQuery = req.Ledger.Query
			}
		}
		if req.Payment != nil {
			paymentConfig, ok := req.Payment.(map[string]interface{})
			if ok {
				if _, exists := paymentConfig["poolId"]; exists {
					api.BadRequest(w, ErrValidation, fmt.Errorf("poolId cannot be updated for balance policies"))
					return
				}
				if ct, exists := paymentConfig["connectorType"]; exists {
					if s, ok := ct.(string); ok {
						serviceReq.ConnectorType = &s
					}
				}
				if ci, exists := paymentConfig["connectorId"]; exists {
					if s, ok := ci.(string); ok {
						serviceReq.ConnectorID = &s
					}
				}
			}
		}
		if req.Matcher != nil {
			serviceReq.DeterministicFields = req.Matcher.DeterministicFields
			serviceReq.ScoringConfig = req.Matcher.Scoring
		}

		policy, err := b.GetService().UpdatePolicyV2(r.Context(), id, serviceReq)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		api.Ok(w, PolicyToV2Response(policy))
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

func validateBalancePolicyRequest(req *CreateBalancePolicyV2Request) error {
	if req.Name == "" {
		return errors.New("missing name")
	}
	if req.Ledger.Name == "" {
		return errors.New("missing ledger.name")
	}
	if req.Payment.PoolID == "" {
		return errors.New("missing payment.poolId")
	}
	return nil
}

func validateTransactionPolicyRequest(req *CreateTransactionPolicyV2Request) error {
	if req.Name == "" {
		return errors.New("missing name")
	}
	if req.Ledger.Name == "" {
		return errors.New("missing ledger.name")
	}
	// Check mutual exclusivity of connectorType and connectorId
	hasConnectorType := req.Payment.ConnectorType != nil && *req.Payment.ConnectorType != ""
	hasConnectorID := req.Payment.ConnectorID != nil && *req.Payment.ConnectorID != ""

	if !hasConnectorType && !hasConnectorID {
		return errors.New("payment must have either connectorType or connectorId")
	}
	if hasConnectorType && hasConnectorID {
		return errors.New("payment.connectorType and payment.connectorId are mutually exclusive")
	}

	// Validate matcher config if provided
	if req.Matcher != nil && req.Matcher.Scoring != nil {
		if err := validateScoringConfig(req.Matcher.Scoring); err != nil {
			return errors.Wrap(err, "invalid matcher.scoring")
		}
	}

	return nil
}

func validateScoringConfig(sc *models.ScoringConfig) error {
	if sc.TimeWindowHours < 0 {
		return errors.New("timeWindowHours must be non-negative")
	}
	if sc.AmountTolerancePercent < 0 || sc.AmountTolerancePercent > 100 {
		return errors.New("amountTolerancePercent must be between 0 and 100")
	}
	if sc.Weights != nil {
		if sc.Weights.Amount < 0 || sc.Weights.Date < 0 || sc.Weights.Metadata < 0 {
			return errors.New("weights must be non-negative")
		}
	}
	if sc.Thresholds != nil {
		if sc.Thresholds.AutoMatch < 0 || sc.Thresholds.AutoMatch > 1 {
			return errors.New("autoMatch threshold must be between 0 and 1")
		}
		if sc.Thresholds.Review < 0 || sc.Thresholds.Review > 1 {
			return errors.New("review threshold must be between 0 and 1")
		}
		if sc.Thresholds.Review > sc.Thresholds.AutoMatch {
			return errors.New("review threshold must be less than or equal to autoMatch threshold")
		}
	}
	return nil
}

func convertToV2Cursor(cursor *bunpaginate.Cursor[models.Policy]) bunpaginate.Cursor[interface{}] {
	var data []interface{}
	for _, p := range cursor.Data {
		data = append(data, PolicyToV2Response(&p))
	}

	return bunpaginate.Cursor[interface{}]{
		PageSize: cursor.PageSize,
		HasMore:  cursor.HasMore,
		Previous: cursor.Previous,
		Next:     cursor.Next,
		Data:     data,
	}
}
