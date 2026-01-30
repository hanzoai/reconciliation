package v2

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/reconciliation/internal/api/backend"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

type anomalyResponse struct {
	ID            string     `json:"id"`
	PolicyID      string     `json:"policyID"`
	TransactionID string     `json:"transactionID"`
	Type          string     `json:"type"`
	Severity      string     `json:"severity"`
	State         string     `json:"state"`
	Reason        string     `json:"reason,omitempty"`
	CreatedAt     time.Time  `json:"createdAt"`
	ResolvedAt    *time.Time `json:"resolvedAt,omitempty"`
	ResolvedBy    *string    `json:"resolvedBy,omitempty"`
}

type transactionResponse struct {
	ID         string                 `json:"id"`
	PolicyID   string                 `json:"policyID"`
	Side       string                 `json:"side"`
	Provider   string                 `json:"provider"`
	ExternalID string                 `json:"externalID"`
	Amount     int64                  `json:"amount"`
	Currency   string                 `json:"currency"`
	OccurredAt time.Time              `json:"occurredAt"`
	IngestedAt time.Time              `json:"ingestedAt"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

func transactionToResponse(tx models.Transaction) transactionResponse {
	var policyIDStr string
	if tx.PolicyID != nil {
		policyIDStr = tx.PolicyID.String()
	}
	return transactionResponse{
		ID:         tx.ID.String(),
		PolicyID:   policyIDStr,
		Side:       string(tx.Side),
		Provider:   tx.Provider,
		ExternalID: tx.ExternalID,
		Amount:     tx.Amount,
		Currency:   tx.Currency,
		OccurredAt: tx.OccurredAt,
		IngestedAt: tx.IngestedAt,
		Metadata:   tx.Metadata,
	}
}

type candidateResponse struct {
	Transaction transactionResponse `json:"transaction"`
	Score       float64             `json:"score"`
	Reasons     []string            `json:"reasons"`
}

type anomalyDetailResponse struct {
	ID                string              `json:"id"`
	PolicyID          string              `json:"policyID"`
	TransactionID     string              `json:"transactionID"`
	Type              string              `json:"type"`
	Severity          string              `json:"severity"`
	State             string              `json:"state"`
	Reason            string              `json:"reason,omitempty"`
	CreatedAt         time.Time           `json:"createdAt"`
	ResolvedAt        *time.Time          `json:"resolvedAt,omitempty"`
	ResolvedBy        *string             `json:"resolvedBy,omitempty"`
	SourceTransaction transactionResponse `json:"sourceTransaction"`
	Candidates        []candidateResponse `json:"candidates"`
}

func anomalyToResponse(a models.Anomaly) anomalyResponse {
	policyIDStr := ""
	if a.PolicyID != nil {
		policyIDStr = a.PolicyID.String()
	}
	transactionIDStr := ""
	if a.TransactionID != nil {
		transactionIDStr = a.TransactionID.String()
	}
	return anomalyResponse{
		ID:            a.ID.String(),
		PolicyID:      policyIDStr,
		TransactionID: transactionIDStr,
		Type:          string(a.Type),
		Severity:      string(a.Severity),
		State:         string(a.State),
		Reason:        a.Reason,
		CreatedAt:     a.CreatedAt,
		ResolvedAt:    a.ResolvedAt,
		ResolvedBy:    a.ResolvedBy,
	}
}

func ListAnomaliesHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := storage.GetAnomaliesQuery{}

		if r.URL.Query().Get("cursor") != "" {
			err := bunpaginate.UnmarshalCursor(r.URL.Query().Get("cursor"), &q)
			if err != nil {
				api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'cursor' query param"))
				return
			}
		} else {
			pageSize, err := getPageSize(r)
			if err != nil {
				api.BadRequest(w, ErrValidation, err)
				return
			}

			filters := storage.AnomaliesFilters{}

			// Parse optional policyID filter
			if policyIDStr := r.URL.Query().Get("policyID"); policyIDStr != "" {
				pID, err := uuid.Parse(policyIDStr)
				if err != nil {
					api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'policyID' query param: must be a valid UUID"))
					return
				}
				filters.PolicyID = &pID
			}

			// Parse state filter
			if state := r.URL.Query().Get("state"); state != "" {
				s := models.AnomalyState(state)
				if !s.IsValid() {
					api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'state' query param: must be one of open, resolved"))
					return
				}
				filters.State = &s
			}

			// Parse type filter
			if anomalyType := r.URL.Query().Get("type"); anomalyType != "" {
				t := models.AnomalyType(anomalyType)
				if !t.IsValid() {
					api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'type' query param: must be one of MISSING_ON_PAYMENTS, MISSING_ON_LEDGER, DUPLICATE_LEDGER, AMOUNT_MISMATCH, CURRENCY_MISMATCH"))
					return
				}
				filters.Type = &t
			}

			// Parse severity filter
			if severity := r.URL.Query().Get("severity"); severity != "" {
				sev := models.Severity(severity)
				if !sev.IsValid() {
					api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'severity' query param: must be one of CRITICAL, HIGH, MEDIUM, LOW"))
					return
				}
				filters.Severity = &sev
			}

			q = storage.NewGetAnomaliesQuery(storage.NewPaginatedQueryOptions(filters).WithPageSize(pageSize))
		}

		cursor, err := b.GetService().ListAnomalies(r.Context(), q)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		// Transform to response format
		responses := make([]anomalyResponse, len(cursor.Data))
		for i, anomaly := range cursor.Data {
			responses[i] = anomalyToResponse(anomaly)
		}

		// Build paginated response
		responseCursor := bunpaginate.Cursor[anomalyResponse]{
			PageSize: cursor.PageSize,
			HasMore:  cursor.HasMore,
			Previous: cursor.Previous,
			Next:     cursor.Next,
			Data:     responses,
		}

		api.RenderCursor(w, responseCursor)
	}
}

func ListPolicyAnomaliesHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		policyID := chi.URLParam(r, "policyID")
		if _, err := uuid.Parse(policyID); err != nil {
			api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'policyID' path param"))
			return
		}

		q := storage.GetAnomaliesQuery{}

		if r.URL.Query().Get("cursor") != "" {
			err := bunpaginate.UnmarshalCursor(r.URL.Query().Get("cursor"), &q)
			if err != nil {
				api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'cursor' query param"))
				return
			}
		} else {
			pageSize, err := getPageSize(r)
			if err != nil {
				api.BadRequest(w, ErrValidation, err)
				return
			}

			filters := storage.AnomaliesFilters{}

			// Parse state filter
			if state := r.URL.Query().Get("state"); state != "" {
				s := models.AnomalyState(state)
				if !s.IsValid() {
					api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'state' query param: must be one of open, resolved"))
					return
				}
				filters.State = &s
			}

			// Parse type filter
			if anomalyType := r.URL.Query().Get("type"); anomalyType != "" {
				t := models.AnomalyType(anomalyType)
				if !t.IsValid() {
					api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'type' query param: must be one of MISSING_ON_PAYMENTS, MISSING_ON_LEDGER, DUPLICATE_LEDGER, AMOUNT_MISMATCH, CURRENCY_MISMATCH"))
					return
				}
				filters.Type = &t
			}

			// Parse severity filter
			if severity := r.URL.Query().Get("severity"); severity != "" {
				sev := models.Severity(severity)
				if !sev.IsValid() {
					api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'severity' query param: must be one of CRITICAL, HIGH, MEDIUM, LOW"))
					return
				}
				filters.Severity = &sev
			}

			q = storage.NewGetAnomaliesQuery(storage.NewPaginatedQueryOptions(filters).WithPageSize(pageSize))
		}

		cursor, err := b.GetService().ListPolicyAnomalies(r.Context(), policyID, q)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		// Transform to response format
		responses := make([]anomalyResponse, len(cursor.Data))
		for i, anomaly := range cursor.Data {
			responses[i] = anomalyToResponse(anomaly)
		}

		// Build paginated response
		responseCursor := bunpaginate.Cursor[anomalyResponse]{
			PageSize: cursor.PageSize,
			HasMore:  cursor.HasMore,
			Previous: cursor.Previous,
			Next:     cursor.Next,
			Data:     responses,
		}

		api.RenderCursor(w, responseCursor)
	}
}

func GetAnomalyHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		anomalyID := chi.URLParam(r, "anomalyID")
		if _, err := uuid.Parse(anomalyID); err != nil {
			api.BadRequest(w, ErrValidation, fmt.Errorf("invalid 'anomalyID' path param"))
			return
		}

		result, err := b.GetService().GetAnomalyByID(r.Context(), anomalyID)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		// Build candidates response
		candidates := make([]candidateResponse, len(result.Candidates))
		for i, c := range result.Candidates {
			candidates[i] = candidateResponse{
				Transaction: transactionToResponse(c.Transaction),
				Score:       c.Score,
				Reasons:     c.Reasons,
			}
		}

		response := anomalyDetailResponse{
			ID:                result.Anomaly.ID.String(),
			PolicyID:          "",
			TransactionID:     "",
			Type:              string(result.Anomaly.Type),
			Severity:          string(result.Anomaly.Severity),
			State:             string(result.Anomaly.State),
			Reason:            result.Anomaly.Reason,
			CreatedAt:         result.Anomaly.CreatedAt,
			ResolvedAt:        result.Anomaly.ResolvedAt,
			ResolvedBy:        result.Anomaly.ResolvedBy,
			SourceTransaction: transactionToResponse(result.SourceTransaction),
			Candidates:        candidates,
		}
		if result.Anomaly.PolicyID != nil {
			response.PolicyID = result.Anomaly.PolicyID.String()
		}
		if result.Anomaly.TransactionID != nil {
			response.TransactionID = result.Anomaly.TransactionID.String()
		}

		api.Ok(w, response)
	}
}
