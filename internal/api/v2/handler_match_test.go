package v2_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sharedapi "github.com/formancehq/go-libs/v3/api"
	rootapi "github.com/formancehq/reconciliation/internal/api"
	"github.com/formancehq/reconciliation/internal/api/service"
	v2 "github.com/formancehq/reconciliation/internal/api/v2"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// AuthUserHeader constant for tests
const AuthUserHeader = "X-User-ID"

// forceMatchResponse is used only in this file to avoid redeclaration with handler_matcher_test.go
type forceMatchResponse struct {
	ID                     string             `json:"id"`
	PolicyID               string             `json:"policyID"`
	LedgerTransactionIDs   []string           `json:"ledgerTransactionIDs"`
	PaymentsTransactionIDs []string           `json:"paymentsTransactionIDs"`
	Score                  float64            `json:"score"`
	Decision               string             `json:"decision"`
	Explanation            models.Explanation `json:"explanation"`
	CreatedAt              time.Time          `json:"createdAt"`
}

func TestForceMatch(t *testing.T) {
	t.Parallel()

	policyID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	ledgerTxID := uuid.MustParse("00000000-0000-0000-0000-000000000010")
	paymentTxID := uuid.MustParse("00000000-0000-0000-0000-000000000020")
	matchID := uuid.MustParse("00000000-0000-0000-0000-000000000030")
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	type testCase struct {
		name               string
		req                *service.ForceMatchRequest
		resolvedBy         string
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
		serviceResponse    *models.Match
	}

	testCases := []testCase{
		{
			name: "successful force match returns 201",
			req: &service.ForceMatchRequest{
				LedgerTxIDs:  []string{ledgerTxID.String()},
				PaymentTxIDs: []string{paymentTxID.String()},
				Reason:       "Manual reconciliation",
			},
			resolvedBy:         "test-user@example.com",
			expectedStatusCode: http.StatusCreated,
			serviceResponse: &models.Match{
				ID:                     matchID,
				PolicyID:               policyID,
				LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
				PaymentsTransactionIDs: []uuid.UUID{paymentTxID},
				Score:                  1.0,
				Decision:               models.DecisionManualMatch,
				Explanation: models.Explanation{
					Reason: "manual match: Manual reconciliation",
				},
				CreatedAt: baseTime,
			},
		},
		{
			name: "match with decision MANUAL_MATCH",
			req: &service.ForceMatchRequest{
				LedgerTxIDs:  []string{ledgerTxID.String()},
				PaymentTxIDs: []string{paymentTxID.String()},
				Reason:       "Testing manual match decision",
			},
			resolvedBy:         "admin@example.com",
			expectedStatusCode: http.StatusCreated,
			serviceResponse: &models.Match{
				ID:                     matchID,
				PolicyID:               policyID,
				LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
				PaymentsTransactionIDs: []uuid.UUID{paymentTxID},
				Score:                  1.0,
				Decision:               models.DecisionManualMatch,
				Explanation: models.Explanation{
					Reason: "manual match: Testing manual match decision",
				},
				CreatedAt: baseTime,
			},
		},
		{
			name: "resolved_by contains authenticated user",
			req: &service.ForceMatchRequest{
				LedgerTxIDs:  []string{ledgerTxID.String()},
				PaymentTxIDs: []string{paymentTxID.String()},
				Reason:       "User-specific match",
			},
			resolvedBy:         "specific-user@example.com",
			expectedStatusCode: http.StatusCreated,
			serviceResponse: &models.Match{
				ID:                     matchID,
				PolicyID:               policyID,
				LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
				PaymentsTransactionIDs: []uuid.UUID{paymentTxID},
				Score:                  1.0,
				Decision:               models.DecisionManualMatch,
				Explanation: models.Explanation{
					Reason: "manual match: User-specific match",
				},
				CreatedAt: baseTime,
			},
		},
		{
			name: "explanation contains manual match and reason",
			req: &service.ForceMatchRequest{
				LedgerTxIDs:  []string{ledgerTxID.String()},
				PaymentTxIDs: []string{paymentTxID.String()},
				Reason:       "Custom reason for this match",
			},
			resolvedBy:         "test-user@example.com",
			expectedStatusCode: http.StatusCreated,
			serviceResponse: &models.Match{
				ID:                     matchID,
				PolicyID:               policyID,
				LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
				PaymentsTransactionIDs: []uuid.UUID{paymentTxID},
				Score:                  1.0,
				Decision:               models.DecisionManualMatch,
				Explanation: models.Explanation{
					Reason: "manual match: Custom reason for this match",
				},
				CreatedAt: baseTime,
			},
		},
		{
			name: "missing reason returns 400",
			req: &service.ForceMatchRequest{
				LedgerTxIDs:  []string{ledgerTxID.String()},
				PaymentTxIDs: []string{paymentTxID.String()},
				Reason:       "",
			},
			resolvedBy:         "test-user@example.com",
			serviceError:       service.ErrValidation,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name:               "invalid request body returns 400",
			req:                nil,
			resolvedBy:         "test-user@example.com",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrMissingOrInvalidBody,
		},
		{
			name: "non-existent transaction returns 404",
			req: &service.ForceMatchRequest{
				LedgerTxIDs:  []string{"00000000-0000-0000-0000-000000000099"},
				PaymentTxIDs: []string{paymentTxID.String()},
				Reason:       "Testing non-existent transaction",
			},
			resolvedBy:         "test-user@example.com",
			serviceError:       storage.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  sharedapi.ErrorCodeNotFound,
		},
		{
			name: "invalid ledger transaction ID returns 400",
			req: &service.ForceMatchRequest{
				LedgerTxIDs:  []string{"invalid-uuid"},
				PaymentTxIDs: []string{paymentTxID.String()},
				Reason:       "Testing invalid UUID",
			},
			resolvedBy:         "test-user@example.com",
			serviceError:       service.ErrInvalidID,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrInvalidID,
		},
		{
			name: "default resolved_by when no header",
			req: &service.ForceMatchRequest{
				LedgerTxIDs:  []string{ledgerTxID.String()},
				PaymentTxIDs: []string{paymentTxID.String()},
				Reason:       "No user header provided",
			},
			resolvedBy:         "", // Empty means no header, will become "anonymous"
			expectedStatusCode: http.StatusCreated,
			serviceResponse: &models.Match{
				ID:                     matchID,
				PolicyID:               policyID,
				LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
				PaymentsTransactionIDs: []uuid.UUID{paymentTxID},
				Score:                  1.0,
				Decision:               models.DecisionManualMatch,
				Explanation: models.Explanation{
					Reason: "manual match: No user header provided",
				},
				CreatedAt: baseTime,
			},
		},
	}

	for _, tc := range testCases {
		testCase := tc
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend, mockService := rootapi.NewTestingBackend(t)

			// Setup mock expectations
			if testCase.req == nil {
				// Invalid body test - no service call expected
			} else if testCase.serviceError != nil {
				expectedResolvedBy := testCase.resolvedBy
				if expectedResolvedBy == "" {
					expectedResolvedBy = "anonymous"
				}
				mockService.EXPECT().
					ForceMatch(gomock.Any(), policyID.String(), testCase.req, expectedResolvedBy).
					Return(nil, testCase.serviceError)
			} else if testCase.serviceResponse != nil {
				expectedResolvedBy := testCase.resolvedBy
				if expectedResolvedBy == "" {
					expectedResolvedBy = "anonymous"
				}
				mockService.EXPECT().
					ForceMatch(gomock.Any(), policyID.String(), testCase.req, expectedResolvedBy).
					Return(testCase.serviceResponse, nil)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			var body []byte
			if testCase.req != nil {
				body, _ = json.Marshal(testCase.req)
			} else {
				body = []byte("invalid json")
			}

			req := httptest.NewRequest(http.MethodPost, "/v2/policies/"+policyID.String()+"/matches", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			if testCase.resolvedBy != "" {
				req.Header.Set(AuthUserHeader, testCase.resolvedBy)
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)

			if testCase.expectedStatusCode == http.StatusCreated {
				var resp sharedapi.BaseResponse[forceMatchResponse]
				sharedapi.Decode(t, rec.Body, &resp)
				require.NotNil(t, resp.Data)

				// Verify match response fields
				require.Equal(t, testCase.serviceResponse.ID.String(), resp.Data.ID)
				require.Equal(t, testCase.serviceResponse.PolicyID.String(), resp.Data.PolicyID)
				require.Equal(t, string(models.DecisionManualMatch), resp.Data.Decision)
				require.Equal(t, 1.0, resp.Data.Score)
				require.Contains(t, resp.Data.Explanation.Reason, "manual match")
				require.Contains(t, resp.Data.Explanation.Reason, testCase.req.Reason)

				// Verify transaction IDs
				require.Len(t, resp.Data.LedgerTransactionIDs, len(testCase.serviceResponse.LedgerTransactionIDs))
				require.Len(t, resp.Data.PaymentsTransactionIDs, len(testCase.serviceResponse.PaymentsTransactionIDs))
			} else {
				errResp := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &errResp)
				require.EqualValues(t, testCase.expectedErrorCode, errResp.ErrorCode)
			}
		})
	}
}

func TestForceMatchRelatedAnomaliesClosed(t *testing.T) {
	t.Parallel()

	// This test verifies the behavior that related anomalies are closed
	// after a force match. The actual anomaly closing logic is in the service layer,
	// so we just verify the service is called with the correct parameters.

	policyID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	ledgerTxID := uuid.MustParse("00000000-0000-0000-0000-000000000010")
	paymentTxID := uuid.MustParse("00000000-0000-0000-0000-000000000020")
	matchID := uuid.MustParse("00000000-0000-0000-0000-000000000030")
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	backend, mockService := rootapi.NewTestingBackend(t)

	req := &service.ForceMatchRequest{
		LedgerTxIDs:  []string{ledgerTxID.String()},
		PaymentTxIDs: []string{paymentTxID.String()},
		Reason:       "Force match with anomaly closure",
	}

	expectedMatch := &models.Match{
		ID:                     matchID,
		PolicyID:               policyID,
		LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
		PaymentsTransactionIDs: []uuid.UUID{paymentTxID},
		Score:                  1.0,
		Decision:               models.DecisionManualMatch,
		Explanation: models.Explanation{
			Reason: "manual match: Force match with anomaly closure",
		},
		CreatedAt: baseTime,
	}

	mockService.EXPECT().
		ForceMatch(gomock.Any(), policyID.String(), req, "test-user@example.com").
		Return(expectedMatch, nil)

	router := rootapi.NewTestRouter(backend, testing.Verbose())

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/v2/policies/"+policyID.String()+"/matches", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(AuthUserHeader, "test-user@example.com")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, httpReq)

	require.Equal(t, http.StatusCreated, rec.Code)

	var resp sharedapi.BaseResponse[forceMatchResponse]
	sharedapi.Decode(t, rec.Body, &resp)
	require.NotNil(t, resp.Data)
	require.Equal(t, matchID.String(), resp.Data.ID)
	require.Equal(t, string(models.DecisionManualMatch), resp.Data.Decision)
}
