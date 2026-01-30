package v2_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	sharedapi "github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	rootapi "github.com/formancehq/reconciliation/internal/api"
	"github.com/formancehq/reconciliation/internal/api/service"
	v2 "github.com/formancehq/reconciliation/internal/api/v2"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func stringPtr(s string) *string {
	return &s
}

func ptrUUID(u uuid.UUID) *uuid.UUID {
	return &u
}

// Response types for testing (mirroring internal types)
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

func TestListAnomalies(t *testing.T) {
	t.Parallel()

	policyUUID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	type testCase struct {
		name               string
		queryParams        map[string]string
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
		serviceResponse    *bunpaginate.Cursor[models.Anomaly]
	}

	testCases := []testCase{
		{
			name: "empty list returns 200",
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data:     []models.Anomaly{},
			},
		},
		{
			name: "list with anomalies from multiple policies",
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data: []models.Anomaly{
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
						PolicyID:      ptrUUID(policyUUID),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000020")),
						Type:          models.AnomalyTypeMissingOnPayments,
						Severity:      models.SeverityCritical,
						State:         models.AnomalyStateOpen,
						Reason:        "Transaction not found in payments",
						CreatedAt:     baseTime,
					},
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000011"),
						PolicyID:      ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000002")),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000021")),
						Type:          models.AnomalyTypeAmountMismatch,
						Severity:      models.SeverityHigh,
						State:         models.AnomalyStateOpen,
						Reason:        "Amount differs",
						CreatedAt:     baseTime,
					},
				},
			},
		},
		{
			name: "filter by policyID",
			queryParams: map[string]string{
				"policyID": "00000000-0000-0000-0000-000000000001",
			},
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data:     []models.Anomaly{},
			},
		},
		{
			name: "invalid policyID returns 400",
			queryParams: map[string]string{
				"policyID": "not-a-uuid",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name: "filter by state",
			queryParams: map[string]string{
				"state": "open",
			},
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data:     []models.Anomaly{},
			},
		},
		{
			name: "invalid state returns 400",
			queryParams: map[string]string{
				"state": "INVALID",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name: "invalid type returns 400",
			queryParams: map[string]string{
				"type": "INVALID",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name: "invalid severity returns 400",
			queryParams: map[string]string{
				"severity": "INVALID",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name:               "service error returns 500",
			serviceError:       errors.New("internal error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  sharedapi.ErrorInternal,
		},
	}

	for _, tc := range testCases {
		testCase := tc
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if testCase.expectedStatusCode == 0 {
				testCase.expectedStatusCode = http.StatusOK
			}

			backend, mockService := rootapi.NewTestingBackend(t)

			if testCase.serviceError != nil {
				mockService.EXPECT().
					ListAnomalies(gomock.Any(), gomock.Any()).
					Return(nil, testCase.serviceError)
			} else if testCase.serviceResponse != nil {
				mockService.EXPECT().
					ListAnomalies(gomock.Any(), gomock.Any()).
					Return(testCase.serviceResponse, nil)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			reqURL := "/v2/anomalies"
			if len(testCase.queryParams) > 0 {
				params := url.Values{}
				for k, v := range testCase.queryParams {
					params.Set(k, v)
				}
				reqURL += "?" + params.Encode()
			}

			req := httptest.NewRequest(http.MethodGet, reqURL, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)

			if testCase.expectedStatusCode == http.StatusOK {
				var resp sharedapi.BaseResponse[anomalyResponse]
				sharedapi.Decode(t, rec.Body, &resp)
				require.NotNil(t, resp.Cursor)
				require.NotNil(t, resp.Cursor.Data)

				if testCase.serviceResponse != nil {
					require.Len(t, resp.Cursor.Data, len(testCase.serviceResponse.Data))
				}
			} else {
				errResp := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &errResp)
				require.EqualValues(t, testCase.expectedErrorCode, errResp.ErrorCode)
			}
		})
	}
}

func TestListPolicyAnomalies(t *testing.T) {
	t.Parallel()

	policyID := "00000000-0000-0000-0000-000000000001"
	policyUUID := uuid.MustParse(policyID)
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	type testCase struct {
		name               string
		policyID           string
		queryParams        map[string]string
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
		serviceResponse    *bunpaginate.Cursor[models.Anomaly]
	}

	testCases := []testCase{
		{
			name:     "empty list returns 200 with empty data array",
			policyID: policyID,
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data:     []models.Anomaly{},
			},
		},
		{
			name:     "list with anomalies returns data",
			policyID: policyID,
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data: []models.Anomaly{
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
						PolicyID:      ptrUUID(policyUUID),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000020")),
						Type:          models.AnomalyTypeMissingOnPayments,
						Severity:      models.SeverityCritical,
						State:         models.AnomalyStateOpen,
						Reason:        "Transaction not found in payments",
						CreatedAt:     baseTime,
					},
				},
			},
		},
		{
			name:     "pagination with pageSize works",
			policyID: policyID,
			queryParams: map[string]string{
				"pageSize": "50",
			},
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 50,
				HasMore:  true,
				Next:     "next-cursor-token",
				Data: []models.Anomaly{
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
						PolicyID:      ptrUUID(policyUUID),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000020")),
						Type:          models.AnomalyTypeMissingOnPayments,
						Severity:      models.SeverityCritical,
						State:         models.AnomalyStateOpen,
						CreatedAt:     baseTime,
					},
				},
			},
		},
		{
			name:     "filter by state=open returns only open anomalies",
			policyID: policyID,
			queryParams: map[string]string{
				"state": "open",
			},
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data: []models.Anomaly{
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
						PolicyID:      ptrUUID(policyUUID),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000020")),
						Type:          models.AnomalyTypeMissingOnPayments,
						Severity:      models.SeverityCritical,
						State:         models.AnomalyStateOpen,
						CreatedAt:     baseTime,
					},
				},
			},
		},
		{
			name:     "filter by type=MISSING_ON_PAYMENTS returns only that type",
			policyID: policyID,
			queryParams: map[string]string{
				"type": "MISSING_ON_PAYMENTS",
			},
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data: []models.Anomaly{
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
						PolicyID:      ptrUUID(policyUUID),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000020")),
						Type:          models.AnomalyTypeMissingOnPayments,
						Severity:      models.SeverityCritical,
						State:         models.AnomalyStateOpen,
						CreatedAt:     baseTime,
					},
				},
			},
		},
		{
			name:     "filter by severity=CRITICAL returns only critical anomalies",
			policyID: policyID,
			queryParams: map[string]string{
				"severity": "CRITICAL",
			},
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data: []models.Anomaly{
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
						PolicyID:      ptrUUID(policyUUID),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000020")),
						Type:          models.AnomalyTypeMissingOnPayments,
						Severity:      models.SeverityCritical,
						State:         models.AnomalyStateOpen,
						CreatedAt:     baseTime,
					},
				},
			},
		},
		{
			name:     "combined filters work together",
			policyID: policyID,
			queryParams: map[string]string{
				"state":    "open",
				"type":     "MISSING_ON_PAYMENTS",
				"severity": "CRITICAL",
			},
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data: []models.Anomaly{
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
						PolicyID:      ptrUUID(policyUUID),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000020")),
						Type:          models.AnomalyTypeMissingOnPayments,
						Severity:      models.SeverityCritical,
						State:         models.AnomalyStateOpen,
						CreatedAt:     baseTime,
					},
				},
			},
		},
		{
			name:               "non-existent policy returns 404",
			policyID:           "00000000-0000-0000-0000-000000000099",
			serviceError:       storage.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  sharedapi.ErrorCodeNotFound,
		},
		{
			name:               "invalid policy ID returns 400",
			policyID:           "invalid-uuid",
			serviceError:       service.ErrInvalidID,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrInvalidID,
		},
		{
			name:     "invalid state returns 400",
			policyID: policyID,
			queryParams: map[string]string{
				"state": "INVALID_STATE",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name:     "invalid type returns 400",
			policyID: policyID,
			queryParams: map[string]string{
				"type": "INVALID_TYPE",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name:     "invalid severity returns 400",
			policyID: policyID,
			queryParams: map[string]string{
				"severity": "INVALID_SEVERITY",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name:     "invalid pageSize returns 400",
			policyID: policyID,
			queryParams: map[string]string{
				"pageSize": "invalid",
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrValidation,
		},
		{
			name:               "service error returns 500",
			policyID:           policyID,
			serviceError:       errors.New("internal error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  sharedapi.ErrorInternal,
		},
		{
			name:     "response includes all required fields",
			policyID: policyID,
			serviceResponse: &bunpaginate.Cursor[models.Anomaly]{
				PageSize: 15,
				HasMore:  false,
				Data: []models.Anomaly{
					{
						ID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
						PolicyID:      ptrUUID(policyUUID),
						TransactionID: ptrUUID(uuid.MustParse("00000000-0000-0000-0000-000000000020")),
						Type:          models.AnomalyTypeAmountMismatch,
						Severity:      models.SeverityHigh,
						State:         models.AnomalyStateResolved,
						Reason:        "Amount differs by 10%",
						CreatedAt:     baseTime,
						ResolvedAt:    &baseTime,
						ResolvedBy:    stringPtr("admin@example.com"),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		testCase := tc
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if testCase.expectedStatusCode == 0 {
				testCase.expectedStatusCode = http.StatusOK
			}

			backend, mockService := rootapi.NewTestingBackend(t)

			// Setup mock expectations
			if testCase.serviceError != nil {
				mockService.EXPECT().
					ListPolicyAnomalies(gomock.Any(), testCase.policyID, gomock.Any()).
					Return(nil, testCase.serviceError)
			} else if testCase.serviceResponse != nil {
				mockService.EXPECT().
					ListPolicyAnomalies(gomock.Any(), testCase.policyID, gomock.Any()).
					Return(testCase.serviceResponse, nil)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			// Build URL with query params
			reqURL := "/v2/policies/" + testCase.policyID + "/anomalies"
			if len(testCase.queryParams) > 0 {
				params := url.Values{}
				for k, v := range testCase.queryParams {
					params.Set(k, v)
				}
				reqURL += "?" + params.Encode()
			}

			req := httptest.NewRequest(http.MethodGet, reqURL, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)

			if testCase.expectedStatusCode == http.StatusOK {
				var resp sharedapi.BaseResponse[anomalyResponse]
				sharedapi.Decode(t, rec.Body, &resp)
				require.NotNil(t, resp.Cursor)
				require.NotNil(t, resp.Cursor.Data)

				// Verify pagination fields match the service response
				if testCase.serviceResponse != nil {
					require.Equal(t, testCase.serviceResponse.HasMore, resp.Cursor.HasMore)
					require.Len(t, resp.Cursor.Data, len(testCase.serviceResponse.Data))
					if testCase.serviceResponse.HasMore {
						require.NotEmpty(t, resp.Cursor.Next, "expected next_cursor to be present when HasMore is true")
					}

					// Verify response fields for first item if present
					if len(resp.Cursor.Data) > 0 && len(testCase.serviceResponse.Data) > 0 {
						expectedAnomaly := testCase.serviceResponse.Data[0]
						actualResponse := resp.Cursor.Data[0]

						require.Equal(t, expectedAnomaly.ID.String(), actualResponse.ID)
						require.NotNil(t, expectedAnomaly.PolicyID)
						require.Equal(t, expectedAnomaly.PolicyID.String(), actualResponse.PolicyID)
						require.NotNil(t, expectedAnomaly.TransactionID)
						require.Equal(t, expectedAnomaly.TransactionID.String(), actualResponse.TransactionID)
						require.Equal(t, string(expectedAnomaly.Type), actualResponse.Type)
						require.Equal(t, string(expectedAnomaly.Severity), actualResponse.Severity)
						require.Equal(t, string(expectedAnomaly.State), actualResponse.State)
						require.Equal(t, expectedAnomaly.Reason, actualResponse.Reason)
						require.Equal(t, expectedAnomaly.CreatedAt, actualResponse.CreatedAt)
					}
				}
			} else {
				errResp := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &errResp)
				require.EqualValues(t, testCase.expectedErrorCode, errResp.ErrorCode)
			}
		})
	}
}

func TestGetAnomalyByID(t *testing.T) {
	t.Parallel()

	anomalyID := "00000000-0000-0000-0000-000000000010"
	policyID := "00000000-0000-0000-0000-000000000001"
	transactionID := "00000000-0000-0000-0000-000000000020"
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	type testCase struct {
		name               string
		anomalyID          string
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
		serviceResponse    *service.AnomalyDetailResult
	}

	testCases := []testCase{
		{
			name:      "existing anomaly returns 200 with complete detail",
			anomalyID: anomalyID,
			serviceResponse: &service.AnomalyDetailResult{
				Anomaly: models.Anomaly{
					ID:            uuid.MustParse(anomalyID),
					PolicyID:      ptrUUID(uuid.MustParse(policyID)),
					TransactionID: ptrUUID(uuid.MustParse(transactionID)),
					Type:          models.AnomalyTypeMissingOnPayments,
					Severity:      models.SeverityCritical,
					State:         models.AnomalyStateOpen,
					Reason:        "Transaction not found in payments",
					CreatedAt:     baseTime,
				},
				SourceTransaction: models.Transaction{
					ID:         uuid.MustParse(transactionID),
					PolicyID:   ptrUUID(uuid.MustParse(policyID)),
					Side:       models.TransactionSideLedger,
					Provider:   "stripe",
					ExternalID: "tx_123",
					Amount:     10000,
					Currency:   "USD",
					OccurredAt: baseTime.Add(-1 * time.Hour),
					IngestedAt: baseTime,
					Metadata:   map[string]interface{}{"order_id": "order_456"},
				},
				Candidates: []service.Candidate{},
			},
		},
		{
			name:      "source transaction included in response",
			anomalyID: anomalyID,
			serviceResponse: &service.AnomalyDetailResult{
				Anomaly: models.Anomaly{
					ID:            uuid.MustParse(anomalyID),
					PolicyID:      ptrUUID(uuid.MustParse(policyID)),
					TransactionID: ptrUUID(uuid.MustParse(transactionID)),
					Type:          models.AnomalyTypeMissingOnLedger,
					Severity:      models.SeverityHigh,
					State:         models.AnomalyStateOpen,
					Reason:        "Payment not found in ledger",
					CreatedAt:     baseTime,
				},
				SourceTransaction: models.Transaction{
					ID:         uuid.MustParse(transactionID),
					PolicyID:   ptrUUID(uuid.MustParse(policyID)),
					Side:       models.TransactionSidePayments,
					Provider:   "wise",
					ExternalID: "pay_789",
					Amount:     5000,
					Currency:   "EUR",
					OccurredAt: baseTime.Add(-2 * time.Hour),
					IngestedAt: baseTime,
					Metadata:   map[string]interface{}{"user_id": "user_123"},
				},
				Candidates: []service.Candidate{},
			},
		},
		{
			name:      "candidates included if present",
			anomalyID: anomalyID,
			serviceResponse: &service.AnomalyDetailResult{
				Anomaly: models.Anomaly{
					ID:            uuid.MustParse(anomalyID),
					PolicyID:      ptrUUID(uuid.MustParse(policyID)),
					TransactionID: ptrUUID(uuid.MustParse(transactionID)),
					Type:          models.AnomalyTypeAmountMismatch,
					Severity:      models.SeverityMedium,
					State:         models.AnomalyStateOpen,
					Reason:        "Amount mismatch exceeds tolerance",
					CreatedAt:     baseTime,
				},
				SourceTransaction: models.Transaction{
					ID:         uuid.MustParse(transactionID),
					PolicyID:   ptrUUID(uuid.MustParse(policyID)),
					Side:       models.TransactionSideLedger,
					Provider:   "stripe",
					ExternalID: "tx_abc",
					Amount:     10000,
					Currency:   "USD",
					OccurredAt: baseTime,
					IngestedAt: baseTime,
				},
				Candidates: []service.Candidate{
					{
						Transaction: models.Transaction{
							ID:         uuid.MustParse("00000000-0000-0000-0000-000000000030"),
							PolicyID:   ptrUUID(uuid.MustParse(policyID)),
							Side:       models.TransactionSidePayments,
							Provider:   "stripe",
							ExternalID: "pay_xyz",
							Amount:     9500,
							Currency:   "USD",
							OccurredAt: baseTime,
							IngestedAt: baseTime,
						},
						Score:   0.75,
						Reasons: []string{"Amount close match", "Same currency"},
					},
				},
			},
		},
		{
			name:               "non-existent anomaly returns 404",
			anomalyID:          "00000000-0000-0000-0000-000000000099",
			serviceError:       storage.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  sharedapi.ErrorCodeNotFound,
		},
		{
			name:               "invalid ID returns 400",
			anomalyID:          "invalid-uuid",
			serviceError:       service.ErrInvalidID,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v2.ErrInvalidID,
		},
		{
			name:               "service error returns 500",
			anomalyID:          anomalyID,
			serviceError:       errors.New("internal error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  sharedapi.ErrorInternal,
		},
	}

	for _, tc := range testCases {
		testCase := tc
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if testCase.expectedStatusCode == 0 {
				testCase.expectedStatusCode = http.StatusOK
			}

			backend, mockService := rootapi.NewTestingBackend(t)

			// Setup mock expectations
			if testCase.serviceError != nil {
				mockService.EXPECT().
					GetAnomalyByID(gomock.Any(), testCase.anomalyID).
					Return(nil, testCase.serviceError)
			} else if testCase.serviceResponse != nil {
				mockService.EXPECT().
					GetAnomalyByID(gomock.Any(), testCase.anomalyID).
					Return(testCase.serviceResponse, nil)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			reqURL := "/v2/anomalies/" + testCase.anomalyID

			req := httptest.NewRequest(http.MethodGet, reqURL, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)

			if testCase.expectedStatusCode == http.StatusOK {
				var resp sharedapi.BaseResponse[anomalyDetailResponse]
				sharedapi.Decode(t, rec.Body, &resp)
				require.NotNil(t, resp.Data)

				// Verify anomaly fields
				expectedAnomaly := testCase.serviceResponse.Anomaly
				require.Equal(t, expectedAnomaly.ID.String(), resp.Data.ID)
				require.NotNil(t, expectedAnomaly.PolicyID)
				require.Equal(t, expectedAnomaly.PolicyID.String(), resp.Data.PolicyID)
				require.NotNil(t, expectedAnomaly.TransactionID)
				require.Equal(t, expectedAnomaly.TransactionID.String(), resp.Data.TransactionID)
				require.Equal(t, string(expectedAnomaly.Type), resp.Data.Type)
				require.Equal(t, string(expectedAnomaly.Severity), resp.Data.Severity)
				require.Equal(t, string(expectedAnomaly.State), resp.Data.State)
				require.Equal(t, expectedAnomaly.Reason, resp.Data.Reason)
				require.Equal(t, expectedAnomaly.CreatedAt, resp.Data.CreatedAt)

				// Verify source transaction is included
				expectedTx := testCase.serviceResponse.SourceTransaction
				require.Equal(t, expectedTx.ID.String(), resp.Data.SourceTransaction.ID)
				require.NotNil(t, expectedTx.PolicyID)
				require.Equal(t, expectedTx.PolicyID.String(), resp.Data.SourceTransaction.PolicyID)
				require.Equal(t, string(expectedTx.Side), resp.Data.SourceTransaction.Side)
				require.Equal(t, expectedTx.Provider, resp.Data.SourceTransaction.Provider)
				require.Equal(t, expectedTx.ExternalID, resp.Data.SourceTransaction.ExternalID)
				require.Equal(t, expectedTx.Amount, resp.Data.SourceTransaction.Amount)
				require.Equal(t, expectedTx.Currency, resp.Data.SourceTransaction.Currency)

				// Verify candidates
				require.Len(t, resp.Data.Candidates, len(testCase.serviceResponse.Candidates))
				for i, expectedCandidate := range testCase.serviceResponse.Candidates {
					actualCandidate := resp.Data.Candidates[i]
					require.Equal(t, expectedCandidate.Transaction.ID.String(), actualCandidate.Transaction.ID)
					require.Equal(t, expectedCandidate.Score, actualCandidate.Score)
					require.Equal(t, expectedCandidate.Reasons, actualCandidate.Reasons)
				}
			} else {
				errResp := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &errResp)
				require.EqualValues(t, testCase.expectedErrorCode, errResp.ErrorCode)
			}
		})
	}
}
