package v1_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sharedapi "github.com/formancehq/go-libs/v3/api"
	rootapi "github.com/formancehq/reconciliation/internal/api"
	"github.com/formancehq/reconciliation/internal/api/service"
	v1 "github.com/formancehq/reconciliation/internal/api/v1"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type reportResponse struct {
	ID                string           `json:"id"`
	PolicyID          string           `json:"policyID"`
	PeriodStart       time.Time        `json:"periodStart"`
	PeriodEnd         time.Time        `json:"periodEnd"`
	TotalTransactions int64            `json:"totalTransactions"`
	MatchedCount      int64            `json:"matchedCount"`
	MatchRate         float64          `json:"matchRate"`
	AnomaliesByType   map[string]int64 `json:"anomaliesByType"`
	GeneratedAt       time.Time        `json:"generatedAt"`
}

func TestGetLatestPolicyReport(t *testing.T) {
	t.Parallel()

	policyID := "00000000-0000-0000-0000-000000000001"
	policyUUID := uuid.MustParse(policyID)
	reportID := "00000000-0000-0000-0000-000000000010"
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	periodStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)

	type testCase struct {
		name               string
		policyID           string
		acceptHeader       string
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
		serviceResponse    *models.Report
	}

	testCases := []testCase{
		{
			name:     "existing report returns 200 with complete JSON",
			policyID: policyID,
			serviceResponse: &models.Report{
				ID:                uuid.MustParse(reportID),
				PolicyID:          &policyUUID,
				PeriodStart:       periodStart,
				PeriodEnd:         periodEnd,
				TotalTransactions: 100,
				MatchedCount:      90,
				MatchRate:         0.9,
				AnomaliesByType: map[string]int64{
					"MISSING_ON_PAYMENTS": 5,
					"MISSING_ON_LEDGER":   3,
					"AMOUNT_MISMATCH":     2,
				},
				GeneratedAt: baseTime,
			},
		},
		{
			name:         "header Accept text/csv returns 200 with valid CSV",
			policyID:     policyID,
			acceptHeader: "text/csv",
			serviceResponse: &models.Report{
				ID:                uuid.MustParse(reportID),
				PolicyID:          &policyUUID,
				PeriodStart:       periodStart,
				PeriodEnd:         periodEnd,
				TotalTransactions: 100,
				MatchedCount:      90,
				MatchRate:         0.9,
				AnomaliesByType: map[string]int64{
					"MISSING_ON_PAYMENTS": 5,
					"MISSING_ON_LEDGER":   3,
				},
				GeneratedAt: baseTime,
			},
		},
		{
			name:               "no report returns 404",
			policyID:           policyID,
			serviceError:       storage.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  sharedapi.ErrorCodeNotFound,
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
			expectedErrorCode:  v1.ErrInvalidID,
		},
		{
			name:               "service error returns 500",
			policyID:           policyID,
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
					GetLatestPolicyReport(gomock.Any(), testCase.policyID).
					Return(nil, testCase.serviceError)
			} else if testCase.serviceResponse != nil {
				mockService.EXPECT().
					GetLatestPolicyReport(gomock.Any(), testCase.policyID).
					Return(testCase.serviceResponse, nil)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			reqURL := "/policies/" + testCase.policyID + "/reports/latest"

			req := httptest.NewRequest(http.MethodGet, reqURL, nil)
			if testCase.acceptHeader != "" {
				req.Header.Set("Accept", testCase.acceptHeader)
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)

			if testCase.expectedStatusCode == http.StatusOK {
				if testCase.acceptHeader == "text/csv" {
					// Verify CSV response
					require.Equal(t, "text/csv; charset=utf-8", rec.Header().Get("Content-Type"))
					require.Contains(t, rec.Header().Get("Content-Disposition"), "attachment; filename=report_")

					body := rec.Body.String()
					// Verify CSV structure
					require.Contains(t, body, "field,value")
					require.Contains(t, body, "id,"+reportID)
					require.Contains(t, body, "policyID,"+policyID)
					require.Contains(t, body, "totalTransactions,100")
					require.Contains(t, body, "matchedCount,90")
					require.Contains(t, body, "matchRate,0.900000")
				} else {
					// Verify JSON response
					var resp sharedapi.BaseResponse[reportResponse]
					sharedapi.Decode(t, rec.Body, &resp)
					require.NotNil(t, resp.Data)

					// Verify all fields
					require.Equal(t, reportID, resp.Data.ID)
					require.Equal(t, policyID, resp.Data.PolicyID)
					require.Equal(t, periodStart, resp.Data.PeriodStart)
					require.Equal(t, periodEnd, resp.Data.PeriodEnd)
					require.Equal(t, int64(100), resp.Data.TotalTransactions)
					require.Equal(t, int64(90), resp.Data.MatchedCount)
					require.Equal(t, 0.9, resp.Data.MatchRate)
					require.Equal(t, baseTime, resp.Data.GeneratedAt)
					require.NotNil(t, resp.Data.AnomaliesByType)
				}
			} else {
				errResp := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &errResp)
				require.EqualValues(t, testCase.expectedErrorCode, errResp.ErrorCode)
			}
		})
	}
}

func TestGetLatestPolicyReportMultipleReports(t *testing.T) {
	t.Parallel()

	policyID := "00000000-0000-0000-0000-000000000001"
	policyUUID := uuid.MustParse(policyID)
	reportID := "00000000-0000-0000-0000-000000000020"
	newerTime := time.Date(2024, 2, 15, 10, 0, 0, 0, time.UTC)
	periodStart := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2024, 2, 29, 23, 59, 59, 0, time.UTC)

	// Test that when multiple reports exist, the most recent one is returned
	t.Run("multiple reports returns most recent", func(t *testing.T) {
		t.Parallel()

		backend, mockService := rootapi.NewTestingBackend(t)

		// The service should return the most recent report
		mostRecentReport := &models.Report{
			ID:                uuid.MustParse(reportID),
			PolicyID:          &policyUUID,
			PeriodStart:       periodStart,
			PeriodEnd:         periodEnd,
			TotalTransactions: 200,
			MatchedCount:      180,
			MatchRate:         0.9,
			AnomaliesByType: map[string]int64{
				"MISSING_ON_PAYMENTS": 10,
			},
			GeneratedAt: newerTime,
		}

		mockService.EXPECT().
			GetLatestPolicyReport(gomock.Any(), policyID).
			Return(mostRecentReport, nil)

		router := rootapi.NewTestRouter(backend, testing.Verbose())

		reqURL := "/policies/" + policyID + "/reports/latest"
		req := httptest.NewRequest(http.MethodGet, reqURL, nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)

		var resp sharedapi.BaseResponse[reportResponse]
		sharedapi.Decode(t, rec.Body, &resp)
		require.NotNil(t, resp.Data)

		// Verify the most recent report is returned
		require.Equal(t, reportID, resp.Data.ID)
		require.Equal(t, newerTime, resp.Data.GeneratedAt)
		require.Equal(t, int64(200), resp.Data.TotalTransactions)
	})
}

func TestGetLatestPolicyReportCSVFormat(t *testing.T) {
	t.Parallel()

	policyID := "00000000-0000-0000-0000-000000000001"
	policyUUID := uuid.MustParse(policyID)
	reportID := "00000000-0000-0000-0000-000000000010"
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	periodStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)

	t.Run("CSV contains all required fields", func(t *testing.T) {
		t.Parallel()

		backend, mockService := rootapi.NewTestingBackend(t)

		report := &models.Report{
			ID:                uuid.MustParse(reportID),
			PolicyID:          &policyUUID,
			PeriodStart:       periodStart,
			PeriodEnd:         periodEnd,
			TotalTransactions: 100,
			MatchedCount:      90,
			MatchRate:         0.9,
			AnomaliesByType: map[string]int64{
				"MISSING_ON_PAYMENTS": 5,
				"MISSING_ON_LEDGER":   3,
				"AMOUNT_MISMATCH":     2,
			},
			GeneratedAt: baseTime,
		}

		mockService.EXPECT().
			GetLatestPolicyReport(gomock.Any(), policyID).
			Return(report, nil)

		router := rootapi.NewTestRouter(backend, testing.Verbose())

		reqURL := "/policies/" + policyID + "/reports/latest"
		req := httptest.NewRequest(http.MethodGet, reqURL, nil)
		req.Header.Set("Accept", "text/csv")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)

		body := rec.Body.String()
		lines := strings.Split(strings.TrimSpace(body), "\n")

		// Verify header
		require.Equal(t, "field,value", lines[0])

		// Verify all required fields are present
		require.Contains(t, body, "id,")
		require.Contains(t, body, "policyID,")
		require.Contains(t, body, "periodStart,")
		require.Contains(t, body, "periodEnd,")
		require.Contains(t, body, "totalTransactions,")
		require.Contains(t, body, "matchedCount,")
		require.Contains(t, body, "matchRate,")
		require.Contains(t, body, "generatedAt,")

		// Verify anomalies by type are included
		require.Contains(t, body, "anomalies_AMOUNT_MISMATCH,2")
		require.Contains(t, body, "anomalies_MISSING_ON_LEDGER,3")
		require.Contains(t, body, "anomalies_MISSING_ON_PAYMENTS,5")
	})

	t.Run("CSV with no anomalies", func(t *testing.T) {
		t.Parallel()

		backend, mockService := rootapi.NewTestingBackend(t)

		report := &models.Report{
			ID:                uuid.MustParse(reportID),
			PolicyID:          &policyUUID,
			PeriodStart:       periodStart,
			PeriodEnd:         periodEnd,
			TotalTransactions: 50,
			MatchedCount:      50,
			MatchRate:         1.0,
			AnomaliesByType:   map[string]int64{},
			GeneratedAt:       baseTime,
		}

		mockService.EXPECT().
			GetLatestPolicyReport(gomock.Any(), policyID).
			Return(report, nil)

		router := rootapi.NewTestRouter(backend, testing.Verbose())

		reqURL := "/policies/" + policyID + "/reports/latest"
		req := httptest.NewRequest(http.MethodGet, reqURL, nil)
		req.Header.Set("Accept", "text/csv")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)

		body := rec.Body.String()
		// Should not contain anomaly lines
		require.NotContains(t, body, "anomalies_")
		// But should contain other fields
		require.Contains(t, body, "matchRate,1.000000")
	})
}
