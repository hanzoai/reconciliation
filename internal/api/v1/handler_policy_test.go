package v1_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

type policyResponse struct {
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	CreatedAt      time.Time              `json:"createdAt"`
	LedgerName     string                 `json:"ledgerName"`
	LedgerQuery    map[string]interface{} `json:"ledgerQuery"`
	PaymentsPoolID string                 `json:"paymentsPoolID"`
	Mode           string                 `json:"mode"`
	Topology       string                 `json:"topology"`
}

func TestCreatePolicy(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name               string
		req                *service.CreatePolicyRequest
		invalidBody        bool
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
	}

	testCases := []testCase{
		{
			name: "nominal",
			req: &service.CreatePolicyRequest{
				Name:           "test",
				LedgerName:     "test",
				LedgerQuery:    map[string]interface{}{},
				PaymentsPoolID: "00000000-0000-0000-0000-000000000000",
			},
		},
		{
			name:               "missing body",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrMissingOrInvalidBody,
		},
		{
			name:               "invalid body",
			invalidBody:        true,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrMissingOrInvalidBody,
		},
		{
			name: "service error validation",
			req: &service.CreatePolicyRequest{
				Name:           "test",
				LedgerName:     "test",
				LedgerQuery:    map[string]interface{}{},
				PaymentsPoolID: "00000000-0000-0000-0000-000000000000",
			},
			serviceError:       service.ErrValidation,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrValidation,
		},
		{
			name: "service error invalid id",
			req: &service.CreatePolicyRequest{
				Name:           "test",
				LedgerName:     "test",
				LedgerQuery:    map[string]interface{}{},
				PaymentsPoolID: "00000000-0000-0000-0000-000000000000",
			},
			serviceError:       service.ErrInvalidID,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrInvalidID,
		},
		{
			name: "storage error not found",
			req: &service.CreatePolicyRequest{
				Name:           "test",
				LedgerName:     "test",
				LedgerQuery:    map[string]interface{}{},
				PaymentsPoolID: "00000000-0000-0000-0000-000000000000",
			},
			serviceError:       storage.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  sharedapi.ErrorCodeNotFound,
		},
		{
			name: "service error other error",
			req: &service.CreatePolicyRequest{
				Name:           "test",
				LedgerName:     "test",
				LedgerQuery:    map[string]interface{}{},
				PaymentsPoolID: "00000000-0000-0000-0000-000000000000",
			},
			serviceError:       errors.New("some error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  sharedapi.ErrorInternal,
		},
	}

	for _, tc := range testCases {
		testCase := tc
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if testCase.expectedStatusCode == 0 {
				testCase.expectedStatusCode = http.StatusCreated
			}

			var policyServiceResponse models.Policy
			if testCase.req != nil {
				policyServiceResponse = models.Policy{
					ID:             uuid.New(),
					CreatedAt:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					Name:           testCase.req.Name,
					LedgerName:     testCase.req.LedgerName,
					LedgerQuery:    testCase.req.LedgerQuery,
					PaymentsPoolID: uuid.MustParse(testCase.req.PaymentsPoolID),
				}
			}

			expectedPolicyResponse := &policyResponse{
				ID:             policyServiceResponse.ID.String(),
				Name:           policyServiceResponse.Name,
				CreatedAt:      policyServiceResponse.CreatedAt,
				LedgerName:     policyServiceResponse.LedgerName,
				LedgerQuery:    policyServiceResponse.LedgerQuery,
				PaymentsPoolID: policyServiceResponse.PaymentsPoolID.String(),
			}

			backend, mockService := rootapi.NewTestingBackend(t)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				mockService.EXPECT().
					CreatePolicy(gomock.Any(), testCase.req).
					Return(&policyServiceResponse, nil)
			}
			if testCase.serviceError != nil {
				mockService.EXPECT().
					CreatePolicy(gomock.Any(), testCase.req).
					Return(nil, testCase.serviceError)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			var body []byte
			if testCase.invalidBody {
				body = []byte("invalid")
			} else if testCase.req != nil {
				var err error
				body, err = json.Marshal(testCase.req)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				var resp sharedapi.BaseResponse[policyResponse]
				sharedapi.Decode(t, rec.Body, &resp)
				require.Equal(t, expectedPolicyResponse, resp.Data)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestDeletePolicy(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name               string
		policyID           string
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
	}

	testCases := []testCase{
		{
			name:     "nominal",
			policyID: "00000000-0000-0000-0000-000000000000",
		},
		{
			name:               "service error validation",
			policyID:           "00000000-0000-0000-0000-000000000000",
			serviceError:       service.ErrValidation,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrValidation,
		},
		{
			name:               "service error invalid id",
			policyID:           "invalid",
			serviceError:       service.ErrInvalidID,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrInvalidID,
		},
		{
			name:               "storage error not found",
			policyID:           "invalid",
			serviceError:       storage.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  sharedapi.ErrorCodeNotFound,
		},
		{
			name:               "service error other error",
			policyID:           "00000000-0000-0000-0000-000000000000",
			serviceError:       errors.New("some error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  sharedapi.ErrorInternal,
		},
	}

	for _, tc := range testCases {
		testCase := tc
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if testCase.expectedStatusCode == 0 {
				testCase.expectedStatusCode = http.StatusNoContent
			}

			backend, mockService := rootapi.NewTestingBackend(t)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				mockService.EXPECT().
					DeletePolicy(gomock.Any(), testCase.policyID).
					Return(nil)
			}
			if testCase.serviceError != nil {
				mockService.EXPECT().
					DeletePolicy(gomock.Any(), testCase.policyID).
					Return(testCase.serviceError)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			req := httptest.NewRequest(http.MethodDelete, "/policies/"+testCase.policyID, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)
			if testCase.expectedStatusCode >= 300 || testCase.expectedStatusCode < 200 {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestGetPolicy(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name               string
		policyID           string
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
	}

	testCases := []testCase{
		{
			name:     "nominal",
			policyID: "00000000-0000-0000-0000-000000000000",
		},
		{
			name:               "service error validation",
			policyID:           "00000000-0000-0000-0000-000000000000",
			serviceError:       service.ErrValidation,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrValidation,
		},
		{
			name:               "service error invalid id",
			policyID:           "00000000-0000-0000-0000-000000000000",
			serviceError:       service.ErrInvalidID,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrInvalidID,
		},
		{
			name:               "storage error not found",
			policyID:           "00000000-0000-0000-0000-000000000000",
			serviceError:       storage.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  sharedapi.ErrorCodeNotFound,
		},
		{
			name:               "service error other error",
			policyID:           "00000000-0000-0000-0000-000000000000",
			serviceError:       errors.New("some error"),
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

			var policyServiceResponse models.Policy
			if testCase.policyID != "" {
				policyServiceResponse = models.Policy{
					ID:             uuid.MustParse(testCase.policyID),
					CreatedAt:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					Name:           "test",
					LedgerName:     "test",
					LedgerQuery:    map[string]interface{}{},
					PaymentsPoolID: uuid.New(),
				}
			}

			expectedPolicyResponse := &policyResponse{
				ID:             policyServiceResponse.ID.String(),
				Name:           policyServiceResponse.Name,
				CreatedAt:      policyServiceResponse.CreatedAt,
				LedgerName:     policyServiceResponse.LedgerName,
				LedgerQuery:    policyServiceResponse.LedgerQuery,
				PaymentsPoolID: policyServiceResponse.PaymentsPoolID.String(),
			}

			backend, mockService := rootapi.NewTestingBackend(t)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				mockService.EXPECT().
					GetPolicy(gomock.Any(), testCase.policyID).
					Return(&policyServiceResponse, nil)
			}
			if testCase.serviceError != nil {
				mockService.EXPECT().
					GetPolicy(gomock.Any(), testCase.policyID).
					Return(nil, testCase.serviceError)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			req := httptest.NewRequest(http.MethodGet, "/policies/"+testCase.policyID, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				var resp sharedapi.BaseResponse[policyResponse]
				sharedapi.Decode(t, rec.Body, &resp)
				require.Equal(t, expectedPolicyResponse, resp.Data)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestUpdatePolicy(t *testing.T) {
	t.Parallel()

	transactionalMode := "transactional"
	balanceMode := "balance"
	newName := "updated-name"

	type testCase struct {
		name               string
		policyID           string
		req                *service.UpdatePolicyRequest
		invalidBody        bool
		expectedStatusCode int
		serviceError       error
		expectedErrorCode  string
	}

	testCases := []testCase{
		{
			name:     "nominal - update name",
			policyID: "00000000-0000-0000-0000-000000000000",
			req: &service.UpdatePolicyRequest{
				Name: &newName,
			},
		},
		{
			name:     "switch to transactional mode",
			policyID: "00000000-0000-0000-0000-000000000000",
			req: &service.UpdatePolicyRequest{
				Mode: &transactionalMode,
			},
		},
		{
			name:     "switch to balance mode",
			policyID: "00000000-0000-0000-0000-000000000000",
			req: &service.UpdatePolicyRequest{
				Mode: &balanceMode,
			},
		},
		{
			name:     "switch to balance mode with force",
			policyID: "00000000-0000-0000-0000-000000000000",
			req: &service.UpdatePolicyRequest{
				Mode:  &balanceMode,
				Force: true,
			},
		},
		{
			name:               "missing body",
			policyID:           "00000000-0000-0000-0000-000000000000",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrMissingOrInvalidBody,
		},
		{
			name:               "invalid body",
			policyID:           "00000000-0000-0000-0000-000000000000",
			invalidBody:        true,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrMissingOrInvalidBody,
		},
		{
			name:     "service error invalid id",
			policyID: "invalid",
			req: &service.UpdatePolicyRequest{
				Name: &newName,
			},
			serviceError:       service.ErrInvalidID,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  v1.ErrInvalidID,
		},
		{
			name:     "storage error not found",
			policyID: "00000000-0000-0000-0000-000000000000",
			req: &service.UpdatePolicyRequest{
				Name: &newName,
			},
			serviceError:       storage.ErrNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  sharedapi.ErrorCodeNotFound,
		},
		{
			name:     "pending review matches error",
			policyID: "00000000-0000-0000-0000-000000000000",
			req: &service.UpdatePolicyRequest{
				Mode: &balanceMode,
			},
			serviceError:       service.ErrPendingReviewMatches,
			expectedStatusCode: http.StatusConflict,
			expectedErrorCode:  v1.ErrPendingReviewMatches,
		},
		{
			name:     "service error other error",
			policyID: "00000000-0000-0000-0000-000000000000",
			req: &service.UpdatePolicyRequest{
				Name: &newName,
			},
			serviceError:       errors.New("some error"),
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

			var policyServiceResponse models.Policy
			parsedID, parseErr := uuid.Parse(testCase.policyID)
			if parseErr == nil {
				mode := "balance"
				if testCase.req != nil && testCase.req.Mode != nil {
					mode = *testCase.req.Mode
				}
				policyServiceResponse = models.Policy{
					ID:             parsedID,
					CreatedAt:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					Name:           "test",
					LedgerName:     "test",
					LedgerQuery:    map[string]interface{}{},
					PaymentsPoolID: uuid.New(),
					Mode:           mode,
				}
				if testCase.req != nil && testCase.req.Name != nil {
					policyServiceResponse.Name = *testCase.req.Name
				}
			}

			expectedPolicyResponse := &policyResponse{
				ID:             policyServiceResponse.ID.String(),
				Name:           policyServiceResponse.Name,
				CreatedAt:      policyServiceResponse.CreatedAt,
				LedgerName:     policyServiceResponse.LedgerName,
				LedgerQuery:    policyServiceResponse.LedgerQuery,
				PaymentsPoolID: policyServiceResponse.PaymentsPoolID.String(),
				Mode:           policyServiceResponse.Mode,
				Topology:       policyServiceResponse.Topology,
			}

			backend, mockService := rootapi.NewTestingBackend(t)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				mockService.EXPECT().
					UpdatePolicy(gomock.Any(), testCase.policyID, testCase.req).
					Return(&policyServiceResponse, nil)
			}
			if testCase.serviceError != nil {
				mockService.EXPECT().
					UpdatePolicy(gomock.Any(), testCase.policyID, testCase.req).
					Return(nil, testCase.serviceError)
			}

			router := rootapi.NewTestRouter(backend, testing.Verbose())

			var body []byte
			if testCase.invalidBody {
				body = []byte("invalid")
			} else if testCase.req != nil {
				var err error
				body, err = json.Marshal(testCase.req)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(http.MethodPatch, "/policies/"+testCase.policyID, bytes.NewReader(body))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectedStatusCode, rec.Code)
			if testCase.expectedStatusCode < 300 && testCase.expectedStatusCode >= 200 {
				var resp sharedapi.BaseResponse[policyResponse]
				sharedapi.Decode(t, rec.Body, &resp)
				require.Equal(t, expectedPolicyResponse, resp.Data)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
