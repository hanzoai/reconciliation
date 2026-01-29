package api

import (
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/reconciliation/internal/api/backend"
	"go.uber.org/mock/gomock"
)

// NewTestingBackend creates a mock backend and service for testing.
// Exported for use in v1 and v2 test packages.
func NewTestingBackend(t *testing.T) (*backend.MockBackend, *backend.MockService) {
	ctrl := gomock.NewController(t)
	mockService := backend.NewMockService(ctrl)
	mockBackend := backend.NewMockBackend(ctrl)
	mockBackend.
		EXPECT().
		GetService().
		AnyTimes().
		DoAndReturn(func() backend.Service {
			return mockService
		})
	t.Cleanup(func() {
		ctrl.Finish()
	})
	return mockBackend, mockService
}

// NewTestRouter creates a router for testing with the given backend.
// Exported for use in v1 and v2 test packages.
func NewTestRouter(b backend.Backend, debug bool) *chi.Mux {
	return NewRouter(b, api.ServiceInfo{
		Debug: debug,
	}, auth.NewNoAuth(), nil)
}
