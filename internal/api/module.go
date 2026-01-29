package api

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/health"
	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/reconciliation/internal/api/backend"
	"github.com/formancehq/reconciliation/internal/api/service"
	"github.com/formancehq/reconciliation/internal/storage"
	"go.uber.org/fx"
)

const (
	ErrInvalidID            = "INVALID_ID"
	ErrMissingOrInvalidBody = "MISSING_OR_INVALID_BODY"
	ErrValidation           = "VALIDATION"
	ErrPendingReviewMatches = "PENDING_REVIEW_MATCHES"
)

func healthCheckModule() fx.Option {
	return fx.Options(
		health.Module(),
		health.ProvideHealthCheck(func() health.NamedCheck {
			return health.NewNamedCheck("default", health.CheckFn(func(ctx context.Context) error {
				return nil
			}))
		}),
	)
}

func HTTPModule(serviceInfo api.ServiceInfo, bind string) fx.Option {
	return fx.Options(
		healthCheckModule(),
		fx.Invoke(func(m *chi.Mux, lc fx.Lifecycle) {
			lc.Append(httpserver.NewHook(m, httpserver.WithAddress(bind)))
		}),
		fx.Provide(func(store *storage.Storage) service.Store {
			return store
		}),
		fx.Supply(serviceInfo),
		fx.Provide(fx.Annotate(service.NewSDKFormance, fx.As(new(service.SDKFormance)))),
		fx.Provide(fx.Annotate(service.NewService, fx.As(new(backend.Service)))),
		fx.Provide(backend.NewDefaultBackend),
		fx.Provide(NewRouter),
	)
}

// HTTPModuleWithPublisher extends HTTPModule with publisher support for backfill events.
func HTTPModuleWithPublisher(serviceInfo api.ServiceInfo, bind string, publishTopic string) fx.Option {
	return fx.Options(
		HTTPModule(serviceInfo, bind),
		fx.Invoke(func(svc backend.Service, params struct {
			fx.In
			Publisher message.Publisher `optional:"true"`
		}) {
			if params.Publisher != nil {
				if s, ok := svc.(*service.Service); ok {
					s.SetPublisher(params.Publisher, publishTopic)
				}
			}
		}),
	)
}
