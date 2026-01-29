package reporting

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

// SchedulerModule creates an fx module for the report scheduler.
func SchedulerModule(config SchedulerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(db *bun.DB, store *storage.Storage) *Scheduler {
			return NewScheduler(db, store, config)
		}),
		fx.Invoke(func(lc fx.Lifecycle, scheduler *Scheduler) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logging.FromContext(ctx).Info("Starting report scheduler")
					return scheduler.Start(ctx)
				},
				OnStop: func(ctx context.Context) error {
					logging.FromContext(ctx).Info("Stopping report scheduler")
					scheduler.Stop()
					return nil
				},
			})
		}),
	)
}
