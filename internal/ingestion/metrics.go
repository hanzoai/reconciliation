package ingestion

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// MeterName is the name of the meter used for ingestion metrics.
	MeterName = "github.com/formancehq/reconciliation/internal/ingestion"

	// StatusSuccess indicates successful event processing.
	StatusSuccess = "success"
	// StatusError indicates failed event processing.
	StatusError = "error"
	// StatusSkipped indicates the event was skipped (e.g., duplicate, irrelevant).
	StatusSkipped = "skipped"

	// SideLedger is the label value for ledger-side events.
	SideLedger = "ledger"
	// SidePayment is the label value for payment-side events.
	SidePayment = "payment"
)

// IngestionMetrics holds the OpenTelemetry metrics instruments for ingestion.
type IngestionMetrics struct {
	// eventsTotal counts the total number of ingestion events processed.
	// Labels: side (ledger/payment), status (success/error/skipped)
	eventsTotal metric.Int64Counter

	// durationSeconds measures the duration of event processing.
	durationSeconds metric.Float64Histogram

	// lagSeconds tracks the lag between event timestamp and ingestion time.
	lagSeconds metric.Float64Gauge
}

// NewIngestionMetrics creates a new IngestionMetrics instance with all metrics initialized.
func NewIngestionMetrics() (*IngestionMetrics, error) {
	meter := otel.Meter(MeterName)

	eventsTotal, err := meter.Int64Counter(
		"reconciliation_ingestion_events_total",
		metric.WithDescription("Total number of ingestion events processed"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	durationSeconds, err := meter.Float64Histogram(
		"reconciliation_ingestion_duration_seconds",
		metric.WithDescription("Duration of event processing in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	lagSeconds, err := meter.Float64Gauge(
		"reconciliation_ingestion_lag_seconds",
		metric.WithDescription("Lag between event timestamp and ingestion time in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return &IngestionMetrics{
		eventsTotal:     eventsTotal,
		durationSeconds: durationSeconds,
		lagSeconds:      lagSeconds,
	}, nil
}

// RecordEvent records an ingestion event with the given side and status.
func (m *IngestionMetrics) RecordEvent(ctx context.Context, side, status string) {
	m.eventsTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("side", side),
			attribute.String("status", status),
		),
	)
}

// RecordDuration records the duration of event processing for the given side.
func (m *IngestionMetrics) RecordDuration(ctx context.Context, side string, duration time.Duration) {
	m.durationSeconds.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("side", side),
		),
	)
}

// RecordLag records the lag between the event timestamp and now for the given side.
func (m *IngestionMetrics) RecordLag(ctx context.Context, side string, eventTimestamp time.Time) {
	lag := time.Since(eventTimestamp).Seconds()
	// Ensure lag is non-negative (in case of clock skew)
	if lag < 0 {
		lag = 0
	}
	m.lagSeconds.Record(ctx, lag,
		metric.WithAttributes(
			attribute.String("side", side),
		),
	)
}

