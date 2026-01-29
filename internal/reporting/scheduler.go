package reporting

import (
	"context"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
)

const (
	// DefaultReportSchedule is the default cron schedule for report generation (6:00 UTC daily).
	DefaultReportSchedule = "0 6 * * *"
)

// SchedulerConfig holds the configuration for the report scheduler.
type SchedulerConfig struct {
	// Schedule is the cron expression for when to run report generation.
	// Defaults to "0 6 * * *" (6:00 UTC daily).
	Schedule string
}

// Scheduler handles the automatic generation of daily reports for all policies.
type Scheduler struct {
	db       *bun.DB
	store    *storage.Storage
	config   SchedulerConfig
	cron     *cron.Cron
	mu       sync.Mutex
	running  bool
	stopOnce sync.Once
}

// NewScheduler creates a new report scheduler.
func NewScheduler(db *bun.DB, store *storage.Storage, config SchedulerConfig) *Scheduler {
	if config.Schedule == "" {
		config.Schedule = DefaultReportSchedule
	}

	return &Scheduler{
		db:     db,
		store:  store,
		config: config,
	}
}

// Start begins the scheduled report generation.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	logger := logging.FromContext(ctx)

	// Create a new cron scheduler with seconds precision disabled (standard cron format)
	s.cron = cron.New(cron.WithLocation(time.UTC))

	// Add the report generation job
	_, err := s.cron.AddFunc(s.config.Schedule, func() {
		s.generateDailyReports(ctx)
	})
	if err != nil {
		return err
	}

	s.cron.Start()
	s.running = true

	logger.WithFields(map[string]interface{}{
		"schedule": s.config.Schedule,
	}).Info("Report scheduler started")

	return nil
}

// Stop halts the scheduled report generation.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.cron != nil {
			s.cron.Stop()
		}
		s.running = false
	})
}

// generateDailyReports generates reports for all policies for the previous day (D-1).
func (s *Scheduler) generateDailyReports(ctx context.Context) {
	logger := logging.FromContext(ctx)

	// Calculate the D-1 period (previous day 00:00 to 23:59:59.999999999)
	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, -1)
	periodStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, time.UTC)

	logger.WithFields(map[string]interface{}{
		"period_start": periodStart,
		"period_end":   periodEnd,
	}).Info("Starting daily report generation")

	// Get all transactional policies (only transactional mode policies have matching data)
	policies, err := s.listTransactionalPolicies(ctx)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("Failed to list policies for report generation")
		return
	}

	if len(policies) == 0 {
		logger.Info("No transactional policies found, skipping report generation")
		return
	}

	generator := NewGenerator(s.db)
	successCount := 0
	errorCount := 0

	for _, policy := range policies {
		if err := s.generateAndPersistReport(ctx, generator, policy.ID, periodStart, periodEnd); err != nil {
			logger.WithFields(map[string]interface{}{
				"policy_id": policy.ID,
				"error":     err.Error(),
			}).Error("Failed to generate report for policy")
			errorCount++
			// Continue with other policies even if one fails
			continue
		}
		successCount++
	}

	logger.WithFields(map[string]interface{}{
		"total_policies": len(policies),
		"success_count":  successCount,
		"error_count":    errorCount,
		"period_start":   periodStart,
		"period_end":     periodEnd,
	}).Info("Daily report generation completed")
}

// generateAndPersistReport generates a report for a policy and persists it to the database.
func (s *Scheduler) generateAndPersistReport(ctx context.Context, generator *Generator, policyID uuid.UUID, from, to time.Time) error {
	logger := logging.FromContext(ctx)

	// Generate the report
	report, err := generator.Generate(ctx, policyID, from, to)
	if err != nil {
		return err
	}

	// Convert to model and persist
	modelReport := &models.Report{
		ID:                uuid.New(),
		PolicyID:          report.PolicyID,
		PeriodStart:       report.PeriodStart,
		PeriodEnd:         report.PeriodEnd,
		TotalTransactions: report.TotalTransactions,
		MatchedCount:      report.MatchedCount,
		MatchRate:         report.MatchRate,
		AnomaliesByType:   report.AnomaliesByType,
		GeneratedAt:       report.GeneratedAt,
	}

	if err := s.store.CreateReport(ctx, modelReport); err != nil {
		return err
	}

	logger.WithFields(map[string]interface{}{
		"policy_id":          policyID,
		"report_id":          modelReport.ID,
		"total_transactions": report.TotalTransactions,
		"matched_count":      report.MatchedCount,
		"match_rate":         report.MatchRate,
	}).Debug("Report generated and persisted")

	return nil
}

// listTransactionalPolicies retrieves all policies in transactional mode from the database.
// Only transactional mode policies have meaningful transaction/matching data for reports.
func (s *Scheduler) listTransactionalPolicies(ctx context.Context) ([]models.Policy, error) {
	var policies []models.Policy
	err := s.db.NewSelect().
		Model(&policies).
		Where("policy_mode = ?", "transactional").
		Order("created_at ASC").
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	return policies, nil
}

// ParseSchedule validates a cron schedule expression.
// Returns an error if the schedule is invalid.
func ParseSchedule(schedule string) error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	_, err := parser.Parse(schedule)
	return err
}

// CalculateDMinusOnePeriod returns the start and end times for the previous day (D-1).
// Start: D-1 00:00:00 UTC
// End: D-1 23:59:59.999999999 UTC
func CalculateDMinusOnePeriod(referenceTime time.Time) (start, end time.Time) {
	refUTC := referenceTime.UTC()
	yesterday := refUTC.AddDate(0, 0, -1)
	start = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	end = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, time.UTC)
	return start, end
}
