package storage

import (
	"context"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
)

func (s *Storage) CreateReport(ctx context.Context, report *models.Report) error {
	_, err := s.db.NewInsert().
		Model(report).
		Exec(ctx)
	if err != nil {
		return e("failed to create report", err)
	}

	return nil
}

func (s *Storage) GetLatestReportByPolicyID(ctx context.Context, policyID uuid.UUID) (*models.Report, error) {
	var report models.Report
	err := s.db.NewSelect().
		Model(&report).
		Where("policy_id = ?", policyID).
		Order("generated_at DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get latest report", err)
	}

	return &report, nil
}
