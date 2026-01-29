package service

import (
	"context"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func (s *Service) GetLatestPolicyReport(ctx context.Context, policyID string) (*models.Report, error) {
	pID, err := uuid.Parse(policyID)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	// Verify the policy exists
	_, err = s.store.GetPolicy(ctx, pID)
	if err != nil {
		return nil, newStorageError(err, "getting policy")
	}

	report, err := s.store.GetLatestReportByPolicyID(ctx, pID)
	if err != nil {
		return nil, newStorageError(err, "getting latest report")
	}

	return report, nil
}
