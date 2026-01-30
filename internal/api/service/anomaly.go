package service

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Candidate represents a potential match candidate for an anomaly
type Candidate struct {
	Transaction models.Transaction
	Score       float64
	Reasons     []string
}

// AnomalyDetailResult contains the full details of an anomaly including source transaction and candidates
type AnomalyDetailResult struct {
	Anomaly           models.Anomaly
	SourceTransaction models.Transaction
	Candidates        []Candidate
}

func (s *Service) ListPolicyAnomalies(ctx context.Context, policyID string, q storage.GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error) {
	pID, err := uuid.Parse(policyID)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	// Verify the policy exists
	_, err = s.store.GetPolicy(ctx, pID)
	if err != nil {
		return nil, newStorageError(err, "getting policy")
	}

	// Set the policy ID filter
	fmt.Printf("[DEBUG-SERVICE] ListPolicyAnomalies: setting policy_id filter to %s\n", pID)
	q.Options.Options.PolicyID = &pID

	anomalies, err := s.store.ListAnomaliesByPolicy(ctx, q)
	if err != nil {
		return nil, newStorageError(err, "listing anomalies")
	}

	fmt.Printf("[DEBUG-SERVICE] ListPolicyAnomalies: found %d anomalies for policy %s\n", len(anomalies.Data), pID)
	return anomalies, nil
}

func (s *Service) ListAnomalies(ctx context.Context, q storage.GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error) {
	anomalies, err := s.store.ListAnomaliesByPolicy(ctx, q)
	if err != nil {
		return nil, newStorageError(err, "listing anomalies")
	}

	return anomalies, nil
}

func (s *Service) GetAnomalyByID(ctx context.Context, anomalyID string) (*AnomalyDetailResult, error) {
	id, err := uuid.Parse(anomalyID)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	// Get the anomaly
	anomaly, err := s.store.GetAnomalyByID(ctx, id)
	if err != nil {
		return nil, newStorageError(err, "getting anomaly")
	}

	if anomaly.TransactionID == nil {
		return nil, errors.Wrap(ErrValidation, "anomaly missing transactionID")
	}

	// Get the source transaction from OpenSearch
	sourceTransaction, err := s.txStore.GetByID(ctx, *anomaly.TransactionID)
	if err != nil {
		return nil, newStorageError(err, "getting source transaction")
	}

	// For now, candidates are empty as they are computed at matching time
	// and not persisted. Future enhancement could store candidates with anomalies.
	candidates := []Candidate{}

	return &AnomalyDetailResult{
		Anomaly:           *anomaly,
		SourceTransaction: *sourceTransaction,
		Candidates:        candidates,
	}, nil
}
