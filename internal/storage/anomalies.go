package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

//go:generate mockgen -source anomalies.go -destination anomalies_mock.go -package storage . AnomalyRepository

type AnomalyRepository interface {
	Create(ctx context.Context, anomaly *models.Anomaly) error
	GetByID(ctx context.Context, id uuid.UUID) (*models.Anomaly, error)
	ListByPolicy(ctx context.Context, q GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error)
	Resolve(ctx context.Context, id uuid.UUID, resolvedBy string) error
	FindOpenByTransactionIDs(ctx context.Context, transactionIDs []uuid.UUID) ([]models.Anomaly, error)
}

type AnomaliesFilters struct {
	PolicyID *uuid.UUID
	State    *models.AnomalyState
	Type     *models.AnomalyType
	Severity *models.Severity
}

type GetAnomaliesQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[AnomaliesFilters]]

func NewGetAnomaliesQuery(opts PaginatedQueryOptions[AnomaliesFilters]) GetAnomaliesQuery {
	return GetAnomaliesQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}

func (s *Storage) CreateAnomaly(ctx context.Context, anomaly *models.Anomaly) error {
	_, err := s.db.NewInsert().
		Model(anomaly).
		Exec(ctx)
	if err != nil {
		return e("failed to create anomaly", err)
	}

	return nil
}

func (s *Storage) GetAnomalyByID(ctx context.Context, id uuid.UUID) (*models.Anomaly, error) {
	var anomaly models.Anomaly
	err := s.db.NewSelect().
		Model(&anomaly).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get anomaly", err)
	}

	return &anomaly, nil
}

func (s *Storage) buildAnomalyListQuery(selectQuery *bun.SelectQuery, q GetAnomaliesQuery) *bun.SelectQuery {
	selectQuery = selectQuery.
		Order("created_at DESC")

	if q.Options.Options.PolicyID != nil {
		fmt.Printf("[DEBUG-STORAGE] Filtering anomalies by policy_id=%s\n", *q.Options.Options.PolicyID)
		selectQuery = selectQuery.Where("policy_id = ?", *q.Options.Options.PolicyID)
	} else {
		fmt.Printf("[DEBUG-STORAGE] WARNING: No policy_id filter set for anomaly query\n")
	}

	if q.Options.Options.State != nil {
		selectQuery = selectQuery.Where("state = ?", *q.Options.Options.State)
	}

	if q.Options.Options.Type != nil {
		selectQuery = selectQuery.Where("type = ?", *q.Options.Options.Type)
	}

	if q.Options.Options.Severity != nil {
		selectQuery = selectQuery.Where("severity = ?", *q.Options.Options.Severity)
	}

	return selectQuery
}

func (s *Storage) ListAnomaliesByPolicy(ctx context.Context, q GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error) {
	return paginateWithOffset[PaginatedQueryOptions[AnomaliesFilters], models.Anomaly](s, ctx,
		(*bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[AnomaliesFilters]])(&q),
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return s.buildAnomalyListQuery(query, q)
		},
	)
}

func (s *Storage) ResolveAnomaly(ctx context.Context, id uuid.UUID, resolvedBy string) error {
	now := time.Now().UTC()
	result, err := s.db.NewUpdate().
		Model((*models.Anomaly)(nil)).
		Set("state = ?", models.AnomalyStateResolved).
		Set("resolved_at = ?", now).
		Set("resolved_by = ?", resolvedBy).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return e("failed to resolve anomaly", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return e("failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return e("anomaly not found", ErrNotFound)
	}

	return nil
}

func (s *Storage) FindOpenAnomaliesByTransactionIDs(ctx context.Context, transactionIDs []uuid.UUID) ([]models.Anomaly, error) {
	if len(transactionIDs) == 0 {
		return []models.Anomaly{}, nil
	}

	var anomalies []models.Anomaly
	err := s.db.NewSelect().
		Model(&anomalies).
		Where("transaction_id IN (?)", bun.In(transactionIDs)).
		Where("state = ?", models.AnomalyStateOpen).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to find open anomalies by transaction IDs", err)
	}

	return anomalies, nil
}

// anomalyRepository wraps Storage to implement AnomalyRepository interface
type anomalyRepository struct {
	*Storage
}

// NewAnomalyRepository creates a new AnomalyRepository
func NewAnomalyRepository(s *Storage) AnomalyRepository {
	return &anomalyRepository{Storage: s}
}

func (r *anomalyRepository) Create(ctx context.Context, anomaly *models.Anomaly) error {
	return r.CreateAnomaly(ctx, anomaly)
}

func (r *anomalyRepository) GetByID(ctx context.Context, id uuid.UUID) (*models.Anomaly, error) {
	return r.GetAnomalyByID(ctx, id)
}

func (r *anomalyRepository) ListByPolicy(ctx context.Context, q GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error) {
	return r.ListAnomaliesByPolicy(ctx, q)
}

func (r *anomalyRepository) Resolve(ctx context.Context, id uuid.UUID, resolvedBy string) error {
	return r.ResolveAnomaly(ctx, id, resolvedBy)
}

func (r *anomalyRepository) FindOpenByTransactionIDs(ctx context.Context, transactionIDs []uuid.UUID) ([]models.Anomaly, error) {
	return r.FindOpenAnomaliesByTransactionIDs(ctx, transactionIDs)
}
