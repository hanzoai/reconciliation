package storage

import (
	"context"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
)

func (s *Storage) CreateBackfill(ctx context.Context, backfill *models.Backfill) error {
	_, err := s.db.NewInsert().
		Model(backfill).
		Exec(ctx)
	if err != nil {
		return e("failed to create backfill", err)
	}
	return nil
}

func (s *Storage) GetBackfill(ctx context.Context, id uuid.UUID) (*models.Backfill, error) {
	var backfill models.Backfill
	err := s.db.NewSelect().
		Model(&backfill).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get backfill", err)
	}
	return &backfill, nil
}

func (s *Storage) UpdateBackfillStatus(ctx context.Context, id uuid.UUID, status models.BackfillStatus, errorMessage *string) error {
	q := s.db.NewUpdate().
		Model((*models.Backfill)(nil)).
		Set("status = ?", status).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id)

	if errorMessage != nil {
		q = q.Set("error_message = ?", *errorMessage)
	}

	result, err := q.Exec(ctx)
	if err != nil {
		return e("failed to update backfill status", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return e("failed to get rows affected", err)
	}
	if rowsAffected == 0 {
		return e("backfill not found", ErrNotFound)
	}
	return nil
}

func (s *Storage) UpdateBackfillProgress(ctx context.Context, id uuid.UUID, ingested int64, lastCursor *string) error {
	q := s.db.NewUpdate().
		Model((*models.Backfill)(nil)).
		Set("ingested = ?", ingested).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id)

	if lastCursor != nil {
		q = q.Set("last_cursor = ?", *lastCursor)
	}

	result, err := q.Exec(ctx)
	if err != nil {
		return e("failed to update backfill progress", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return e("failed to get rows affected", err)
	}
	if rowsAffected == 0 {
		return e("backfill not found", ErrNotFound)
	}
	return nil
}
