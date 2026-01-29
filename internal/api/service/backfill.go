package service

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/reconciliation/internal/events"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type CreateBackfillRequest struct {
	Source string  `json:"source"`
	Ledger *string `json:"ledger,omitempty"`
	Since  string  `json:"since"`
}

func (r *CreateBackfillRequest) Validate() error {
	if r.Source == "" {
		return errors.New("missing source")
	}
	src := models.BackfillSource(r.Source)
	if !src.IsValid() {
		return errors.New("source must be 'ledger' or 'payments'")
	}
	if r.Since == "" {
		return errors.New("missing since")
	}
	if _, err := time.Parse(time.RFC3339, r.Since); err != nil {
		return errors.New("since must be a valid RFC3339 timestamp")
	}
	return nil
}

func (s *Service) CreateBackfill(ctx context.Context, req *CreateBackfillRequest) (*models.Backfill, error) {
	if err := req.Validate(); err != nil {
		return nil, errors.Wrap(ErrValidation, err.Error())
	}

	since, _ := time.Parse(time.RFC3339, req.Since)
	now := time.Now().UTC()

	backfill := &models.Backfill{
		ID:        uuid.New(),
		Source:    models.BackfillSource(req.Source),
		Ledger:    req.Ledger,
		Since:     since,
		Status:    models.BackfillStatusPending,
		Ingested:  0,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.store.CreateBackfill(ctx, backfill); err != nil {
		return nil, newStorageError(err, "creating backfill")
	}

	// Publish BACKFILL event to NATS
	if s.publisher != nil && s.publishTopic != "" {
		event := events.NewBackfillEvent(backfill.ID.String())
		data, err := event.Marshal()
		if err == nil {
			msg := message.NewMessage(uuid.New().String(), data)
			msg.SetContext(ctx)
			_ = s.publisher.Publish(s.publishTopic, msg)
		}
	}

	return backfill, nil
}

func (s *Service) GetBackfill(ctx context.Context, id string) (*models.Backfill, error) {
	backfillID, err := uuid.Parse(id)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	backfill, err := s.store.GetBackfill(ctx, backfillID)
	if err != nil {
		return nil, newStorageError(err, "getting backfill")
	}

	return backfill, nil
}
