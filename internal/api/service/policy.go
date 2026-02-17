package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type CreatePolicyRequest struct {
	Name            string                 `json:"name"`
	LedgerName      string                 `json:"ledgerName"`
	LedgerQuery     map[string]interface{} `json:"ledgerQuery"`
	PaymentsPoolID  string                 `json:"paymentsPoolID"`
	AssertionMode   models.AssertionMode   `json:"assertionMode"`
	AssertionConfig map[string]interface{} `json:"assertionConfig"`
}

func (r *CreatePolicyRequest) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("%w: missing name", ErrValidation)
	}

	if r.LedgerName == "" {
		return fmt.Errorf("%w: missing ledgerName", ErrValidation)
	}

	if r.PaymentsPoolID == "" {
		return fmt.Errorf("%w: missing paymentsPoolId", ErrValidation)
	}

	mode := models.NormalizeAssertionMode(r.AssertionMode)
	if !mode.IsValid() {
		return fmt.Errorf("%w: invalid assertionMode", ErrValidation)
	}

	if mode == models.AssertionModeMinBuffer {
		if _, err := parseMinBufferConfig(r.AssertionConfig); err != nil {
			return fmt.Errorf("%w: %s", ErrValidation, err.Error())
		}
	}

	r.AssertionMode = mode
	if r.AssertionConfig == nil {
		r.AssertionConfig = map[string]interface{}{}
	}

	return nil
}

func (s *Service) CreatePolicy(ctx context.Context, req *CreatePolicyRequest) (*models.Policy, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	paymentPoolID, err := uuid.Parse(req.PaymentsPoolID)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	policy := &models.Policy{
		ID:              uuid.New(),
		Name:            req.Name,
		CreatedAt:       time.Now().UTC(),
		LedgerName:      req.LedgerName,
		LedgerQuery:     req.LedgerQuery,
		PaymentsPoolID:  paymentPoolID,
		AssertionMode:   req.AssertionMode,
		AssertionConfig: req.AssertionConfig,
	}

	err = s.store.CreatePolicy(ctx, policy)
	if err != nil {
		return nil, newStorageError(err, "creating policy")
	}

	return policy, nil
}

type minBufferConfig struct {
	BufferType  string                          `json:"bufferType"`
	BufferValue int64                           `json:"bufferValue"`
	PerAsset    map[string]minBufferAssetConfig `json:"perAsset"`
}

type minBufferAssetConfig struct {
	BufferType  string `json:"bufferType"`
	BufferValue int64  `json:"bufferValue"`
}

func parseMinBufferConfig(raw map[string]interface{}) (*minBufferConfig, error) {
	if len(raw) == 0 {
		return nil, errors.New("missing assertionConfig for MIN_BUFFER")
	}

	payload, err := json.Marshal(raw)
	if err != nil {
		return nil, errors.Wrap(err, "invalid assertionConfig")
	}

	var cfg minBufferConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return nil, errors.Wrap(err, "invalid assertionConfig")
	}

	if err := validateBuffer(cfg.BufferType, cfg.BufferValue); err != nil {
		return nil, err
	}

	for asset, assetCfg := range cfg.PerAsset {
		if asset == "" {
			return nil, errors.New("asset key cannot be empty in assertionConfig.perAsset")
		}
		if err := validateBuffer(assetCfg.BufferType, assetCfg.BufferValue); err != nil {
			return nil, fmt.Errorf("invalid perAsset config for %s: %w", asset, err)
		}
	}

	return &cfg, nil
}

func validateBuffer(bufferType string, bufferValue int64) error {
	switch bufferType {
	case "ABSOLUTE", "BPS":
	default:
		return errors.New("bufferType must be ABSOLUTE or BPS")
	}

	if bufferValue < 0 {
		return errors.New("bufferValue must be >= 0")
	}

	return nil
}

func (s *Service) DeletePolicy(ctx context.Context, id string) error {
	pID, err := uuid.Parse(id)
	if err != nil {
		return errors.Wrap(ErrInvalidID, err.Error())
	}

	return newStorageError(s.store.DeletePolicy(ctx, pID), "deleting policy")
}

func (s *Service) GetPolicy(ctx context.Context, id string) (*models.Policy, error) {
	pID, err := uuid.Parse(id)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	policy, err := s.store.GetPolicy(ctx, pID)
	if err != nil {
		return nil, newStorageError(err, "getting policy")
	}

	return policy, nil
}

func (s *Service) ListPolicies(ctx context.Context, q storage.GetPoliciesQuery) (*bunpaginate.Cursor[models.Policy], error) {
	policies, err := s.store.ListPolicies(ctx, q)
	return policies, newStorageError(err, "listing policies")
}
