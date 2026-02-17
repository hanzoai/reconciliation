package service

import (
	"bytes"
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
	AssertionMode   models.AssertionMode   `json:"mode"`
	AssertionConfig map[string]interface{} `json:"assertionConfig"`
}

type UpdatePolicyRequest = CreatePolicyRequest

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
		return fmt.Errorf("%w: invalid mode", ErrValidation)
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
		PolicyID:        uuid.New(),
		Version:         1,
		Name:            req.Name,
		CreatedAt:       time.Now().UTC(),
		Lifecycle:       models.PolicyLifecycleEnabled,
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

func (s *Service) UpdatePolicy(ctx context.Context, id string, req *UpdatePolicyRequest) (*models.Policy, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	policyID, err := uuid.Parse(id)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	latest, err := s.store.GetPolicy(ctx, policyID)
	if err != nil {
		return nil, newStorageError(err, "getting policy")
	}

	paymentPoolID, err := uuid.Parse(req.PaymentsPoolID)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	policy := &models.Policy{
		ID:              uuid.New(),
		PolicyID:        latest.PolicyID,
		Name:            req.Name,
		CreatedAt:       time.Now().UTC(),
		Lifecycle:       latest.Lifecycle,
		LedgerName:      req.LedgerName,
		LedgerQuery:     req.LedgerQuery,
		PaymentsPoolID:  paymentPoolID,
		AssertionMode:   req.AssertionMode,
		AssertionConfig: req.AssertionConfig,
	}

	if err := s.store.CreatePolicyVersion(ctx, policy); err != nil {
		return nil, newStorageError(err, "creating policy version")
	}

	return policy, nil
}

type minBufferConfig struct {
	Assets map[string]minBufferAssetRule `json:"assets"`
}

type minBufferAssetRule struct {
	BPS      *int64 `json:"bps,omitempty"`
	Absolute *int64 `json:"absolute,omitempty"`
}

func parseMinBufferConfig(raw map[string]interface{}) (*minBufferConfig, error) {
	if len(raw) == 0 {
		return nil, errors.New("missing assertionConfig for MIN_BUFFER")
	}

	payload, err := json.Marshal(raw)
	if err != nil {
		return nil, errors.Wrap(err, "invalid assertionConfig")
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()

	var cfg minBufferConfig
	if err := dec.Decode(&cfg); err != nil {
		return nil, errors.Wrap(err, "invalid assertionConfig")
	}

	if len(cfg.Assets) == 0 {
		return nil, errors.New("assertionConfig.assets must not be empty")
	}

	for asset, assetCfg := range cfg.Assets {
		if asset == "" {
			return nil, errors.New("asset key cannot be empty in assertionConfig.assets")
		}
		if err := validateMinBufferRule(asset, assetCfg); err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}

func validateMinBufferRule(asset string, rule minBufferAssetRule) error {
	hasBPS := rule.BPS != nil
	hasAbsolute := rule.Absolute != nil
	if hasBPS == hasAbsolute {
		return fmt.Errorf("invalid rule for %s: exactly one of bps or absolute must be set", asset)
	}

	if asset == "*" && hasAbsolute {
		return errors.New("invalid rule for *: only bps is allowed")
	}

	if hasBPS && *rule.BPS < 0 {
		return fmt.Errorf("invalid rule for %s: bps must be >= 0", asset)
	}

	if hasAbsolute && *rule.Absolute < 0 {
		return fmt.Errorf("invalid rule for %s: absolute must be >= 0", asset)
	}

	return nil
}

func (s *Service) ArchivePolicy(ctx context.Context, id string) error {
	pID, err := uuid.Parse(id)
	if err != nil {
		return errors.Wrap(ErrInvalidID, err.Error())
	}

	return newStorageError(s.store.ArchivePolicy(ctx, pID), "archiving policy")
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
