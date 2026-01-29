package service

import (
	"context"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type CreatePolicyRequest struct {
	Name           string                 `json:"name"`
	LedgerName     string                 `json:"ledgerName"`
	LedgerQuery    map[string]interface{} `json:"ledgerQuery"`
	PaymentsPoolID string                 `json:"paymentsPoolID"`

	// Transactional reconciliation fields (optional)
	// If Mode is not specified or empty, defaults to "balance"
	Mode                *string               `json:"mode,omitempty"`                // "balance" (default) or "transactional"
	Topology            *string               `json:"topology,omitempty"`            // "1:1" (default), "1:N", or "N:1"
	DeterministicFields []string              `json:"deterministicFields,omitempty"` // defaults to ["external_id"]
	ScoringConfig       *models.ScoringConfig `json:"scoringConfig,omitempty"`       // scoring configuration for transactional mode
	PaymentsProvider    *string               `json:"paymentsProvider,omitempty"`    // provider name for dynamic payments lookup (e.g., "stripe", "wise")
}

// validModes contains the allowed values for policy mode
var validModes = map[string]bool{
	"balance":       true,
	"transactional": true,
}

// validTopologies contains the allowed values for policy topology
var validTopologies = map[string]bool{
	"1:1": true,
	"1:N": true,
	"N:1": true,
}

func (r *CreatePolicyRequest) Validate() error {
	if r.Name == "" {
		return errors.New("missing name")
	}

	if r.LedgerName == "" {
		return errors.New("missing ledgerName")
	}

	if r.PaymentsPoolID == "" {
		return errors.New("missing paymentsPoolId")
	}

	// Validate mode if provided
	if r.Mode != nil && *r.Mode != "" {
		if !validModes[*r.Mode] {
			return errors.New("invalid mode: must be 'balance' or 'transactional'")
		}
	}

	// Validate topology if provided
	if r.Topology != nil && *r.Topology != "" {
		if !validTopologies[*r.Topology] {
			return errors.New("invalid topology: must be '1:1', '1:N', or 'N:1'")
		}
	}

	// Validate scoring config if provided
	if r.ScoringConfig != nil {
		if err := validateScoringConfig(r.ScoringConfig); err != nil {
			return errors.Wrap(err, "invalid scoringConfig")
		}
	}

	return nil
}

func validateScoringConfig(sc *models.ScoringConfig) error {
	if sc.TimeWindowHours < 0 {
		return errors.New("timeWindowHours must be non-negative")
	}
	if sc.AmountTolerancePercent < 0 || sc.AmountTolerancePercent > 100 {
		return errors.New("amountTolerancePercent must be between 0 and 100")
	}
	if sc.Weights != nil {
		if sc.Weights.Amount < 0 || sc.Weights.Date < 0 || sc.Weights.Metadata < 0 {
			return errors.New("weights must be non-negative")
		}
	}
	if sc.Thresholds != nil {
		if sc.Thresholds.AutoMatch < 0 || sc.Thresholds.AutoMatch > 1 {
			return errors.New("autoMatch threshold must be between 0 and 1")
		}
		if sc.Thresholds.Review < 0 || sc.Thresholds.Review > 1 {
			return errors.New("review threshold must be between 0 and 1")
		}
		if sc.Thresholds.Review > sc.Thresholds.AutoMatch {
			return errors.New("review threshold must be less than or equal to autoMatch threshold")
		}
	}
	return nil
}

func (s *Service) CreatePolicy(ctx context.Context, req *CreatePolicyRequest) (*models.Policy, error) {
	paymentPoolID, err := uuid.Parse(req.PaymentsPoolID)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	// Default mode is "balance" (legacy behavior)
	mode := "balance"
	if req.Mode != nil && *req.Mode != "" {
		mode = *req.Mode
	}

	// Default topology is "1:1"
	topology := "1:1"
	if req.Topology != nil && *req.Topology != "" {
		topology = *req.Topology
	}

	// Get payments provider if provided
	paymentsProvider := ""
	if req.PaymentsProvider != nil {
		paymentsProvider = *req.PaymentsProvider
	}

	policy := &models.Policy{
		ID:                  uuid.New(),
		Name:                req.Name,
		CreatedAt:           time.Now().UTC(),
		LedgerName:          req.LedgerName,
		LedgerQuery:         req.LedgerQuery,
		PaymentsPoolID:      paymentPoolID,
		PaymentsProvider:    paymentsProvider,
		Mode:                mode,
		Topology:            topology,
		DeterministicFields: req.DeterministicFields,
		ScoringConfig:       req.ScoringConfig,
	}

	// If transactional mode and no deterministic fields specified, use default
	if mode == "transactional" && len(policy.DeterministicFields) == 0 {
		policy.DeterministicFields = []string{"external_id"}
	}

	// If transactional mode and no scoring config, use defaults
	if mode == "transactional" && policy.ScoringConfig == nil {
		policy.ScoringConfig = defaultScoringConfig()
	}

	err = s.store.CreatePolicy(ctx, policy)
	if err != nil {
		return nil, newStorageError(err, "creating policy")
	}

	return policy, nil
}

// defaultScoringConfig returns the default scoring configuration for transactional mode
func defaultScoringConfig() *models.ScoringConfig {
	return &models.ScoringConfig{
		TimeWindowHours:         24,
		AmountTolerancePercent:  0.0,
		MetadataFields:          []string{"order_id", "user_id"},
		MetadataCaseInsensitive: false,
		Weights: &models.ScoringWeights{
			Amount:   0.4,
			Date:     0.3,
			Metadata: 0.3,
		},
		Thresholds: &models.ScoringThresholds{
			AutoMatch: 0.85,
			Review:    0.60,
		},
	}
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

// UpdatePolicyRequest represents a request to update a policy.
// All fields are optional - only provided fields will be updated.
type UpdatePolicyRequest struct {
	Name                *string               `json:"name,omitempty"`
	Mode                *string               `json:"mode,omitempty"`                // "balance" or "transactional"
	Topology            *string               `json:"topology,omitempty"`            // "1:1", "1:N", or "N:1"
	DeterministicFields []string              `json:"deterministicFields,omitempty"` // fields for deterministic matching
	ScoringConfig       *models.ScoringConfig `json:"scoringConfig,omitempty"`       // scoring configuration
	PaymentsProvider    *string               `json:"paymentsProvider,omitempty"`    // provider name for dynamic payments lookup
	Force               bool                  `json:"force,omitempty"`               // force mode switch even with pending reviews
}

func (r *UpdatePolicyRequest) Validate() error {
	// Validate mode if provided
	if r.Mode != nil && *r.Mode != "" {
		if !validModes[*r.Mode] {
			return errors.New("invalid mode: must be 'balance' or 'transactional'")
		}
	}

	// Validate topology if provided
	if r.Topology != nil && *r.Topology != "" {
		if !validTopologies[*r.Topology] {
			return errors.New("invalid topology: must be '1:1', '1:N', or 'N:1'")
		}
	}

	// Validate scoring config if provided
	if r.ScoringConfig != nil {
		if err := validateScoringConfig(r.ScoringConfig); err != nil {
			return errors.Wrap(err, "invalid scoringConfig")
		}
	}

	return nil
}

// ErrPendingReviewMatches is returned when trying to switch from transactional to balance
// mode while there are matches pending review.
var ErrPendingReviewMatches = errors.New("cannot switch to balance mode: there are matches pending review")

// ModeSwitchWarning contains information about potential issues when switching modes.
type ModeSwitchWarning struct {
	PendingReviewCount int64  `json:"pendingReviewCount,omitempty"`
	Message            string `json:"message,omitempty"`
}

// UpdatePolicyResult contains the updated policy and any warnings.
type UpdatePolicyResult struct {
	Policy   *models.Policy     `json:"policy"`
	Warnings *ModeSwitchWarning `json:"warnings,omitempty"`
}

func (s *Service) UpdatePolicy(ctx context.Context, id string, req *UpdatePolicyRequest) (*models.Policy, error) {
	pID, err := uuid.Parse(id)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	// Get existing policy
	policy, err := s.store.GetPolicy(ctx, pID)
	if err != nil {
		return nil, newStorageError(err, "getting policy")
	}

	// Track mode switches
	switchingToTransactional := req.Mode != nil && *req.Mode == "transactional" && policy.Mode != "transactional"
	switchingToBalance := req.Mode != nil && *req.Mode == "balance" && policy.Mode == "transactional"

	// Validation when switching from transactional to balance mode
	if switchingToBalance && !req.Force {
		// Check if there are pending review matches
		pendingCount, err := s.store.CountMatchesByDecision(ctx, pID, models.DecisionRequiresReview)
		if err != nil {
			return nil, newStorageError(err, "counting pending matches")
		}

		if pendingCount > 0 {
			return nil, errors.Wrapf(ErrPendingReviewMatches,
				"there are %d matches pending review - use 'force: true' to proceed anyway", pendingCount)
		}
	}

	// Apply updates
	if req.Name != nil {
		policy.Name = *req.Name
	}
	if req.Mode != nil {
		policy.Mode = *req.Mode
	}
	if req.Topology != nil {
		policy.Topology = *req.Topology
	}
	if req.DeterministicFields != nil {
		policy.DeterministicFields = req.DeterministicFields
	}
	if req.ScoringConfig != nil {
		policy.ScoringConfig = req.ScoringConfig
	}
	if req.PaymentsProvider != nil {
		policy.PaymentsProvider = *req.PaymentsProvider
	}

	// If switching to transactional mode and no deterministic fields, set defaults
	if switchingToTransactional && len(policy.DeterministicFields) == 0 {
		policy.DeterministicFields = []string{"external_id"}
	}

	// If switching to transactional mode and no scoring config, set defaults
	if switchingToTransactional && policy.ScoringConfig == nil {
		policy.ScoringConfig = defaultScoringConfig()
	}

	// If switching to balance mode, clear transactional fields
	if switchingToBalance {
		policy.DeterministicFields = nil
		policy.Topology = "1:1"
		policy.ScoringConfig = nil
	}

	// If switching to transactional mode and no topology, set default
	if switchingToTransactional && policy.Topology == "" {
		policy.Topology = "1:1"
	}

	// Update in storage
	err = s.store.UpdatePolicy(ctx, policy)
	if err != nil {
		return nil, newStorageError(err, "updating policy")
	}

	return policy, nil
}

// CreatePolicyV2BalanceRequest represents a v2 API request to create a balance policy.
type CreatePolicyV2BalanceRequest struct {
	Name       string                 `json:"name"`
	LedgerName string                 `json:"ledgerName"`
	LedgerQuery map[string]interface{} `json:"ledgerQuery"`
	PoolID     string                 `json:"poolId"`
}

// CreatePolicyV2Balance creates a new balance-mode policy using v2 API format.
func (s *Service) CreatePolicyV2Balance(ctx context.Context, req *CreatePolicyV2BalanceRequest) (*models.Policy, error) {
	paymentPoolID, err := uuid.Parse(req.PoolID)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	// Default ledger query if not provided
	ledgerQuery := req.LedgerQuery
	if ledgerQuery == nil {
		ledgerQuery = make(map[string]interface{})
	}

	policy := &models.Policy{
		ID:             uuid.New(),
		Name:           req.Name,
		CreatedAt:      time.Now().UTC(),
		LedgerName:     req.LedgerName,
		LedgerQuery:    ledgerQuery,
		PaymentsPoolID: paymentPoolID,
		Mode:           "balance",
		Topology:       "1:1",
	}

	err = s.store.CreatePolicy(ctx, policy)
	if err != nil {
		return nil, newStorageError(err, "creating policy")
	}

	return policy, nil
}

// CreatePolicyV2TransactionRequest represents a v2 API request to create a transaction policy.
type CreatePolicyV2TransactionRequest struct {
	Name                string                 `json:"name"`
	LedgerName          string                 `json:"ledgerName"`
	LedgerQuery         map[string]interface{} `json:"ledgerQuery"`
	ConnectorType       *string                `json:"connectorType,omitempty"`
	ConnectorID         *string                `json:"connectorId,omitempty"`
	DeterministicFields []string               `json:"deterministicFields,omitempty"`
	ScoringConfig       *models.ScoringConfig  `json:"scoringConfig,omitempty"`
}

// CreatePolicyV2Transaction creates a new transactional-mode policy using v2 API format.
func (s *Service) CreatePolicyV2Transaction(ctx context.Context, req *CreatePolicyV2TransactionRequest) (*models.Policy, error) {
	// connectorType and connectorId are mutually exclusive
	if req.ConnectorType != nil && *req.ConnectorType != "" &&
		req.ConnectorID != nil && *req.ConnectorID != "" {
		return nil, errors.New("connectorType and connectorId are mutually exclusive")
	}

	// Default ledger query if not provided
	ledgerQuery := req.LedgerQuery
	if ledgerQuery == nil {
		ledgerQuery = make(map[string]interface{})
	}

	// Apply defaults for transactional mode
	deterministicFields := req.DeterministicFields
	if len(deterministicFields) == 0 {
		deterministicFields = []string{"external_id"}
	}

	scoringConfig := req.ScoringConfig
	if scoringConfig == nil {
		scoringConfig = defaultScoringConfig()
	}

	policy := &models.Policy{
		ID:                  uuid.New(),
		Name:                req.Name,
		CreatedAt:           time.Now().UTC(),
		LedgerName:          req.LedgerName,
		LedgerQuery:         ledgerQuery,
		PaymentsPoolID:      uuid.Nil, // Not used in transaction mode
		Mode:                "transactional",
		Topology:            "1:1",
		DeterministicFields: deterministicFields,
		ScoringConfig:       scoringConfig,
		ConnectorType:       req.ConnectorType,
		ConnectorID:         req.ConnectorID,
	}

	err := s.store.CreatePolicy(ctx, policy)
	if err != nil {
		return nil, newStorageError(err, "creating policy")
	}

	return policy, nil
}

// UpdatePolicyV2Request represents a v2 API request to update a policy.
type UpdatePolicyV2Request struct {
	Name                *string                `json:"name,omitempty"`
	LedgerName          *string                `json:"ledgerName,omitempty"`
	LedgerQuery         map[string]interface{} `json:"ledgerQuery,omitempty"`
	ConnectorType       *string                `json:"connectorType,omitempty"`
	ConnectorID         *string                `json:"connectorId,omitempty"`
	DeterministicFields []string               `json:"deterministicFields,omitempty"`
	ScoringConfig       *models.ScoringConfig  `json:"scoringConfig,omitempty"`
}

// UpdatePolicyV2 updates a policy using v2 API format.
func (s *Service) UpdatePolicyV2(ctx context.Context, id string, req *UpdatePolicyV2Request) (*models.Policy, error) {
	pID, err := uuid.Parse(id)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	// Get existing policy
	policy, err := s.store.GetPolicy(ctx, pID)
	if err != nil {
		return nil, newStorageError(err, "getting policy")
	}

	// Apply updates
	if req.Name != nil {
		policy.Name = *req.Name
	}

	if req.LedgerName != nil {
		policy.LedgerName = *req.LedgerName
	}

	if req.LedgerQuery != nil {
		policy.LedgerQuery = req.LedgerQuery
	}

	// For transaction mode, handle connector and matcher updates
	if policy.Mode == "transactional" {
		// Handle connector updates (mutually exclusive)
		if req.ConnectorType != nil && *req.ConnectorType != "" {
			policy.ConnectorType = req.ConnectorType
			policy.ConnectorID = nil // Clear the other
		}
		if req.ConnectorID != nil && *req.ConnectorID != "" {
			policy.ConnectorID = req.ConnectorID
			policy.ConnectorType = nil // Clear the other
		}

		// Handle matcher config update
		if req.DeterministicFields != nil {
			policy.DeterministicFields = req.DeterministicFields
		}
		if req.ScoringConfig != nil {
			if err := validateScoringConfig(req.ScoringConfig); err != nil {
				return nil, err
			}
			policy.ScoringConfig = req.ScoringConfig
		}
	}

	// Update in storage
	err = s.store.UpdatePolicy(ctx, policy)
	if err != nil {
		return nil, newStorageError(err, "updating policy")
	}

	return policy, nil
}
