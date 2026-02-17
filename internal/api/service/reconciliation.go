package service

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type ReconciliationRequest struct {
	ReconciledAtLedger   time.Time `json:"reconciledAtLedger"`
	ReconciledAtPayments time.Time `json:"reconciledAtPayments"`
	PolicyVersion        *int64    `json:"policyVersion,omitempty"`
}

func (r *ReconciliationRequest) Validate() error {
	if r.ReconciledAtLedger.IsZero() {
		return errors.New("missing reconciledAtLedger")
	}

	if r.ReconciledAtLedger.After(time.Now()) {
		return errors.New("reconciledAtLedger must be in the past")
	}

	if r.ReconciledAtPayments.IsZero() {
		return errors.New("missing reconciledAtPayments")
	}

	if r.ReconciledAtPayments.After(time.Now()) {
		return errors.New("ReconciledAtPayments must be in the past")
	}

	return nil
}

func (s *Service) Reconciliation(ctx context.Context, policyID string, req *ReconciliationRequest) (*models.Reconciliation, error) {
	id, err := uuid.Parse(policyID)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	eg, ctxGroup := errgroup.WithContext(ctx)
	var policy *models.Policy
	if req.PolicyVersion != nil {
		policy, err = s.store.GetPolicyVersion(ctx, id, *req.PolicyVersion)
		if err != nil {
			return nil, newStorageError(err, "failed to get policy version")
		}
	} else {
		policy, err = s.store.GetPolicy(ctx, id)
		if err != nil {
			return nil, newStorageError(err, "failed to get policy")
		}
	}

	var paymentsBalances map[string]*big.Int
	eg.Go(func() error {
		var err error
		paymentsBalances, err = s.getPaymentPoolBalance(ctxGroup, policy.PaymentsPoolID.String(), req.ReconciledAtPayments)
		return err
	})

	var ledgerBalances map[string]*big.Int
	eg.Go(func() error {
		var err error
		ledgerBalances, err = s.getAccountsAggregatedBalance(ctxGroup, policy.LedgerName, policy.LedgerQuery, req.ReconciledAtLedger)
		return err
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if normalized := models.NormalizePolicyLifecycle(policy.Lifecycle); normalized != models.PolicyLifecycleEnabled {
		return nil, fmt.Errorf("%w: policy is not enabled", ErrValidation)
	}

	assertionMode := models.NormalizeAssertionMode(policy.AssertionMode)
	if !assertionMode.IsValid() {
		return nil, fmt.Errorf("%w: invalid assertion mode on policy", ErrValidation)
	}

	var minBuffer *minBufferConfig
	if assertionMode == models.AssertionModeMinBuffer {
		minBuffer, err = parseMinBufferConfig(policy.AssertionConfig)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid assertionConfig on policy: %s", ErrValidation, err.Error())
		}
	}

	res := &models.Reconciliation{
		ID:                   uuid.New(),
		PolicyID:             policy.PolicyID,
		PolicyVersion:        policy.Version,
		CreatedAt:            time.Now().UTC(),
		ReconciledAtLedger:   req.ReconciledAtLedger,
		ReconciledAtPayments: req.ReconciledAtPayments,
		Status:               models.ReconciliationOK,
		PaymentsBalances:     paymentsBalances,
		LedgerBalances:       ledgerBalances,
		DriftBalances:        make(map[string]*big.Int),
	}

	for asset, ledgerBalance := range ledgerBalances {
		paymentBalance, ok := paymentsBalances[asset]
		if !ok {
			paymentBalance = nil
		}

		err := s.computeDrift(res, asset, ledgerBalance, paymentBalance, assertionMode, minBuffer)
		if err != nil {
			res.Status = models.ReconciliationNotOK
			if res.Error == "" {
				res.Error = err.Error()
			} else {
				res.Error = res.Error + "; " + err.Error()
			}
		}
	}

	if err := s.store.CreateReconciation(ctx, res); err != nil {
		return nil, newStorageError(err, "failed to create reconciliation")
	}

	return res, nil
}

func (s *Service) computeDrift(
	res *models.Reconciliation,
	asset string,
	ledgerBalance *big.Int,
	paymentBalance *big.Int,
	assertionMode models.AssertionMode,
	minBuffer *minBufferConfig,
) error {
	switch {
	case ledgerBalance == nil && paymentBalance == nil:
		// Not possible
		return nil
	case ledgerBalance == nil && paymentBalance != nil:
		var balance big.Int
		balance.Set(paymentBalance).Abs(&balance)
		res.DriftBalances[asset] = &balance
		return fmt.Errorf("missing asset %s in ledgerBalances", asset)
	case ledgerBalance != nil && paymentBalance == nil:
		var balance big.Int
		balance.Set(ledgerBalance).Abs(&balance)
		res.DriftBalances[asset] = &balance
		return fmt.Errorf("missing asset %s in paymentBalances", asset)
	case ledgerBalance != nil && paymentBalance != nil:
		var drift big.Int
		drift.Set(paymentBalance).Add(&drift, ledgerBalance)

		err := evaluateDrift(assertionMode, asset, &drift, ledgerBalance, minBuffer)
		res.DriftBalances[asset] = drift.Abs(&drift)
		return err
	}

	return nil
}

func evaluateDrift(
	assertionMode models.AssertionMode,
	asset string,
	drift *big.Int,
	ledgerBalance *big.Int,
	minBuffer *minBufferConfig,
) error {
	switch assertionMode {
	case models.AssertionModeCoverage:
		if drift.Cmp(big.NewInt(0)) < 0 {
			return fmt.Errorf("balance drift for asset %s", asset)
		}
		return nil
	case models.AssertionModeEquality:
		if drift.Cmp(big.NewInt(0)) != 0 {
			return fmt.Errorf("equality drift for asset %s", asset)
		}
		return nil
	case models.AssertionModeMinBuffer:
		required, err := requiredBufferForAsset(minBuffer, asset, ledgerBalance)
		if err != nil {
			return err
		}
		if drift.Cmp(required) < 0 {
			return fmt.Errorf("min buffer drift for asset %s", asset)
		}
		return nil
	default:
		return fmt.Errorf("unknown assertion mode %s", assertionMode)
	}
}

func requiredBufferForAsset(cfg *minBufferConfig, asset string, ledgerBalance *big.Int) (*big.Int, error) {
	if cfg == nil {
		return nil, errors.New("missing assertionConfig for MIN_BUFFER")
	}

	rule, ok := cfg.Assets[asset]
	if !ok {
		rule, ok = cfg.Assets["*"]
		if !ok {
			return nil, fmt.Errorf("missing MIN_BUFFER rule for asset %s", asset)
		}
	}

	if rule.Absolute != nil {
		return big.NewInt(*rule.Absolute), nil
	}
	if rule.BPS != nil {
		var exposure big.Int
		exposure.Abs(ledgerBalance)

		var required big.Int
		required.Mul(&exposure, big.NewInt(*rule.BPS))
		required.Div(&required, big.NewInt(10000))
		return &required, nil
	}

	return nil, fmt.Errorf("invalid MIN_BUFFER rule for asset %s", asset)
}

func (s *Service) GetReconciliation(ctx context.Context, id string) (*models.Reconciliation, error) {
	rID, err := uuid.Parse(id)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, err.Error())
	}

	reco, err := s.store.GetReconciliation(ctx, rID)
	return reco, newStorageError(err, "getting reconciliation")
}

func (s *Service) ListReconciliations(ctx context.Context, q storage.GetReconciliationsQuery) (*bunpaginate.Cursor[models.Reconciliation], error) {
	reconciliations, err := s.store.ListReconciliations(ctx, q)
	return reconciliations, newStorageError(err, "listing reconciliations")
}
