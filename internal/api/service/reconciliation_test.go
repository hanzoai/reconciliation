package service

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestReconciliation(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name             string
		ledgerVersion    string
		paymentsVersion  string
		ledgerBalances   map[string]*big.Int
		paymentsBalances map[string]*big.Int
		policy           *models.Policy
		expectedReco     *models.Reconciliation
		expectedError    bool
	}

	testCases := []testCase{
		{
			name:            "nominal with drift = 0",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(100),
				"EUR": big.NewInt(200),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
				"EUR": big.NewInt(-200),
			},
			expectedReco: &models.Reconciliation{
				ReconciledAtLedger:   time.Time{},
				ReconciledAtPayments: time.Time{},
				Status:               models.ReconciliationOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(100),
					"EUR": big.NewInt(200),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-100),
					"EUR": big.NewInt(-200),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(0),
					"EUR": big.NewInt(0),
				},
				Error: "",
			},
		},
		{
			name:            "nominal with drift >= 0",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(200),
				"EUR": big.NewInt(300),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
				"EUR": big.NewInt(-200),
			},
			expectedReco: &models.Reconciliation{
				ReconciledAtLedger:   time.Time{},
				ReconciledAtPayments: time.Time{},
				Status:               models.ReconciliationOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(200),
					"EUR": big.NewInt(300),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-100),
					"EUR": big.NewInt(-200),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(100),
					"EUR": big.NewInt(100),
				},
				Error: "",
			},
		},
		{
			name:            "nominal with drift < 0",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(100),
				"EUR": big.NewInt(200),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
				"EUR": big.NewInt(-400),
			},
			expectedReco: &models.Reconciliation{
				ReconciledAtLedger:   time.Time{},
				ReconciledAtPayments: time.Time{},
				Status:               models.ReconciliationNotOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(100),
					"EUR": big.NewInt(200),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-100),
					"EUR": big.NewInt(-400),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(0),
					"EUR": big.NewInt(200),
				},
				Error: "balance drift for asset EUR",
			},
		},
		{
			name:            "payments extra assets are ignored",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"EUR": big.NewInt(200),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
				"EUR": big.NewInt(-200),
			},
			expectedReco: &models.Reconciliation{
				ReconciledAtLedger:   time.Time{},
				ReconciledAtPayments: time.Time{},
				Status:               models.ReconciliationOK,
				LedgerBalances: map[string]*big.Int{
					"EUR": big.NewInt(200),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-100),
					"EUR": big.NewInt(-200),
				},
				DriftBalances: map[string]*big.Int{
					"EUR": big.NewInt(0),
				},
				Error: "",
			},
		},
		{
			name:            "same length, different asset and no drift",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(100),
				"EUR": big.NewInt(200),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
				"DKK": big.NewInt(-200),
			},
			expectedReco: &models.Reconciliation{
				ReconciledAtLedger:   time.Time{},
				ReconciledAtPayments: time.Time{},
				Status:               models.ReconciliationNotOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(100),
					"EUR": big.NewInt(200),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-100),
					"DKK": big.NewInt(-200),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(0),
					"EUR": big.NewInt(200),
				},
				Error: "missing asset EUR in paymentBalances",
			},
		},
		{
			name:            "missing payments balance with ledger balance at 0",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(100),
				"EUR": big.NewInt(0),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
			},
			expectedReco: &models.Reconciliation{
				ReconciledAtLedger:   time.Time{},
				ReconciledAtPayments: time.Time{},
				Status:               models.ReconciliationNotOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(100),
					"EUR": big.NewInt(0),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-100),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(0),
					"EUR": big.NewInt(0),
				},
				Error: "missing asset EUR in paymentBalances",
			},
		},
		{
			name:            "missing ledger balance with payments balance at 0",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(100),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
				"EUR": big.NewInt(0),
			},
			expectedReco: &models.Reconciliation{
				ReconciledAtLedger:   time.Time{},
				ReconciledAtPayments: time.Time{},
				Status:               models.ReconciliationOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(100),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-100),
					"EUR": big.NewInt(0),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(0),
				},
				Error: "",
			},
		},
		{
			name:            "equality mode fails on positive drift",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(200),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
			},
			policy: &models.Policy{
				AssertionMode:   models.AssertionModeEquality,
				AssertionConfig: map[string]interface{}{},
			},
			expectedReco: &models.Reconciliation{
				Status: models.ReconciliationNotOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(200),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-100),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(100),
				},
				Error: "equality drift for asset USD",
			},
		},
		{
			name:            "min buffer mode absolute passes",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(200),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-150),
			},
			policy: &models.Policy{
				AssertionMode: models.AssertionModeMinBuffer,
				AssertionConfig: map[string]interface{}{
					"assets": map[string]interface{}{
						"USD": map[string]interface{}{
							"absolute": 25,
						},
					},
				},
			},
			expectedReco: &models.Reconciliation{
				Status: models.ReconciliationOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(200),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-150),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(50),
				},
				Error: "",
			},
		},
		{
			name:            "min buffer mode bps fails",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(10000),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-9500),
			},
			policy: &models.Policy{
				AssertionMode: models.AssertionModeMinBuffer,
				AssertionConfig: map[string]interface{}{
					"assets": map[string]interface{}{
						"*": map[string]interface{}{
							"bps": 600,
						},
					},
				},
			},
			expectedReco: &models.Reconciliation{
				Status: models.ReconciliationNotOK,
				LedgerBalances: map[string]*big.Int{
					"USD": big.NewInt(10000),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD": big.NewInt(-9500),
				},
				DriftBalances: map[string]*big.Int{
					"USD": big.NewInt(500),
				},
				Error: "min buffer drift for asset USD",
			},
		},
		{
			name:            "min buffer invalid config returns error",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD": big.NewInt(100),
			},
			paymentsBalances: map[string]*big.Int{
				"USD": big.NewInt(-100),
			},
			policy: &models.Policy{
				AssertionMode: models.AssertionModeMinBuffer,
				AssertionConfig: map[string]interface{}{
					"assets": map[string]interface{}{
						"*": map[string]interface{}{
							"absolute": 1,
						},
					},
				},
			},
			expectedError: true,
		},
		{
			name:            "min buffer strict list unknown asset fails at runtime",
			ledgerVersion:   "v2.0.0-beta.1",
			paymentsVersion: "v1.0.0-rc.4",
			ledgerBalances: map[string]*big.Int{
				"USD/2": big.NewInt(100),
				"EUR/2": big.NewInt(50),
			},
			paymentsBalances: map[string]*big.Int{
				"USD/2": big.NewInt(-100),
				"EUR/2": big.NewInt(-50),
			},
			policy: &models.Policy{
				AssertionMode: models.AssertionModeMinBuffer,
				AssertionConfig: map[string]interface{}{
					"assets": map[string]interface{}{
						"USD/2": map[string]interface{}{
							"bps": 0,
						},
					},
				},
			},
			expectedReco: &models.Reconciliation{
				Status: models.ReconciliationNotOK,
				LedgerBalances: map[string]*big.Int{
					"USD/2": big.NewInt(100),
					"EUR/2": big.NewInt(50),
				},
				PaymentsBalances: map[string]*big.Int{
					"USD/2": big.NewInt(-100),
					"EUR/2": big.NewInt(-50),
				},
				DriftBalances: map[string]*big.Int{
					"USD/2": big.NewInt(0),
					"EUR/2": big.NewInt(0),
				},
				Error: "missing MIN_BUFFER rule for asset EUR/2",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewService(newMockStore(tc.policy), newMockSDKFormanceClient(
				tc.ledgerVersion,
				tc.ledgerBalances,
				tc.paymentsVersion,
				tc.paymentsBalances,
			))

			reco, err := s.Reconciliation(context.Background(), uuid.New().String(), &ReconciliationRequest{})
			if tc.expectedError {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedReco.Status, reco.Status)
			compareBalancesMap(t, tc.expectedReco.LedgerBalances, reco.LedgerBalances)
			compareBalancesMap(t, tc.expectedReco.PaymentsBalances, reco.PaymentsBalances)
			compareBalancesMap(t, tc.expectedReco.DriftBalances, reco.DriftBalances)
			require.Equal(t, tc.expectedReco.Error, reco.Error)
		})
	}
}

func compareBalancesMap(t *testing.T, expected, actual map[string]*big.Int) {
	require.Equal(t, len(expected), len(actual))
	for k, v := range expected {
		require.Equal(t, v.Cmp(actual[k]), 0)
	}
}
