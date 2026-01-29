package main

import (
	"time"
)

// ExpectedMatch represents an expected match between ledger and payment transactions.
type ExpectedMatch struct {
	LedgerExternalID   string   `json:"ledgerExternalId"`
	PaymentExternalIDs []string `json:"paymentExternalIds"` // Supports 1:N and N:1
}

// ExpectedOrphans represents expected orphan transactions.
type ExpectedOrphans struct {
	Ledger   []string `json:"ledger"`
	Payments []string `json:"payments"`
}

// PolicyExpectation represents expected results for a specific policy.
type PolicyExpectation struct {
	PolicyID        string          `json:"policyId"`
	PolicyName      string          `json:"policyName"`
	ExpectedMatches []ExpectedMatch `json:"expectedMatches"`
	ExpectedOrphans ExpectedOrphans `json:"expectedOrphans"`
}

// Manifest contains all expected test results.
type Manifest struct {
	GeneratedAt time.Time           `json:"generatedAt"`
	Policies    []PolicyExpectation `json:"policies"`
}

// NewManifest creates a new manifest with the current timestamp.
func NewManifest() *Manifest {
	return &Manifest{
		GeneratedAt: time.Now().UTC(),
		Policies:    make([]PolicyExpectation, 0),
	}
}

// AddPolicyExpectation adds a policy expectation to the manifest.
func (m *Manifest) AddPolicyExpectation(pe PolicyExpectation) {
	m.Policies = append(m.Policies, pe)
}

// SeededTransaction tracks a transaction that was seeded for testing.
type SeededTransaction struct {
	ExternalID string
	Side       string // "ledger" or "payment"
	PolicyName string
	Amount     int64
	Currency   string
}

// SeededPair tracks a ledger/payment pair that was seeded together.
type SeededPair struct {
	LedgerExternalID   string
	PaymentExternalIDs []string
	PolicyName         string
}

// SeedTracker tracks what was seeded for manifest generation.
type SeedTracker struct {
	PerfectMatches       []SeededPair
	ProbabilisticMatches []SeededPair
	OrphanLedger         []SeededTransaction
	OrphanPayments       []SeededTransaction
	OneToManyMatches     []SeededPair
	ManyToOneMatches     []SeededPair
}

// NewSeedTracker creates a new seed tracker.
func NewSeedTracker() *SeedTracker {
	return &SeedTracker{
		PerfectMatches:       make([]SeededPair, 0),
		ProbabilisticMatches: make([]SeededPair, 0),
		OrphanLedger:         make([]SeededTransaction, 0),
		OrphanPayments:       make([]SeededTransaction, 0),
		OneToManyMatches:     make([]SeededPair, 0),
		ManyToOneMatches:     make([]SeededPair, 0),
	}
}

// ToManifest converts the seed tracker to a manifest for verification.
func (st *SeedTracker) ToManifest(policyIDs map[string]string) *Manifest {
	manifest := NewManifest()

	// Perfect match policy expectation
	if policyID, ok := policyIDs["test-perfect-match-usd"]; ok {
		pe := PolicyExpectation{
			PolicyID:        policyID,
			PolicyName:      "test-perfect-match-usd",
			ExpectedMatches: make([]ExpectedMatch, 0, len(st.PerfectMatches)),
			ExpectedOrphans: ExpectedOrphans{},
		}
		for _, pair := range st.PerfectMatches {
			pe.ExpectedMatches = append(pe.ExpectedMatches, ExpectedMatch{
				LedgerExternalID:   pair.LedgerExternalID,
				PaymentExternalIDs: pair.PaymentExternalIDs,
			})
		}
		manifest.AddPolicyExpectation(pe)
	}

	// Probabilistic match policy expectation
	if policyID, ok := policyIDs["test-probabilistic-match-eur"]; ok {
		pe := PolicyExpectation{
			PolicyID:        policyID,
			PolicyName:      "test-probabilistic-match-eur",
			ExpectedMatches: make([]ExpectedMatch, 0, len(st.ProbabilisticMatches)),
			ExpectedOrphans: ExpectedOrphans{},
		}
		for _, pair := range st.ProbabilisticMatches {
			pe.ExpectedMatches = append(pe.ExpectedMatches, ExpectedMatch{
				LedgerExternalID:   pair.LedgerExternalID,
				PaymentExternalIDs: pair.PaymentExternalIDs,
			})
		}
		manifest.AddPolicyExpectation(pe)
	}

	// Orphan ledger policy expectation
	if policyID, ok := policyIDs["test-orphan-ledger-usd"]; ok {
		pe := PolicyExpectation{
			PolicyID:        policyID,
			PolicyName:      "test-orphan-ledger-usd",
			ExpectedMatches: []ExpectedMatch{},
			ExpectedOrphans: ExpectedOrphans{
				Ledger:   make([]string, 0, len(st.OrphanLedger)),
				Payments: []string{},
			},
		}
		for _, tx := range st.OrphanLedger {
			pe.ExpectedOrphans.Ledger = append(pe.ExpectedOrphans.Ledger, tx.ExternalID)
		}
		manifest.AddPolicyExpectation(pe)
	}

	// Orphan payment policy expectation
	if policyID, ok := policyIDs["test-orphan-payment-gbp"]; ok {
		pe := PolicyExpectation{
			PolicyID:        policyID,
			PolicyName:      "test-orphan-payment-gbp",
			ExpectedMatches: []ExpectedMatch{},
			ExpectedOrphans: ExpectedOrphans{
				Ledger:   []string{},
				Payments: make([]string, 0, len(st.OrphanPayments)),
			},
		}
		for _, tx := range st.OrphanPayments {
			pe.ExpectedOrphans.Payments = append(pe.ExpectedOrphans.Payments, tx.ExternalID)
		}
		manifest.AddPolicyExpectation(pe)
	}

	// One-to-many policy expectation
	if policyID, ok := policyIDs["test-one-to-many-usd"]; ok {
		pe := PolicyExpectation{
			PolicyID:        policyID,
			PolicyName:      "test-one-to-many-usd",
			ExpectedMatches: make([]ExpectedMatch, 0, len(st.OneToManyMatches)),
			ExpectedOrphans: ExpectedOrphans{},
		}
		for _, pair := range st.OneToManyMatches {
			pe.ExpectedMatches = append(pe.ExpectedMatches, ExpectedMatch{
				LedgerExternalID:   pair.LedgerExternalID,
				PaymentExternalIDs: pair.PaymentExternalIDs,
			})
		}
		manifest.AddPolicyExpectation(pe)
	}

	// Many-to-one policy expectation
	if policyID, ok := policyIDs["test-many-to-one-eur"]; ok {
		pe := PolicyExpectation{
			PolicyID:        policyID,
			PolicyName:      "test-many-to-one-eur",
			ExpectedMatches: make([]ExpectedMatch, 0, len(st.ManyToOneMatches)),
			ExpectedOrphans: ExpectedOrphans{},
		}
		for _, pair := range st.ManyToOneMatches {
			pe.ExpectedMatches = append(pe.ExpectedMatches, ExpectedMatch{
				LedgerExternalID:   pair.LedgerExternalID,
				PaymentExternalIDs: pair.PaymentExternalIDs,
			})
		}
		manifest.AddPolicyExpectation(pe)
	}

	return manifest
}
