package main

import (
	"context"
	"fmt"

	"github.com/formancehq/reconciliation/internal/models"
)

// strPtr returns a pointer to a string.
func strPtr(s string) *string {
	return &s
}

// PolicyDefinition represents a test policy definition.
type PolicyDefinition struct {
	Name        string
	Description string
	Request     CreatePolicyV2Request
}

// ScenarioConfig holds the provider/ledger configuration for a test scenario.
// Each scenario MUST use unique providers to avoid cross-contamination of transactions.
type ScenarioConfig struct {
	LedgerName    string
	ConnectorType string
}

// GetScenarioConfigs returns provider configurations for each test scenario.
// IMPORTANT: Each scenario uses a UNIQUE ledger and connector to ensure
// transactions are only matched by their intended policy.
func GetScenarioConfigs() map[string]ScenarioConfig {
	return map[string]ScenarioConfig{
		"perfect-match":  {LedgerName: "test-ledger-perfect", ConnectorType: "test-provider-perfect"},
		"probabilistic":  {LedgerName: "test-ledger-prob", ConnectorType: "test-provider-prob"},
		"orphan-ledger":  {LedgerName: "test-ledger-orphan-l", ConnectorType: "test-provider-orphan-l"},
		"orphan-payment": {LedgerName: "test-ledger-orphan-p", ConnectorType: "test-provider-orphan-p"},
		"one-to-many":    {LedgerName: "test-ledger-1toN", ConnectorType: "test-provider-1toN"},
		"many-to-one":    {LedgerName: "test-ledger-Nto1", ConnectorType: "test-provider-Nto1"},
	}
}

// GetTestPolicies returns the list of test policies to create.
// Each policy uses a UNIQUE ledger_name and connector_type to ensure isolation.
func GetTestPolicies() []PolicyDefinition {
	configs := GetScenarioConfigs()

	return []PolicyDefinition{
		{
			Name:        "test-perfect-match-usd",
			Description: "Deterministic matching by external_id for USD transactions (1:1)",
			Request: CreatePolicyV2Request{
				Name: "test-perfect-match-usd",
				Type: "transaction",
				Ledger: LedgerConfigV2{
					Name: configs["perfect-match"].LedgerName,
				},
				Payment: PaymentConfigTransactionV2{
					ConnectorType: strPtr(configs["perfect-match"].ConnectorType),
				},
				Matcher: &MatcherConfigV2{
					DeterministicFields: []string{"external_id"},
				},
			},
		},
		{
			Name:        "test-probabilistic-match-eur",
			Description: "Probabilistic matching with 3% tolerance and 2h time window (1:1)",
			Request: CreatePolicyV2Request{
				Name: "test-probabilistic-match-eur",
				Type: "transaction",
				Ledger: LedgerConfigV2{
					Name: configs["probabilistic"].LedgerName,
				},
				Payment: PaymentConfigTransactionV2{
					ConnectorType: strPtr(configs["probabilistic"].ConnectorType),
				},
				Matcher: &MatcherConfigV2{
					Scoring: &models.ScoringConfig{
						TimeWindowHours:         2,
						AmountTolerancePercent:  3.0,
						MetadataFields:          []string{"order_id", "user_id"},
						MetadataCaseInsensitive: false,
						Weights: &models.ScoringWeights{
							Amount:   0.4,
							Date:     0.3,
							Metadata: 0.3,
						},
						Thresholds: &models.ScoringThresholds{
							AutoMatch: 0.85,
							Review:    0.65,
						},
					},
				},
			},
		},
		{
			Name:        "test-orphan-ledger-usd",
			Description: "Detect orphan ledger entries (no matching payments)",
			Request: CreatePolicyV2Request{
				Name: "test-orphan-ledger-usd",
				Type: "transaction",
				Ledger: LedgerConfigV2{
					Name: configs["orphan-ledger"].LedgerName,
				},
				Payment: PaymentConfigTransactionV2{
					ConnectorType: strPtr(configs["orphan-ledger"].ConnectorType),
				},
				Matcher: &MatcherConfigV2{
					DeterministicFields: []string{"external_id"},
				},
			},
		},
		{
			Name:        "test-orphan-payment-gbp",
			Description: "Detect orphan payments (no matching ledger entries)",
			Request: CreatePolicyV2Request{
				Name: "test-orphan-payment-gbp",
				Type: "transaction",
				Ledger: LedgerConfigV2{
					Name: configs["orphan-payment"].LedgerName,
				},
				Payment: PaymentConfigTransactionV2{
					ConnectorType: strPtr(configs["orphan-payment"].ConnectorType),
				},
				Matcher: &MatcherConfigV2{
					DeterministicFields: []string{"external_id"},
				},
			},
		},
		{
			Name:        "test-one-to-many-usd",
			Description: "1:N topology - 1 ledger entry matches N payments",
			Request: CreatePolicyV2Request{
				Name: "test-one-to-many-usd",
				Type: "transaction",
				Ledger: LedgerConfigV2{
					Name: configs["one-to-many"].LedgerName,
				},
				Payment: PaymentConfigTransactionV2{
					ConnectorType: strPtr(configs["one-to-many"].ConnectorType),
				},
				Matcher: &MatcherConfigV2{
					DeterministicFields: []string{"external_id"},
				},
			},
		},
		{
			Name:        "test-many-to-one-eur",
			Description: "N:1 topology - N ledger entries match 1 payment",
			Request: CreatePolicyV2Request{
				Name: "test-many-to-one-eur",
				Type: "transaction",
				Ledger: LedgerConfigV2{
					Name: configs["many-to-one"].LedgerName,
				},
				Payment: PaymentConfigTransactionV2{
					ConnectorType: strPtr(configs["many-to-one"].ConnectorType),
				},
				Matcher: &MatcherConfigV2{
					DeterministicFields: []string{"external_id"},
				},
			},
		},
	}
}

// scenarioToPolicyName maps scenario names to policy names.
var scenarioToPolicyName = map[string]string{
	"perfect-match":  "test-perfect-match-usd",
	"probabilistic":  "test-probabilistic-match-eur",
	"orphan-ledger":  "test-orphan-ledger-usd",
	"orphan-payment": "test-orphan-payment-gbp",
	"one-to-many":    "test-one-to-many-usd",
	"many-to-one":    "test-many-to-one-eur",
}

// CreateTestPolicies creates test policies and returns a map of policy names to IDs.
// If scenarioFilter is non-nil, only creates policies for the specified scenarios.
func CreateTestPolicies(ctx context.Context, client *APIClient, verbose bool, scenarioFilter map[string]bool) (map[string]string, error) {
	policies := GetTestPolicies()
	policyIDs := make(map[string]string)

	// Build a set of policy names to create based on scenario filter
	policyNamesToCreate := make(map[string]bool)
	for scenario := range scenarioFilter {
		if policyName, ok := scenarioToPolicyName[scenario]; ok {
			policyNamesToCreate[policyName] = true
		}
	}

	for _, p := range policies {
		// Skip if not in filter
		if scenarioFilter != nil && !policyNamesToCreate[p.Name] {
			if verbose {
				fmt.Printf("      Skipping: %s (not in scenario filter)\n", p.Name)
			}
			continue
		}

		policy, err := client.CreatePolicyV2(ctx, &p.Request)
		if err != nil {
			return nil, fmt.Errorf("failed to create policy %s: %w", p.Name, err)
		}

		policyIDs[p.Name] = policy.ID
		fmt.Printf("      Created: %s (id: %s)\n", p.Name, policy.ID)

		if verbose {
			fmt.Printf("               Description: %s\n", p.Description)
		}
	}

	return policyIDs, nil
}
