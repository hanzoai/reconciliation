package main

import (
	"context"
	"fmt"
	"strings"
)

// CleanupTestPolicies deletes all policies with the "test-" prefix.
func CleanupTestPolicies(ctx context.Context, client *APIClient, verbose bool) error {
	policies, err := client.ListPoliciesV2(ctx)
	if err != nil {
		return fmt.Errorf("failed to list policies: %w", err)
	}

	count := 0
	for _, p := range policies {
		// Only delete policies with "test-" prefix
		if strings.HasPrefix(p.Name, "test-") {
			if verbose {
				fmt.Printf("      Deleting: %s (%s)\n", p.Name, p.ID)
			}
			if err := client.DeletePolicy(ctx, p.ID); err != nil {
				return fmt.Errorf("failed to delete policy %s: %w", p.Name, err)
			}
			count++
		}
	}

	fmt.Printf("      Deleted %d policies\n", count)
	return nil
}

// CleanupTestTransactions deletes all transactions for test providers.
// If scenarioFilter is non-nil, only cleans up the specified scenarios.
func CleanupTestTransactions(ctx context.Context, client *APIClient, verbose bool, scenarioFilter map[string]bool) error {
	var totalLedger, totalPayment int64

	// Get all scenario configurations to clean up their providers
	configs := GetScenarioConfigs()

	for scenario, cfg := range configs {
		// Skip if not in filter
		if scenarioFilter != nil && !scenarioFilter[scenario] {
			continue
		}

		// Clean up ledger transactions for this scenario
		ledgerDeleted, err := client.CleanupTransactions(ctx, cfg.LedgerName, "LEDGER")
		if err != nil {
			if verbose {
				fmt.Printf("      Warning: failed to cleanup ledger transactions for %s: %v\n", scenario, err)
			}
			// Don't fail - the endpoint might not exist on older versions
		} else {
			totalLedger += ledgerDeleted
			if verbose && ledgerDeleted > 0 {
				fmt.Printf("      Deleted %d ledger transactions for %s\n", ledgerDeleted, scenario)
			}
		}

		// Clean up payment transactions for this scenario
		paymentDeleted, err := client.CleanupTransactions(ctx, cfg.ConnectorType, "PAYMENTS")
		if err != nil {
			if verbose {
				fmt.Printf("      Warning: failed to cleanup payment transactions for %s: %v\n", scenario, err)
			}
		} else {
			totalPayment += paymentDeleted
			if verbose && paymentDeleted > 0 {
				fmt.Printf("      Deleted %d payment transactions for %s\n", paymentDeleted, scenario)
			}
		}
	}

	fmt.Printf("      Deleted %d ledger transactions\n", totalLedger)
	fmt.Printf("      Deleted %d payment transactions\n", totalPayment)
	fmt.Printf("      Deleted %d transactions total\n", totalLedger+totalPayment)
	return nil
}
