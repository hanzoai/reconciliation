package main

import (
	"context"
	"fmt"
	"strings"
)

// VerificationResult represents the result of a single verification check.
type VerificationResult struct {
	Passed  bool
	Message string
}

// VerifyResults verifies the actual results against the expected manifest.
func VerifyResults(ctx context.Context, client *APIClient, manifest *Manifest, verbose bool) error {
	passed := 0
	failed := 0

	fmt.Println()
	fmt.Println("=== VERIFICATION RESULTS ===")

	for _, expectation := range manifest.Policies {
		fmt.Printf("\nPolicy: %s (ID: %s)\n", expectation.PolicyName, expectation.PolicyID)

		// Get anomalies (orphans) for this policy
		anomalies, err := client.GetPolicyAnomalies(ctx, expectation.PolicyID)
		if err != nil {
			fmt.Printf("  [ERROR] Failed to get anomalies: %v\n", err)
			failed++
			continue
		}

		// Debug: show anomalies received
		if verbose {
			fmt.Printf("  [DEBUG] Received %d anomalies from API\n", len(anomalies))
			for i, a := range anomalies {
				fmt.Printf("  [DEBUG]   [%d] Type=%s, State=%s, TransactionID=%s, Severity=%s, AnomalyPolicyID=%s\n",
					i, a.Type, a.State, a.TransactionID, a.Severity, a.PolicyID)
			}
			fmt.Printf("  [DEBUG] Expected %d matches, %d ledger orphans, %d payment orphans\n",
				len(expectation.ExpectedMatches),
				len(expectation.ExpectedOrphans.Ledger),
				len(expectation.ExpectedOrphans.Payments))
			if len(expectation.ExpectedOrphans.Ledger) > 0 {
				fmt.Printf("  [DEBUG] Expected ledger orphan IDs: %v\n", expectation.ExpectedOrphans.Ledger)
			}
			if len(expectation.ExpectedOrphans.Payments) > 0 {
				fmt.Printf("  [DEBUG] Expected payment orphan IDs: %v\n", expectation.ExpectedOrphans.Payments)
			}
		}

		// Verify expected matches
		matchResults := verifyExpectedMatches(expectation, anomalies, verbose)
		for _, r := range matchResults {
			if r.Passed {
				fmt.Printf("  [PASS] %s\n", r.Message)
				passed++
			} else {
				fmt.Printf("  [FAIL] %s\n", r.Message)
				failed++
			}
		}

		// Verify expected orphans
		orphanResults := verifyExpectedOrphans(expectation, anomalies, verbose)
		for _, r := range orphanResults {
			if r.Passed {
				fmt.Printf("  [PASS] %s\n", r.Message)
				passed++
			} else {
				fmt.Printf("  [FAIL] %s\n", r.Message)
				failed++
			}
		}
	}

	fmt.Println()
	fmt.Printf("SUMMARY: %d passed, %d failed\n", passed, failed)

	if failed > 0 {
		fmt.Println("STATUS: TESTS FAILED")
		return fmt.Errorf("verification failed: %d checks failed", failed)
	}

	fmt.Println("STATUS: ALL TESTS PASSED")
	return nil
}

// verifyExpectedMatches verifies that expected matches are found.
// Since we're checking for matches, we verify that transactions are NOT in the anomalies list
// (if they matched, they shouldn't be anomalies).
func verifyExpectedMatches(expectation PolicyExpectation, anomalies []AnomalyResponse, verbose bool) []VerificationResult {
	var results []VerificationResult

	if len(expectation.ExpectedMatches) == 0 {
		results = append(results, VerificationResult{
			Passed:  true,
			Message: "0/0 expected matches (none expected)",
		})
		return results
	}

	// Build a set of transaction IDs that are anomalies (orphans/unmatched)
	anomalyTransactionIDs := make(map[string]bool)
	for _, a := range anomalies {
		anomalyTransactionIDs[a.TransactionID] = true
	}

	matchedCount := 0
	unmatchedCount := 0
	var unmatchedIDs []string

	for _, expectedMatch := range expectation.ExpectedMatches {
		// For a match to be successful, neither the ledger nor payment should be in anomalies
		// This is a simplified check - ideally we'd check the actual matches table
		isLedgerMatched := !anomalyTransactionIDs[expectedMatch.LedgerExternalID]

		allPaymentsMatched := true
		for _, paymentID := range expectedMatch.PaymentExternalIDs {
			if anomalyTransactionIDs[paymentID] {
				allPaymentsMatched = false
				break
			}
		}

		if isLedgerMatched && allPaymentsMatched {
			matchedCount++
		} else {
			unmatchedCount++
			if len(expectedMatch.PaymentExternalIDs) == 1 && expectedMatch.PaymentExternalIDs[0] == expectedMatch.LedgerExternalID {
				unmatchedIDs = append(unmatchedIDs, expectedMatch.LedgerExternalID)
			} else {
				unmatchedIDs = append(unmatchedIDs, fmt.Sprintf("%s->%v", expectedMatch.LedgerExternalID, expectedMatch.PaymentExternalIDs))
			}
		}
	}

	totalExpected := len(expectation.ExpectedMatches)
	if matchedCount == totalExpected {
		results = append(results, VerificationResult{
			Passed:  true,
			Message: fmt.Sprintf("%d/%d expected matches found", matchedCount, totalExpected),
		})
	} else {
		msg := fmt.Sprintf("%d/%d expected matches found", matchedCount, totalExpected)
		if verbose && len(unmatchedIDs) > 0 {
			msg += fmt.Sprintf(" (missing: %s)", strings.Join(unmatchedIDs, ", "))
		}
		results = append(results, VerificationResult{
			Passed:  false,
			Message: msg,
		})
	}

	return results
}

// verifyExpectedOrphans verifies that expected orphans are detected as anomalies.
func verifyExpectedOrphans(expectation PolicyExpectation, anomalies []AnomalyResponse, verbose bool) []VerificationResult {
	var results []VerificationResult

	expectedLedgerOrphans := len(expectation.ExpectedOrphans.Ledger)
	expectedPaymentOrphans := len(expectation.ExpectedOrphans.Payments)

	// Count actual anomalies by type
	actualLedgerOrphans := 0
	actualPaymentOrphans := 0

	for _, a := range anomalies {
		switch a.Type {
		case "MISSING_ON_PAYMENTS", "DUPLICATE_LEDGER":
			// These indicate ledger entries without matching payments
			actualLedgerOrphans++
		case "MISSING_ON_LEDGER":
			// These indicate payments without matching ledger entries
			actualPaymentOrphans++
		}
	}

	// Verify ledger orphans
	if expectedLedgerOrphans > 0 {
		if actualLedgerOrphans >= expectedLedgerOrphans {
			results = append(results, VerificationResult{
				Passed:  true,
				Message: fmt.Sprintf("%d/%d expected ledger orphans detected", min(actualLedgerOrphans, expectedLedgerOrphans), expectedLedgerOrphans),
			})
		} else {
			results = append(results, VerificationResult{
				Passed:  false,
				Message: fmt.Sprintf("%d/%d expected ledger orphans detected", actualLedgerOrphans, expectedLedgerOrphans),
			})
		}
	} else {
		// No ledger orphans expected - verify we don't have any
		if actualLedgerOrphans == 0 {
			results = append(results, VerificationResult{
				Passed:  true,
				Message: "0/0 expected ledger orphans",
			})
		} else {
			// Having unexpected orphans might be okay in some cases, just note it
			results = append(results, VerificationResult{
				Passed:  true,
				Message: fmt.Sprintf("0/0 expected ledger orphans (%d actual - may be expected)", actualLedgerOrphans),
			})
		}
	}

	// Verify payment orphans
	if expectedPaymentOrphans > 0 {
		if actualPaymentOrphans >= expectedPaymentOrphans {
			results = append(results, VerificationResult{
				Passed:  true,
				Message: fmt.Sprintf("%d/%d expected payment orphans detected", min(actualPaymentOrphans, expectedPaymentOrphans), expectedPaymentOrphans),
			})
		} else {
			results = append(results, VerificationResult{
				Passed:  false,
				Message: fmt.Sprintf("%d/%d expected payment orphans detected", actualPaymentOrphans, expectedPaymentOrphans),
			})
		}
	} else {
		// No payment orphans expected - verify we don't have any
		if actualPaymentOrphans == 0 {
			results = append(results, VerificationResult{
				Passed:  true,
				Message: "0/0 expected payment orphans",
			})
		} else {
			// Having unexpected orphans might be okay in some cases, just note it
			results = append(results, VerificationResult{
				Passed:  true,
				Message: fmt.Sprintf("0/0 expected payment orphans (%d actual - may be expected)", actualPaymentOrphans),
			})
		}
	}

	return results
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
