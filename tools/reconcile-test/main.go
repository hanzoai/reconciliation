package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

const (
	apiURLFlag        = "api-url"
	clientIDFlag      = "client-id"
	clientSecretFlag  = "client-secret"
	natsURLFlag       = "nats-url"
	ledgerTopicFlag   = "ledger-topic"
	paymentsTopicFlag = "payments-topic"
	countFlag         = "count"
	pollIntervalFlag  = "poll-interval"
	concurrencyFlag   = "concurrency"
	verboseFlag       = "verbose"
	skipCleanupFlag   = "skip-cleanup"
	skipSeedFlag      = "skip-seed"
	scenarioFlag      = "scenario"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reconcile-test",
		Short: "All-in-one reconciliation testing tool",
		Long: `Reconcile-test is an all-in-one tool for testing the reconciliation service.

It performs the following steps:
1. Cleanup - Delete existing test policies (prefix "test-")
2. Create Policies - Create test policies for each scenario
3. Seed Data - Publish test events via NATS
4. Monitor Progress - Poll API for ingestion, matches, and anomalies (Ctrl+C to stop)

The matching worker processes transactions automatically via NATS.
Press Ctrl+C to stop monitoring and display the final verification summary.

Scenarios covered:
- Perfect matches (deterministic by external_id)
- Probabilistic matches (amount tolerance, time window, metadata)
- Orphan ledger entries (no matching payments)
- Orphan payments (no matching ledger entries)
- 1:N topology (1 ledger -> N payments)
- N:1 topology (N ledgers -> 1 payment)

Authentication:
For Formance stack, provide OAuth2 credentials:
  --client-id and --client-secret (or CLIENT_ID/CLIENT_SECRET env vars)
  Uses scope: reconciliation:read reconciliation:write

For local testing without authentication, only --api-url is required.`,
		RunE: run,
	}

	// Connection flags
	cmd.Flags().String(apiURLFlag, os.Getenv("STACK_URL"), "Formance stack URL (or local API URL)")
	cmd.Flags().String(clientIDFlag, os.Getenv("CLIENT_ID"), "OAuth2 client ID (optional for local testing)")
	cmd.Flags().String(clientSecretFlag, os.Getenv("CLIENT_SECRET"), "OAuth2 client secret (optional for local testing)")
	cmd.Flags().String(natsURLFlag, "nats://localhost:4222", "NATS server URL")

	// Topic flags
	cmd.Flags().String(ledgerTopicFlag, "stacks.ledger", "Topic for ledger events")
	cmd.Flags().String(paymentsTopicFlag, "stacks.payments", "Topic for payment events")

	// Test configuration
	cmd.Flags().Int(countFlag, 10, "Base count for each scenario")
	cmd.Flags().Duration(pollIntervalFlag, 2*time.Second, "Poll interval for monitoring dashboard")
	cmd.Flags().Int(concurrencyFlag, 50, "Number of concurrent NATS publish workers")
	cmd.Flags().Bool(verboseFlag, false, "Enable verbose output")

	// Skip flags for partial runs
	cmd.Flags().Bool(skipCleanupFlag, false, "Skip cleanup step")
	cmd.Flags().Bool(skipSeedFlag, false, "Skip seeding step")

	// Scenario filter
	cmd.Flags().StringSlice(scenarioFlag, nil, "Run only specific scenarios (orphan-ledger, orphan-payment, perfect-match, probabilistic, one-to-many, many-to-one)")

	return cmd
}

func run(cmd *cobra.Command, _ []string) error {
	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Parse flags
	apiURL, _ := cmd.Flags().GetString(apiURLFlag)
	clientID, _ := cmd.Flags().GetString(clientIDFlag)
	clientSecret, _ := cmd.Flags().GetString(clientSecretFlag)
	natsURL, _ := cmd.Flags().GetString(natsURLFlag)
	ledgerTopic, _ := cmd.Flags().GetString(ledgerTopicFlag)
	paymentsTopic, _ := cmd.Flags().GetString(paymentsTopicFlag)
	count, _ := cmd.Flags().GetInt(countFlag)
	pollInterval, _ := cmd.Flags().GetDuration(pollIntervalFlag)
	concurrency, _ := cmd.Flags().GetInt(concurrencyFlag)
	verbose, _ := cmd.Flags().GetBool(verboseFlag)
	skipCleanup, _ := cmd.Flags().GetBool(skipCleanupFlag)
	skipSeed, _ := cmd.Flags().GetBool(skipSeedFlag)
	scenarios, _ := cmd.Flags().GetStringSlice(scenarioFlag)

	// Convert scenarios to a set for easy lookup
	scenarioFilter := make(map[string]bool)
	for _, s := range scenarios {
		scenarioFilter[s] = true
	}
	// If no filter specified, run all scenarios
	if len(scenarioFilter) == 0 {
		scenarioFilter = nil
	}

	// Validate required flags
	if apiURL == "" {
		return fmt.Errorf("api-url is required (use --api-url or STACK_URL env)")
	}

	// Create API client
	apiClient := NewAPIClient(apiURL, clientID, clientSecret)

	// Log authentication mode
	if clientID != "" && clientSecret != "" {
		fmt.Printf("Using OAuth2 authentication (scope: reconciliation:read reconciliation:write)\n")
	} else {
		fmt.Printf("Running without authentication (local mode)\n")
	}
	fmt.Println()

	// Step 1: Cleanup
	if !skipCleanup {
		fmt.Println("[1/4] Cleaning up existing test data...")
		if err := CleanupTestTransactions(ctx, apiClient, verbose, scenarioFilter); err != nil {
			if verbose {
				fmt.Printf("      Warning: transaction cleanup failed: %v\n", err)
			}
		}
		if err := CleanupTestPolicies(ctx, apiClient, verbose); err != nil {
			return fmt.Errorf("cleanup failed: %w", err)
		}
	} else {
		fmt.Println("[1/4] Skipping cleanup (--skip-cleanup)")
	}

	// Step 2: Create policies
	fmt.Println("[2/4] Creating test policies...")
	if len(scenarios) > 0 {
		fmt.Printf("      Filtering scenarios: %v\n", scenarios)
	}
	policyIDs, err := CreateTestPolicies(ctx, apiClient, verbose, scenarioFilter)
	if err != nil {
		return fmt.Errorf("policy creation failed: %w", err)
	}

	// Create seed tracker for manifest generation
	var tracker *SeedTracker

	// Step 3: Seed data
	if !skipSeed {
		fmt.Println("[3/4] Seeding test data...")

		publisher, err := NewPublisher(natsURL)
		if err != nil {
			return fmt.Errorf("failed to create publisher: %w", err)
		}
		defer func() { _ = publisher.Close() }()

		seedConfig := SeedConfig{
			LedgerTopic:    ledgerTopic,
			PaymentsTopic:  paymentsTopic,
			Count:          count,
			Concurrency:    concurrency,
			Verbose:        verbose,
			ScenarioFilter: scenarioFilter,
		}

		tracker, _, err = SeedTestData(ctx, publisher, seedConfig)
		if err != nil {
			return fmt.Errorf("seeding failed: %w", err)
		}
	} else {
		fmt.Println("[3/4] Skipping seeding (--skip-seed)")
		tracker = NewSeedTracker()
	}

	// Step 4: Monitor progress (runs until Ctrl+C)
	fmt.Println("[4/4] Starting monitor (press Ctrl+C to stop and verify)...")
	fmt.Println()

	monitorCtx, monitorCancel := context.WithCancel(ctx)

	// Run monitor in background
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		monitorProgress(monitorCtx, apiClient, tracker, policyIDs, MonitorConfig{
			PollInterval: pollInterval,
			Verbose:      verbose,
		})
	}()

	// Wait for signal
	<-sigCh
	fmt.Println("\n\nShutting down monitor...")
	monitorCancel()
	<-monitorDone

	// Final verification
	fmt.Println()
	manifest := tracker.ToManifest(policyIDs)
	if err := VerifyResults(ctx, apiClient, manifest, verbose); err != nil {
		return err
	}

	return nil
}
