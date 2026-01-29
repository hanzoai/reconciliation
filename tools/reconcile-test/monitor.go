package main

import (
	"context"
	"fmt"
	"time"
)

// MonitorConfig holds configuration for the progress monitor.
type MonitorConfig struct {
	PollInterval time.Duration
	Verbose      bool
}

// PolicyStatus tracks the live status of a single policy.
type PolicyStatus struct {
	Name           string
	ID             string
	ExpectedCount  int
	MatchCount     int
	AnomalyCount   int
	ExpectsMatches bool // true if we expect matches, false if we expect anomalies
	Done           bool
}

// MonitorStats holds aggregated stats for the dashboard.
type MonitorStats struct {
	TotalMatches   int
	TotalAnomalies int
	Policies       []PolicyStatus
	Elapsed        time.Duration
}

// monitorProgress polls the API in a loop and displays a live dashboard.
// It runs until ctx is cancelled (e.g. Ctrl+C).
func monitorProgress(ctx context.Context, client *APIClient, tracker *SeedTracker, policyIDs map[string]string, config MonitorConfig) {
	ticker := time.NewTicker(config.PollInterval)
	defer ticker.Stop()

	startTime := time.Now()

	// Build policy status list from manifest
	manifest := tracker.ToManifest(policyIDs)
	policyStatuses := make([]PolicyStatus, 0, len(manifest.Policies))
	for _, pe := range manifest.Policies {
		ps := PolicyStatus{
			Name: pe.PolicyName,
			ID:   pe.PolicyID,
		}
		if len(pe.ExpectedMatches) > 0 {
			ps.ExpectsMatches = true
			ps.ExpectedCount = len(pe.ExpectedMatches)
		} else {
			ps.ExpectsMatches = false
			ps.ExpectedCount = len(pe.ExpectedOrphans.Ledger) + len(pe.ExpectedOrphans.Payments)
		}
		policyStatuses = append(policyStatuses, ps)
	}

	// Initial display
	displayDashboard(MonitorStats{Policies: policyStatuses})

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := MonitorStats{
				Elapsed: time.Since(startTime),
			}

			// Poll anomalies per policy and update statuses
			updatedPolicies := make([]PolicyStatus, len(policyStatuses))
			copy(updatedPolicies, policyStatuses)

			totalMatches := 0
			totalAnomalies := 0

			for i := range updatedPolicies {
				ps := &updatedPolicies[i]
				anomalies, err := client.GetPolicyAnomalies(ctx, ps.ID)
				if err != nil {
					continue
				}

				anomalyCount := len(anomalies)

				if ps.ExpectsMatches {
					// For match policies: transactions NOT in anomalies = matched
					ps.MatchCount = ps.ExpectedCount - anomalyCount
					if ps.MatchCount < 0 {
						ps.MatchCount = 0
					}
					ps.AnomalyCount = anomalyCount
					ps.Done = ps.MatchCount >= ps.ExpectedCount
					totalMatches += ps.MatchCount
				} else {
					// For orphan policies: anomalies ARE the expected result
					ps.AnomalyCount = anomalyCount
					ps.Done = anomalyCount >= ps.ExpectedCount
					totalAnomalies += anomalyCount
				}
			}

			stats.TotalMatches = totalMatches
			stats.TotalAnomalies = totalAnomalies
			stats.Policies = updatedPolicies
			policyStatuses = updatedPolicies

			displayDashboard(stats)
		}
	}
}

// displayDashboard clears the screen and displays the current status.
func displayDashboard(stats MonitorStats) {
	fmt.Print("\033[2J\033[H")

	elapsed := formatDuration(stats.Elapsed)
	fmt.Printf("══ RECONCILIATION MONITOR ══  Elapsed: %s  Press Ctrl+C to stop\n\n", elapsed)

	fmt.Printf("SUMMARY: Matches: %d  Anomalies: %d\n\n", stats.TotalMatches, stats.TotalAnomalies)

	fmt.Printf("POLICIES\n")
	for _, ps := range stats.Policies {
		icon := "⏳"
		if ps.Done {
			icon = "✓"
		}

		if ps.ExpectsMatches {
			fmt.Printf("  %-35s %d/%d matches  %s\n", ps.Name, ps.MatchCount, ps.ExpectedCount, icon)
		} else {
			fmt.Printf("  %-35s %d/%d anomalies %s\n", ps.Name, ps.AnomalyCount, ps.ExpectedCount, icon)
		}
	}
	fmt.Println()
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}
