package v1

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/reconciliation/internal/api/backend"
)

type reportResponse struct {
	ID                string           `json:"id"`
	PolicyID          string           `json:"policyID"`
	PeriodStart       time.Time        `json:"periodStart"`
	PeriodEnd         time.Time        `json:"periodEnd"`
	TotalTransactions int64            `json:"totalTransactions"`
	MatchedCount      int64            `json:"matchedCount"`
	MatchRate         float64          `json:"matchRate"`
	AnomaliesByType   map[string]int64 `json:"anomaliesByType"`
	GeneratedAt       time.Time        `json:"generatedAt"`
}

func GetLatestPolicyReportHandler(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		policyID := chi.URLParam(r, "policyID")

		report, err := b.GetService().GetLatestPolicyReport(r.Context(), policyID)
		if err != nil {
			handleServiceErrors(w, r, err)
			return
		}

		// Check Accept header for CSV format
		accept := r.Header.Get("Accept")
		if accept == "text/csv" {
			renderReportCSV(w, report.ID.String(), report.PolicyID.String(), report.PeriodStart, report.PeriodEnd,
				report.TotalTransactions, report.MatchedCount, report.MatchRate, report.AnomaliesByType, report.GeneratedAt)
			return
		}

		// Default to JSON response
		data := &reportResponse{
			ID:                report.ID.String(),
			PolicyID:          report.PolicyID.String(),
			PeriodStart:       report.PeriodStart,
			PeriodEnd:         report.PeriodEnd,
			TotalTransactions: report.TotalTransactions,
			MatchedCount:      report.MatchedCount,
			MatchRate:         report.MatchRate,
			AnomaliesByType:   report.AnomaliesByType,
			GeneratedAt:       report.GeneratedAt,
		}

		api.Ok(w, data)
	}
}

func renderReportCSV(w http.ResponseWriter, id, policyID string, periodStart, periodEnd time.Time,
	totalTransactions, matchedCount int64, matchRate float64, anomaliesByType map[string]int64, generatedAt time.Time) {

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=report_%s.csv", id))

	writer := csv.NewWriter(w)
	defer writer.Flush()

	// Write header row
	_ = writer.Write([]string{"field", "value"})

	// Write data rows
	_ = writer.Write([]string{"id", id})
	_ = writer.Write([]string{"policyID", policyID})
	_ = writer.Write([]string{"periodStart", periodStart.Format(time.RFC3339)})
	_ = writer.Write([]string{"periodEnd", periodEnd.Format(time.RFC3339)})
	_ = writer.Write([]string{"totalTransactions", fmt.Sprintf("%d", totalTransactions)})
	_ = writer.Write([]string{"matchedCount", fmt.Sprintf("%d", matchedCount)})
	_ = writer.Write([]string{"matchRate", fmt.Sprintf("%.6f", matchRate)})
	_ = writer.Write([]string{"generatedAt", generatedAt.Format(time.RFC3339)})

	// Write anomalies by type in sorted order for consistent output
	if len(anomaliesByType) > 0 {
		types := make([]string, 0, len(anomaliesByType))
		for t := range anomaliesByType {
			types = append(types, t)
		}
		sort.Strings(types)

		for _, t := range types {
			_ = writer.Write([]string{fmt.Sprintf("anomalies_%s", t), fmt.Sprintf("%d", anomaliesByType[t])})
		}
	}
}
