package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnomalyType_IsValid(t *testing.T) {
	tests := []struct {
		anomalyType AnomalyType
		expected    bool
	}{
		{AnomalyTypeMissingOnPayments, true},
		{AnomalyTypeMissingOnLedger, true},
		{AnomalyTypeDuplicateLedger, true},
		{AnomalyTypeAmountMismatch, true},
		{AnomalyTypeCurrencyMismatch, true},
		{"INVALID", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(string(tt.anomalyType), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.anomalyType.IsValid())
		})
	}
}

func TestAnomalyType_String(t *testing.T) {
	assert.Equal(t, "MISSING_ON_PAYMENTS", AnomalyTypeMissingOnPayments.String())
	assert.Equal(t, "MISSING_ON_LEDGER", AnomalyTypeMissingOnLedger.String())
	assert.Equal(t, "DUPLICATE_LEDGER", AnomalyTypeDuplicateLedger.String())
	assert.Equal(t, "AMOUNT_MISMATCH", AnomalyTypeAmountMismatch.String())
	assert.Equal(t, "CURRENCY_MISMATCH", AnomalyTypeCurrencyMismatch.String())
}

func TestSeverity_IsValid(t *testing.T) {
	tests := []struct {
		severity Severity
		expected bool
	}{
		{SeverityCritical, true},
		{SeverityHigh, true},
		{SeverityMedium, true},
		{SeverityLow, true},
		{"INVALID", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(string(tt.severity), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.severity.IsValid())
		})
	}
}

func TestSeverity_String(t *testing.T) {
	assert.Equal(t, "CRITICAL", SeverityCritical.String())
	assert.Equal(t, "HIGH", SeverityHigh.String())
	assert.Equal(t, "MEDIUM", SeverityMedium.String())
	assert.Equal(t, "LOW", SeverityLow.String())
}
