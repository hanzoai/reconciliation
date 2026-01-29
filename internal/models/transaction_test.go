package models

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func ptrUUID(u uuid.UUID) *uuid.UUID {
	return &u
}

func validTransaction() Transaction {
	return Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(uuid.New()),
		Side:       TransactionSideLedger,
		Provider:   "stripe",
		ExternalID: "txn_123",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}
}

func TestTransaction_Validate_Success(t *testing.T) {
	tx := validTransaction()
	err := tx.Validate()
	assert.NoError(t, err)
}

func TestTransaction_Validate_NilPolicyID(t *testing.T) {
	// PolicyID is now optional, so nil should be valid
	tx := validTransaction()
	tx.PolicyID = nil
	err := tx.Validate()
	assert.NoError(t, err, "nil PolicyID should be valid")
}

func TestTransaction_Validate_MissingSide(t *testing.T) {
	tx := validTransaction()
	tx.Side = ""
	err := tx.Validate()
	assert.EqualError(t, err, "side is required")
}

func TestTransaction_Validate_InvalidSide(t *testing.T) {
	tx := validTransaction()
	tx.Side = "INVALID"
	err := tx.Validate()
	assert.EqualError(t, err, "side must be LEDGER or PAYMENTS")
}

func TestTransaction_Validate_MissingProvider(t *testing.T) {
	tx := validTransaction()
	tx.Provider = ""
	err := tx.Validate()
	assert.EqualError(t, err, "provider is required")
}

func TestTransaction_Validate_MissingExternalID(t *testing.T) {
	tx := validTransaction()
	tx.ExternalID = ""
	err := tx.Validate()
	assert.EqualError(t, err, "externalID is required")
}

func TestTransaction_Validate_MissingCurrency(t *testing.T) {
	tx := validTransaction()
	tx.Currency = ""
	err := tx.Validate()
	assert.EqualError(t, err, "currency is required")
}

func TestTransaction_Validate_MissingOccurredAt(t *testing.T) {
	tx := validTransaction()
	tx.OccurredAt = time.Time{}
	err := tx.Validate()
	assert.EqualError(t, err, "occurredAt is required")
}

func TestTransactionSide_IsValid(t *testing.T) {
	tests := []struct {
		side     TransactionSide
		expected bool
	}{
		{TransactionSideLedger, true},
		{TransactionSidePayments, true},
		{"INVALID", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(string(tt.side), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.side.IsValid())
		})
	}
}

func TestTransactionSide_String(t *testing.T) {
	assert.Equal(t, "LEDGER", TransactionSideLedger.String())
	assert.Equal(t, "PAYMENTS", TransactionSidePayments.String())
}
