package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScoreMetadataOverlap_AllFieldsMatch(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	assert.Equal(t, 1.0, score, "all fields match should return 1.0")
}

func TestScoreMetadataOverlap_HalfFieldsMatch(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-789",
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	assert.Equal(t, 0.5, score, "1/2 fields match should return 0.5")
}

func TestScoreMetadataOverlap_NoFieldsMatch(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-999",
		"user_id":  "USR-999",
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	assert.Equal(t, 0.0, score, "0/2 fields match should return 0.0")
}

func TestScoreMetadataOverlap_FieldAbsentOnOneSide(t *testing.T) {
	// Field absent on m2 should not be counted
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
		// user_id is missing
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	// Only order_id is compared (1 field), and it matches
	assert.Equal(t, 1.0, score, "absent field should not be counted; 1/1 match = 1.0")
}

func TestScoreMetadataOverlap_FieldAbsentOnBothSides(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	// Only order_id is compared (1 field), and it matches
	assert.Equal(t, 1.0, score, "both absent fields should not be counted; 1/1 match = 1.0")
}

func TestScoreMetadataOverlap_EmptyFieldNotCounted(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	// user_id is empty on m1, so not counted; only order_id is compared
	assert.Equal(t, 1.0, score, "empty field on one side should not be counted; 1/1 match = 1.0")
}

func TestScoreMetadataOverlap_BothEmptyFieldsNotCounted(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "",
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	// Both user_id are empty, so not counted; only order_id is compared
	assert.Equal(t, 1.0, score, "both empty fields should not be counted; 1/1 match = 1.0")
}

func TestScoreMetadataOverlap_CaseInsensitiveMatch(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	m2 := map[string]interface{}{
		"order_id": "ord-123",
		"user_id":  "usr-456",
	}
	fields := []string{"order_id", "user_id"}

	// Case-sensitive should not match
	scoreCaseSensitive := ScoreMetadataOverlap(m1, m2, fields, false)
	assert.Equal(t, 0.0, scoreCaseSensitive, "case-sensitive: different case should return 0.0")

	// Case-insensitive should match
	scoreCaseInsensitive := ScoreMetadataOverlap(m1, m2, fields, true)
	assert.Equal(t, 1.0, scoreCaseInsensitive, "case-insensitive: different case should return 1.0")
}

func TestScoreMetadataOverlap_CaseInsensitivePartialMatch(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	m2 := map[string]interface{}{
		"order_id": "ord-123",
		"user_id":  "USR-999", // Different value
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, true)
	assert.Equal(t, 0.5, score, "case-insensitive: 1/2 match should return 0.5")
}

func TestScoreMetadataOverlap_EmptyFields(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	fields := []string{}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	assert.Equal(t, 0.0, score, "empty fields list should return 0.0")
}

func TestScoreMetadataOverlap_NilMetadata(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	fields := []string{"order_id", "user_id"}

	// m2 is nil
	score1 := ScoreMetadataOverlap(m1, nil, fields, false)
	assert.Equal(t, 0.0, score1, "nil m2 should return 0.0")

	// m1 is nil
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	score2 := ScoreMetadataOverlap(nil, m2, fields, false)
	assert.Equal(t, 0.0, score2, "nil m1 should return 0.0")

	// Both nil
	score3 := ScoreMetadataOverlap(nil, nil, fields, false)
	assert.Equal(t, 0.0, score3, "both nil should return 0.0")
}

func TestScoreMetadataOverlap_AllFieldsAbsent(t *testing.T) {
	m1 := map[string]interface{}{
		"other_field": "value1",
	}
	m2 := map[string]interface{}{
		"other_field": "value2",
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	assert.Equal(t, 0.0, score, "all requested fields absent should return 0.0")
}

func TestScoreMetadataOverlap_ThreeFields(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id":   "ORD-123",
		"user_id":    "USR-456",
		"product_id": "PRD-789",
	}
	m2 := map[string]interface{}{
		"order_id":   "ORD-123",
		"user_id":    "USR-456",
		"product_id": "PRD-000", // Different
	}
	fields := []string{"order_id", "user_id", "product_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	// 2/3 fields match
	assert.InDelta(t, 0.6667, score, 0.001, "2/3 fields match should return ~0.667")
}

func TestScoreMetadataOverlap_NilValueInMetadata(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  nil,
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
		"user_id":  "USR-456",
	}
	fields := []string{"order_id", "user_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	// nil is treated as empty string, so user_id not counted
	assert.Equal(t, 1.0, score, "nil value should not be counted; 1/1 match = 1.0")
}

func TestScoreMetadataOverlap_DefaultFields(t *testing.T) {
	config := DefaultMetadataOverlapConfig()
	assert.Equal(t, []string{"order_id", "user_id"}, config.Fields, "default fields should be order_id and user_id")
	assert.False(t, config.CaseInsensitive, "default case sensitivity should be false")
}

func TestScoreMetadataOverlap_SingleFieldMatch(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	fields := []string{"order_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	assert.Equal(t, 1.0, score, "single field match should return 1.0")
}

func TestScoreMetadataOverlap_SingleFieldNoMatch(t *testing.T) {
	m1 := map[string]interface{}{
		"order_id": "ORD-123",
	}
	m2 := map[string]interface{}{
		"order_id": "ORD-456",
	}
	fields := []string{"order_id"}

	score := ScoreMetadataOverlap(m1, m2, fields, false)
	assert.Equal(t, 0.0, score, "single field no match should return 0.0")
}
