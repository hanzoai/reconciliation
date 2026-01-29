package matching

import (
	"strings"
)

// MetadataOverlapConfig holds configuration for metadata overlap scoring.
type MetadataOverlapConfig struct {
	// Fields is the list of metadata fields to compare.
	// If empty, defaults to ["order_id", "user_id"].
	Fields []string

	// CaseInsensitive enables case-insensitive comparison of field values.
	// Default is false (case-sensitive).
	CaseInsensitive bool
}

// DefaultMetadataOverlapConfig returns the default configuration for metadata overlap scoring.
func DefaultMetadataOverlapConfig() MetadataOverlapConfig {
	return MetadataOverlapConfig{
		Fields:          []string{"order_id", "user_id"},
		CaseInsensitive: false,
	}
}

// ScoreMetadataOverlap calculates a score based on common metadata fields.
// The score is the ratio of matched fields to compared fields.
//
// Scoring rules:
// - Score is 1.0 when all compared fields match
// - Score is 0.5 when half of the compared fields match
// - Score is 0.0 when no fields match
// - Fields absent on either side are not counted in the comparison
// - Empty string values are not counted in the comparison
//
// Parameters:
//   - m1, m2: The metadata maps to compare
//   - fields: The list of fields to compare
//   - caseInsensitive: If true, performs case-insensitive comparison
//
// Returns a float64 score in the range [0, 1]
func ScoreMetadataOverlap(m1, m2 map[string]interface{}, fields []string, caseInsensitive bool) float64 {
	if len(fields) == 0 {
		return 0.0
	}

	if m1 == nil || m2 == nil {
		return 0.0
	}

	matchedCount := 0
	comparedCount := 0

	for _, field := range fields {
		val1, ok1 := m1[field]
		val2, ok2 := m2[field]

		// Skip if field is absent on either side
		if !ok1 || !ok2 {
			continue
		}

		// Convert to strings for comparison
		str1 := toString(val1)
		str2 := toString(val2)

		// Skip empty values
		if str1 == "" || str2 == "" {
			continue
		}

		comparedCount++

		// Compare values
		if caseInsensitive {
			if strings.EqualFold(str1, str2) {
				matchedCount++
			}
		} else {
			if str1 == str2 {
				matchedCount++
			}
		}
	}

	// No fields were compared (all absent or empty)
	if comparedCount == 0 {
		return 0.0
	}

	return float64(matchedCount) / float64(comparedCount)
}

// toString converts an interface{} value to its string representation.
func toString(val interface{}) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v
	case *string:
		if v == nil {
			return ""
		}
		return *v
	default:
		// For other types, use fmt.Sprintf but avoid importing fmt
		// since we're just checking for simple types
		return ""
	}
}
