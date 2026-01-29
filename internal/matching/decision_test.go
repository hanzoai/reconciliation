package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecideFromScore(t *testing.T) {
	t.Parallel()

	defaultThresholds := DefaultThresholds()

	tests := []struct {
		name       string
		score      float64
		thresholds Thresholds
		expected   Decision
	}{
		{
			name:       "score 0.90 with default thresholds -> MATCHED",
			score:      0.90,
			thresholds: defaultThresholds,
			expected:   DecisionMatched,
		},
		{
			name:       "score 0.85 (exactly) -> MATCHED",
			score:      0.85,
			thresholds: defaultThresholds,
			expected:   DecisionMatched,
		},
		{
			name:       "score 0.84 -> REQUIRES_REVIEW",
			score:      0.84,
			thresholds: defaultThresholds,
			expected:   DecisionRequiresReview,
		},
		{
			name:       "score 0.60 (exactly) -> REQUIRES_REVIEW",
			score:      0.60,
			thresholds: defaultThresholds,
			expected:   DecisionRequiresReview,
		},
		{
			name:       "score 0.59 -> UNMATCHED",
			score:      0.59,
			thresholds: defaultThresholds,
			expected:   DecisionUnmatched,
		},
		{
			name:       "score 1.0 -> MATCHED",
			score:      1.0,
			thresholds: defaultThresholds,
			expected:   DecisionMatched,
		},
		{
			name:       "score 0.0 -> UNMATCHED",
			score:      0.0,
			thresholds: defaultThresholds,
			expected:   DecisionUnmatched,
		},
		{
			name:  "custom thresholds - score above auto_match",
			score: 0.95,
			thresholds: Thresholds{
				AutoMatch: 0.90,
				Review:    0.70,
			},
			expected: DecisionMatched,
		},
		{
			name:  "custom thresholds - score exactly at auto_match",
			score: 0.90,
			thresholds: Thresholds{
				AutoMatch: 0.90,
				Review:    0.70,
			},
			expected: DecisionMatched,
		},
		{
			name:  "custom thresholds - score in review range",
			score: 0.80,
			thresholds: Thresholds{
				AutoMatch: 0.90,
				Review:    0.70,
			},
			expected: DecisionRequiresReview,
		},
		{
			name:  "custom thresholds - score exactly at review",
			score: 0.70,
			thresholds: Thresholds{
				AutoMatch: 0.90,
				Review:    0.70,
			},
			expected: DecisionRequiresReview,
		},
		{
			name:  "custom thresholds - score below review",
			score: 0.69,
			thresholds: Thresholds{
				AutoMatch: 0.90,
				Review:    0.70,
			},
			expected: DecisionUnmatched,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := DecideFromScore(tt.score, tt.thresholds)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDefaultThresholds(t *testing.T) {
	t.Parallel()

	thresholds := DefaultThresholds()

	assert.Equal(t, 0.85, thresholds.AutoMatch)
	assert.Equal(t, 0.60, thresholds.Review)
}
