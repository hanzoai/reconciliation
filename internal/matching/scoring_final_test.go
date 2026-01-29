package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeFinalScore_AllScoresOne(t *testing.T) {
	scores := map[string]float64{
		"timestamp": 1.0,
		"amount":    1.0,
		"metadata":  1.0,
	}

	result := ComputeFinalScore(scores, nil)
	assert.Equal(t, 1.0, result, "all scores 1.0 should return 1.0")
}

func TestComputeFinalScore_AllScoresZero(t *testing.T) {
	scores := map[string]float64{
		"timestamp": 0.0,
		"amount":    0.0,
		"metadata":  0.0,
	}

	result := ComputeFinalScore(scores, nil)
	assert.Equal(t, 0.0, result, "all scores 0.0 should return 0.0")
}

func TestComputeFinalScore_MixedScoresDefaultWeights(t *testing.T) {
	scores := map[string]float64{
		"timestamp": 0.8,
		"amount":    0.6,
		"metadata":  0.4,
	}

	// Default weights: timestamp: 0.3, amount: 0.4, metadata: 0.3
	// Expected: (0.8*0.3 + 0.6*0.4 + 0.4*0.3) / (0.3 + 0.4 + 0.3)
	//         = (0.24 + 0.24 + 0.12) / 1.0
	//         = 0.60
	result := ComputeFinalScore(scores, nil)
	assert.InDelta(t, 0.60, result, 0.001, "mixed scores with default weights should return correct calculation")
}

func TestComputeFinalScore_CustomWeights(t *testing.T) {
	scores := map[string]float64{
		"timestamp": 0.8,
		"amount":    0.6,
		"metadata":  0.4,
	}

	customWeights := map[string]float64{
		"timestamp": 0.5,
		"amount":    0.3,
		"metadata":  0.2,
	}

	// Expected: (0.8*0.5 + 0.6*0.3 + 0.4*0.2) / (0.5 + 0.3 + 0.2)
	//         = (0.40 + 0.18 + 0.08) / 1.0
	//         = 0.66
	result := ComputeFinalScore(scores, customWeights)
	assert.InDelta(t, 0.66, result, 0.001, "custom weights should be respected in calculation")
}

func TestComputeFinalScore_WeightsDontSumToOne_AutoNormalization(t *testing.T) {
	scores := map[string]float64{
		"timestamp": 1.0,
		"amount":    0.5,
		"metadata":  0.0,
	}

	// Weights that don't sum to 1
	weightsNotNormalized := map[string]float64{
		"timestamp": 0.6,
		"amount":    0.8,
		"metadata":  0.6,
	}

	// Expected: (1.0*0.6 + 0.5*0.8 + 0.0*0.6) / (0.6 + 0.8 + 0.6)
	//         = (0.6 + 0.4 + 0.0) / 2.0
	//         = 1.0 / 2.0
	//         = 0.5
	result := ComputeFinalScore(scores, weightsNotNormalized)
	assert.InDelta(t, 0.5, result, 0.001, "weights that don't sum to 1 should be automatically normalized")
}

func TestComputeFinalScore_MissingScore_IgnoredInCalculation(t *testing.T) {
	// Only provide timestamp and amount, missing metadata
	scores := map[string]float64{
		"timestamp": 1.0,
		"amount":    1.0,
	}

	// Default weights: timestamp: 0.3, amount: 0.4, metadata: 0.3
	// Since metadata is missing, it should be ignored
	// Expected: (1.0*0.3 + 1.0*0.4) / (0.3 + 0.4)
	//         = 0.7 / 0.7
	//         = 1.0
	result := ComputeFinalScore(scores, nil)
	assert.Equal(t, 1.0, result, "missing score should be ignored in calculation")
}

func TestComputeFinalScore_EmptyScores(t *testing.T) {
	scores := map[string]float64{}

	result := ComputeFinalScore(scores, nil)
	assert.Equal(t, 0.0, result, "empty scores should return 0.0")
}

func TestComputeFinalScore_NilScores(t *testing.T) {
	result := ComputeFinalScore(nil, nil)
	assert.Equal(t, 0.0, result, "nil scores should return 0.0")
}

func TestComputeFinalScore_AllScoresMissing(t *testing.T) {
	// Scores with keys that don't match any weight key
	scores := map[string]float64{
		"unknown": 1.0,
		"other":   0.5,
	}

	// Default weights expect: timestamp, amount, metadata
	result := ComputeFinalScore(scores, nil)
	assert.Equal(t, 0.0, result, "all scores missing from weights should return 0.0")
}

func TestComputeFinalScore_PartialScoresMissing(t *testing.T) {
	// Only timestamp provided
	scores := map[string]float64{
		"timestamp": 0.8,
	}

	// Default weights: timestamp: 0.3, amount: 0.4, metadata: 0.3
	// Only timestamp is present, so only its weight is used
	// Expected: (0.8*0.3) / 0.3 = 0.8
	result := ComputeFinalScore(scores, nil)
	assert.Equal(t, 0.8, result, "single score should return that score (normalized)")
}

func TestComputeFinalScore_ResultBetweenZeroAndOne(t *testing.T) {
	testCases := []struct {
		name    string
		scores  map[string]float64
		weights map[string]float64
	}{
		{
			name: "normal case",
			scores: map[string]float64{
				"timestamp": 0.5,
				"amount":    0.7,
				"metadata":  0.3,
			},
			weights: nil,
		},
		{
			name: "extreme weights",
			scores: map[string]float64{
				"timestamp": 0.2,
				"amount":    0.9,
				"metadata":  0.1,
			},
			weights: map[string]float64{
				"timestamp": 10.0,
				"amount":    0.1,
				"metadata":  5.0,
			},
		},
		{
			name: "single score",
			scores: map[string]float64{
				"amount": 0.75,
			},
			weights: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ComputeFinalScore(tc.scores, tc.weights)
			assert.GreaterOrEqual(t, result, 0.0, "result should be >= 0")
			assert.LessOrEqual(t, result, 1.0, "result should be <= 1")
		})
	}
}

func TestDefaultScoringWeights(t *testing.T) {
	weights := DefaultScoringWeights()

	assert.Equal(t, 0.3, weights["timestamp"], "timestamp weight should be 0.3")
	assert.Equal(t, 0.4, weights["amount"], "amount weight should be 0.4")
	assert.Equal(t, 0.3, weights["metadata"], "metadata weight should be 0.3")

	// Verify weights sum to 1.0
	total := weights["timestamp"] + weights["amount"] + weights["metadata"]
	assert.Equal(t, 1.0, total, "default weights should sum to 1.0")
}
