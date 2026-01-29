package matching

// DefaultScoringWeights returns the default weights for final score computation.
// Keys: "timestamp", "amount", "metadata"
// Values: timestamp: 0.3, amount: 0.4, metadata: 0.3
func DefaultScoringWeights() map[string]float64 {
	return map[string]float64{
		"timestamp": 0.3,
		"amount":    0.4,
		"metadata":  0.3,
	}
}

// ComputeFinalScore computes a normalized weighted sum of partial scores.
// The formula is: sum(score_i * weight_i) / sum(weight_i)
//
// Parameters:
//   - scores: map of score names to their values (expected to be in [0, 1])
//   - weights: map of score names to their weights (if nil, uses DefaultScoringWeights)
//
// Behavior:
//   - Weights that don't sum to 1 are automatically normalized
//   - Missing scores (keys in weights but not in scores) are ignored in calculation
//   - Returns a float64 score in the range [0, 1]
func ComputeFinalScore(scores map[string]float64, weights map[string]float64) float64 {
	if weights == nil {
		weights = DefaultScoringWeights()
	}

	if len(scores) == 0 {
		return 0.0
	}

	var weightedSum float64
	var totalWeight float64

	for name, weight := range weights {
		score, exists := scores[name]
		if !exists {
			// Missing score -> ignored in calculation
			continue
		}

		weightedSum += score * weight
		totalWeight += weight
	}

	// No applicable weights (all scores were missing)
	if totalWeight == 0 {
		return 0.0
	}

	// Normalize by total weight (handles both weights that don't sum to 1 and missing scores)
	return weightedSum / totalWeight
}
