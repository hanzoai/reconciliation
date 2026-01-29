package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicy_IsTransactional(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		expected bool
	}{
		{
			name:     "empty mode returns false (backward compatibility)",
			mode:     "",
			expected: false,
		},
		{
			name:     "balance mode returns false",
			mode:     "balance",
			expected: false,
		},
		{
			name:     "transactional mode returns true",
			mode:     "transactional",
			expected: true,
		},
		{
			name:     "unknown mode returns false",
			mode:     "unknown",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Policy{Mode: tt.mode}
			assert.Equal(t, tt.expected, p.IsTransactional())
		})
	}
}

func TestScoringConfig_JSONSerialization(t *testing.T) {
	t.Run("serialize full ScoringConfig", func(t *testing.T) {
		config := &ScoringConfig{
			TimeWindowHours:        24,
			AmountTolerancePercent: 0.01,
			Weights: &ScoringWeights{
				Amount:   0.5,
				Date:     0.3,
				Metadata: 0.2,
			},
			Thresholds: &ScoringThresholds{
				AutoMatch: 0.95,
				Review:    0.75,
			},
		}

		data, err := json.Marshal(config)
		require.NoError(t, err)

		var decoded ScoringConfig
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, config.TimeWindowHours, decoded.TimeWindowHours)
		assert.Equal(t, config.AmountTolerancePercent, decoded.AmountTolerancePercent)
		require.NotNil(t, decoded.Weights)
		assert.Equal(t, config.Weights.Amount, decoded.Weights.Amount)
		assert.Equal(t, config.Weights.Date, decoded.Weights.Date)
		assert.Equal(t, config.Weights.Metadata, decoded.Weights.Metadata)
		require.NotNil(t, decoded.Thresholds)
		assert.Equal(t, config.Thresholds.AutoMatch, decoded.Thresholds.AutoMatch)
		assert.Equal(t, config.Thresholds.Review, decoded.Thresholds.Review)
	})

	t.Run("deserialize partial ScoringConfig", func(t *testing.T) {
		jsonData := `{"timeWindowHours": 48}`

		var config ScoringConfig
		err := json.Unmarshal([]byte(jsonData), &config)
		require.NoError(t, err)

		assert.Equal(t, 48, config.TimeWindowHours)
		assert.Equal(t, 0.0, config.AmountTolerancePercent)
		assert.Nil(t, config.Weights)
		assert.Nil(t, config.Thresholds)
	})

	t.Run("deserialize empty object applies defaults", func(t *testing.T) {
		jsonData := `{}`

		var config ScoringConfig
		err := json.Unmarshal([]byte(jsonData), &config)
		require.NoError(t, err)

		assert.Equal(t, 0, config.TimeWindowHours)
		assert.Equal(t, 0.0, config.AmountTolerancePercent)
		assert.Nil(t, config.Weights)
		assert.Nil(t, config.Thresholds)
	})

	t.Run("nil ScoringConfig in Policy", func(t *testing.T) {
		p := &Policy{
			Name:          "test-policy",
			Mode:          "balance",
			ScoringConfig: nil,
		}

		data, err := json.Marshal(p)
		require.NoError(t, err)

		var decoded Policy
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Nil(t, decoded.ScoringConfig)
	})
}

func TestPolicy_BackwardCompatibility(t *testing.T) {
	t.Run("policy without new fields works in balance mode", func(t *testing.T) {
		p := &Policy{
			Name: "legacy-policy",
		}

		assert.False(t, p.IsTransactional())
		assert.Equal(t, "", p.Mode)
		assert.Equal(t, "", p.Topology)
		assert.Nil(t, p.DeterministicFields)
		assert.Nil(t, p.ScoringConfig)
	})

	t.Run("policy with explicit balance mode", func(t *testing.T) {
		p := &Policy{
			Name: "balance-policy",
			Mode: "balance",
		}

		assert.False(t, p.IsTransactional())
	})

	t.Run("transactional policy with all fields", func(t *testing.T) {
		p := &Policy{
			Name:                "transactional-policy",
			Mode:                "transactional",
			Topology:            "1:N",
			DeterministicFields: []string{"external_id", "reference"},
			ScoringConfig: &ScoringConfig{
				TimeWindowHours:        72,
				AmountTolerancePercent: 0.02,
				Weights: &ScoringWeights{
					Amount:   0.6,
					Date:     0.25,
					Metadata: 0.15,
				},
				Thresholds: &ScoringThresholds{
					AutoMatch: 0.90,
					Review:    0.70,
				},
			},
		}

		assert.True(t, p.IsTransactional())
		assert.Equal(t, "transactional", p.Mode)
		assert.Equal(t, "1:N", p.Topology)
		assert.Equal(t, []string{"external_id", "reference"}, p.DeterministicFields)
		require.NotNil(t, p.ScoringConfig)
		assert.Equal(t, 72, p.ScoringConfig.TimeWindowHours)
		assert.Equal(t, 0.02, p.ScoringConfig.AmountTolerancePercent)
	})
}
