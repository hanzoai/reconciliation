package service

import (
	"testing"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/stretchr/testify/require"
)

func TestCreatePolicyRequestValidateAssertionMode(t *testing.T) {
	t.Parallel()

	base := CreatePolicyRequest{
		Name:           "test",
		LedgerName:     "default",
		LedgerQuery:    map[string]interface{}{},
		PaymentsPoolID: "00000000-0000-0000-0000-000000000000",
	}

	t.Run("default mode", func(t *testing.T) {
		req := base
		err := req.Validate()
		require.NoError(t, err)
		require.Equal(t, models.AssertionModeCoverage, req.AssertionMode)
	})

	t.Run("invalid mode", func(t *testing.T) {
		req := base
		req.AssertionMode = models.AssertionMode("INVALID")
		err := req.Validate()
		require.Error(t, err)
	})

	t.Run("min buffer without config", func(t *testing.T) {
		req := base
		req.AssertionMode = models.AssertionModeMinBuffer
		err := req.Validate()
		require.Error(t, err)
	})

	t.Run("min buffer with config", func(t *testing.T) {
		req := base
		req.AssertionMode = models.AssertionModeMinBuffer
		req.AssertionConfig = map[string]interface{}{
			"assets": map[string]interface{}{
				"*": map[string]interface{}{
					"bps": 100,
				},
			},
		}
		err := req.Validate()
		require.NoError(t, err)
	})

	t.Run("min buffer invalid star absolute", func(t *testing.T) {
		req := base
		req.AssertionMode = models.AssertionModeMinBuffer
		req.AssertionConfig = map[string]interface{}{
			"assets": map[string]interface{}{
				"*": map[string]interface{}{
					"absolute": 10,
				},
			},
		}
		err := req.Validate()
		require.Error(t, err)
	})

	t.Run("min buffer invalid both fields", func(t *testing.T) {
		req := base
		req.AssertionMode = models.AssertionModeMinBuffer
		req.AssertionConfig = map[string]interface{}{
			"assets": map[string]interface{}{
				"USD/2": map[string]interface{}{
					"bps":      10,
					"absolute": 100,
				},
			},
		}
		err := req.Validate()
		require.Error(t, err)
	})
}
