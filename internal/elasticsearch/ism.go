package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// TransactionISMPolicyName returns the ISM policy name for a given stack.
// Example: mystack-reconciliation
func TransactionISMPolicyName(stack string) string {
	return fmt.Sprintf("%s-%s", stack, TransactionIndexSuffix)
}

// ISMConfig holds the configuration for Index State Management (OpenSearch equivalent of ILM).
type ISMConfig struct {
	// Enabled determines if ISM is enabled.
	Enabled bool
	// HotPhaseDays is the number of days to keep indices in the hot phase.
	// After this period, indices will transition to warm.
	// Default: 90 days.
	HotPhaseDays int
	// WarmPhaseRolloverDays is the age in days when indices move from hot to warm.
	// Default: 365 days (1 year).
	WarmPhaseRolloverDays int
	// DeletePhaseEnabled determines if indices should be deleted after the warm phase.
	DeletePhaseEnabled bool
	// DeletePhaseDays is the age in days when indices are deleted (if DeletePhaseEnabled is true).
	// This is measured from index creation.
	DeletePhaseDays int
}

// DefaultISMConfig returns the default ISM configuration.
func DefaultISMConfig() ISMConfig {
	return ISMConfig{
		Enabled:               true,
		HotPhaseDays:          90,
		WarmPhaseRolloverDays: 365,
		DeletePhaseEnabled:    false,
		DeletePhaseDays:       0,
	}
}

// ismPolicyBody builds the ISM policy body for OpenSearch.
// OpenSearch ISM uses a state machine model instead of Elasticsearch's phase model.
func ismPolicyBody(stack string, config ISMConfig) map[string]interface{} {
	states := []map[string]interface{}{
		{
			"name":    "hot",
			"actions": []map[string]interface{}{},
			"transitions": []map[string]interface{}{
				{
					"state_name": "warm",
					"conditions": map[string]interface{}{
						"min_index_age": fmt.Sprintf("%dd", config.WarmPhaseRolloverDays),
					},
				},
			},
		},
		{
			"name": "warm",
			"actions": []map[string]interface{}{
				{"read_only": map[string]interface{}{}},
			},
			"transitions": []map[string]interface{}{},
		},
	}

	// Add delete state and transition if enabled
	if config.DeletePhaseEnabled && config.DeletePhaseDays > 0 {
		// Update warm state to transition to delete
		states[1]["transitions"] = []map[string]interface{}{
			{
				"state_name": "delete",
				"conditions": map[string]interface{}{
					"min_index_age": fmt.Sprintf("%dd", config.DeletePhaseDays),
				},
			},
		}

		// Add delete state
		states = append(states, map[string]interface{}{
			"name": "delete",
			"actions": []map[string]interface{}{
				{"delete": map[string]interface{}{}},
			},
			"transitions": []map[string]interface{}{},
		})
	}

	return map[string]interface{}{
		"policy": map[string]interface{}{
			"description":   "Reconciliation transactions index lifecycle policy",
			"default_state": "hot",
			"states":        states,
			"ism_template": []map[string]interface{}{
				{
					// Match pattern: *-reconciliation-* (e.g., mystack-reconciliation-2026-01)
					"index_patterns": []string{fmt.Sprintf("%s-%s-*", stack, TransactionIndexSuffix)},
					"priority":       100,
				},
			},
		},
	}
}

// CreateISMPolicy creates the ISM policy for transaction indices.
func (c *Client) CreateISMPolicy(ctx context.Context, stack string, config ISMConfig) error {
	body := ismPolicyBody(stack, config)
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal ISM policy body: %w", err)
	}

	// Use the low-level transport to call the ISM API
	url := fmt.Sprintf("_plugins/_ism/policies/%s", TransactionISMPolicyName(stack))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create ISM policy request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := c.client.Client.Perform(req)
	if err != nil {
		return fmt.Errorf("failed to create ISM policy: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode >= 400 {
		bodyContent, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to create ISM policy: %d - %s", res.StatusCode, string(bodyContent))
	}

	return nil
}

// ISMPolicyExists checks if the ISM policy exists.
func (c *Client) ISMPolicyExists(ctx context.Context, stack string) (bool, error) {
	url := fmt.Sprintf("_plugins/_ism/policies/%s", TransactionISMPolicyName(stack))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create ISM policy exists request: %w", err)
	}

	res, err := c.client.Client.Perform(req)
	if err != nil {
		// Check if error indicates policy not found
		if strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if ISM policy exists: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode == 404 {
		return false, nil
	}

	if res.StatusCode >= 400 {
		bodyContent, _ := io.ReadAll(res.Body)
		return false, fmt.Errorf("failed to check if ISM policy exists: %d - %s", res.StatusCode, string(bodyContent))
	}

	return true, nil
}

// EnsureISMPolicy ensures the ISM policy exists with the given configuration.
// If it doesn't exist, it creates it.
func (c *Client) EnsureISMPolicy(ctx context.Context, stack string, config ISMConfig) error {
	if !config.Enabled {
		return nil
	}

	exists, err := c.ISMPolicyExists(ctx, stack)
	if err != nil {
		return err
	}

	if !exists {
		return c.CreateISMPolicy(ctx, stack, config)
	}

	return nil
}

// Aliases for backward compatibility with existing code that uses ILM naming

// ILMConfig is an alias for ISMConfig for backward compatibility.
type ILMConfig = ISMConfig

// DefaultILMConfig returns the default ISM configuration (alias for backward compatibility).
func DefaultILMConfig() ILMConfig {
	return DefaultISMConfig()
}

// TransactionILMPolicyName is an alias for TransactionISMPolicyName for backward compatibility.
func TransactionILMPolicyName(stack string) string {
	return TransactionISMPolicyName(stack)
}

// EnsureILMPolicy is an alias for EnsureISMPolicy for backward compatibility.
func (c *Client) EnsureILMPolicy(ctx context.Context, stack string, config ILMConfig) error {
	return c.EnsureISMPolicy(ctx, stack, config)
}
