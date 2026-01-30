package matching

import "github.com/formancehq/reconciliation/internal/models"

func expectedProviderForPolicySide(policy *models.Policy, side models.TransactionSide) (string, bool) {
	if policy == nil {
		return "", false
	}

	switch side {
	case models.TransactionSideLedger:
		if policy.LedgerName != "" {
			return policy.LedgerName, true
		}
	case models.TransactionSidePayments:
		provider := policy.PaymentsProvider
		if policy.ConnectorType != nil && *policy.ConnectorType != "" {
			provider = *policy.ConnectorType
		}
		if provider != "" {
			return provider, true
		}
	}

	return "", false
}
