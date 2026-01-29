<p align="center">
  <img src="https://formance01.b-cdn.net/Github-Attachements/banners/reconciliation-readme-banner.webp" alt="reconciliation" width="100%" />
</p>

# Formance Reconciliation

Formance Reconciliation compares balances between your Formance Ledger and cash pools to verify financial consistency and identify any discrepancies that need investigation. Regular reconciliation is critical for producing reports that prove the assets in your ledger are backed by actual funds, ensuring audit compliance and financial integrity.


# Documentation

- [Documentation](https://docs.formance.com/modules/reconciliation)
- [API Reference](https://docs.formance.com/api-reference/introduction)

## Elasticsearch Configuration

The reconciliation service can optionally use Elasticsearch for transaction indexing and search capabilities.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ELASTICSEARCH_URL` | Elasticsearch server URL | Required |
| `ELASTICSEARCH_USERNAME` | Authentication username | Optional |
| `ELASTICSEARCH_PASSWORD` | Authentication password | Optional |

### Index Lifecycle Management (ILM)

ILM policies automatically manage index lifecycle through hot, warm, and optional delete phases.

| Variable | Description | Default |
|----------|-------------|---------|
| `ELASTICSEARCH_ILM_ENABLED` | Enable ILM policy management | `true` |
| `ELASTICSEARCH_ILM_HOT_PHASE_DAYS` | Days to keep index in hot phase before rollover | `90` |
| `ELASTICSEARCH_ILM_WARM_PHASE_ROLLOVER_DAYS` | Days before moving to warm phase (read-only) | `365` |
| `ELASTICSEARCH_ILM_DELETE_PHASE_ENABLED` | Enable automatic index deletion | `false` |
| `ELASTICSEARCH_ILM_DELETE_PHASE_DAYS` | Days before deleting index (if delete enabled) | Not set |

#### ILM Policy: `reconciliation-transactions`

The service creates an ILM policy named `reconciliation-transactions` with the following phases:

1. **Hot Phase** (default: 90 days)
   - Active indexing and querying
   - Rolls over after configured duration

2. **Warm Phase** (default: after 365 days)
   - Index becomes read-only
   - Optimized for less frequent queries

3. **Delete Phase** (optional, disabled by default)
   - Automatically deletes old indices
   - Enable with `ELASTICSEARCH_ILM_DELETE_PHASE_ENABLED=true`