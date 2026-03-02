# Hanzo Reconciliation

Automated transaction matching engine. Compares ledger balances against payment provider data to verify financial consistency and identify discrepancies.

## Architecture

```
hanzo/commerce    Storefront, catalog, orders
       |
hanzo/payments    Payment routing (50+ processors)
       |
hanzo/treasury    Ledger, reconciliation, wallets   <-- includes this
       |
lux/treasury      On-chain treasury, MPC/KMS wallets
```

## Features

- **Automated Matching** — Rule-based matching of ledger transactions against external data
- **Policy Engine** — Define reconciliation policies per payment provider
- **Discrepancy Detection** — Identify mismatches in amounts, timing, or status
- **Multi-Provider** — Reconcile across Stripe, Adyen, bank feeds, and 50+ connectors
- **Scheduled Runs** — Automated periodic reconciliation with alerting
- **Audit Reports** — Generate compliance-ready reconciliation reports

## Quick Start

```bash
# Start with Docker
docker compose up -d

# Create a reconciliation policy
curl -X POST http://localhost:8080/v2/reconciliation/policies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "stripe-daily",
    "ledgerName": "default",
    "paymentsPoolID": "stripe-pool",
    "schedule": "0 2 * * *"
  }'

# Trigger reconciliation
curl -X POST http://localhost:8080/v2/reconciliation/policies/{id}/reconcile
```

## API

- `POST /v2/reconciliation/policies` — Create reconciliation policy
- `GET /v2/reconciliation/policies` — List policies
- `POST /v2/reconciliation/policies/{id}/reconcile` — Run reconciliation
- `GET /v2/reconciliation/results` — Get reconciliation results

## Development

```bash
go build ./...
go test ./...
```

## License

MIT — see [LICENSE](LICENSE)

