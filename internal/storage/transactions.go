package storage

// NOTE: Transaction storage has been migrated to OpenSearch.
// The PostgreSQL transactions table is now deprecated and will be dropped.
// See internal/storage/transaction_store.go for the new TransactionStore interface.
// See internal/storage/opensearch_transaction_store.go for the OpenSearch implementation.

// This file is kept temporarily for the migration that will drop the transactions table.
// The TransactionRepository interface and related code have been removed.
