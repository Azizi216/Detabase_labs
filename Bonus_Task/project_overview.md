# Banking Transaction System - Documentation
**Author:** Azizi Hashmatullah

## Project Overview
A comprehensive banking transaction system for KazFinance Bank that handles multi-currency accounts (KZT, USD, EUR, RUB), supports instant transfers between customers, enforces daily transaction limits, maintains complete audit trails, and provides real-time reporting for regulatory compliance.

## Database Schema Design
### Core Tables
- **customers** – Stores customer information with TIN (12‑digit unique identifier)
- **accounts** – Multi-currency accounts with IBAN-format account numbers
- **transactions** – Records all financial transactions with currency conversion details
- **exchange_rates** – Historical exchange rates with validity periods
- **audit_log** – Comprehensive audit trail using JSONB for flexibility

## Key Design Decisions
### 1. Currency Handling Strategy
**Problem:** Need to handle transactions between different currencies while maintaining accurate balance calculations and daily limits.

**Solution:**
- All daily limits calculated in KZT for consistency
- Exchange rates stored with validity periods for historical accuracy
- Indirect currency conversion through KZT when direct rates unavailable
- Transfer currency must match sender account currency (business requirement)

**Rationale:** Ensures consistent limit enforcement regardless of currency fluctuations and prevents currency mismatch errors.

### 2. Transaction Isolation and ACID Compliance
**Problem:** Concurrent transactions could lead to race conditions and inconsistent balances.

**Solution:**
- `SELECT ... FOR UPDATE` for row‑level locking
- `SAVEPOINT` for partial rollback scenarios
- Advisory locks (`pg_try_advisory_xact_lock`) for batch processing
- Comprehensive error handling with custom SQLSTATE codes

**Rationale:** Guarantees data integrity under high concurrency.

### 3. Indexing Strategy
**Problem:** Optimize performance for frequent queries while minimizing storage overhead.

**Solution:** Mixed index types based on query patterns:

| Index Type | Purpose | Justification |
|-----------|---------|---------------|
| B-tree (covering) | Account lookups | Frequent WHERE on `account_number` |
| Partial B-tree | Active accounts | 90% of queries only need active accounts |
| Expression index | Email searches | Case-insensitive searches |
| GIN index | JSONB queries | Efficient audit log analysis |
| Hash index | Currency pairs | Fast exact match lookups |
| BRIN index | Time-series data | Efficient for large audit logs |
| Composite index | Date-range queries | Common reporting patterns |

### 4. Audit Trail Design
**Problem:** Need comprehensive, queryable audit trail without schema changes.

**Solution:**
- JSONB for flexible old/new value storage
- Generic audit structure for all events
- Automatic triggers
- IP address logging

### 5. Batch Processing Architecture
**Problem:** Handle large salary batch payments atomically with partial failure support.

**Solution:**
- JSONB input for flexible batch structure
- Individual `SAVEPOINT` for each payment
- Advisory locks for exclusivity
- Daily limit bypass for salary transfers
- Detailed success/failure reporting

## Performance Optimizations
### Critical Query Patterns Optimized
- Account lookups: covering index `(account_number, customer_id, is_active)`
- Daily limit checks: composite index `(from_account_id, DATE(created_at), status)`
- Reports: composite `(created_at, status, type)`
- Currency conversions: hash index on currency pairs
- Audit analysis: BRIN on timestamps

### Concurrency Handling
- Row-level locking prevents double spending
- Advisory locks prevent duplicate batch execution
- SAVEPOINT-based partial batch recovery
- All operations inside strict transaction boundaries

## Security Considerations
- SECURITY BARRIER views for sensitive reporting
- Parameterized procedures prevent SQL injection
- Full audit trails for forensics
- Input validation on all parameters

## Testing Strategy
### Automated Tests
- Successful transfers
- All error conditions
- Currency mismatch scenarios
- Batch success / partial / failed cases
- High-concurrency simulation

### Performance Testing
- `EXPLAIN ANALYZE` on critical queries
- Index usage statistics
- Concurrency benchmarks

## Extension Points
- Add new currencies via CHECK constraint update
- Add new transaction types via extension of type CHECK
- Create new reporting views using existing schema
- Add new audit fields via JSONB
- Scales well with large datasets thanks to BRIN and composite indexes

## Compliance Features
- Real-time daily limit tracking
- Suspicious activity detection
- Full audit history
- Real-time & historical reports
- Exchange rate auditability

## Deployment Considerations
- Regular VACUUM/REINDEX for audit_log
- Frequent exchange rate updates
- Off-peak batch processing
- Monitoring limit utilization & suspicious activity
- Backup strategy prioritizing transaction logs

---


