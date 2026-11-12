
# Design Notes and Engineering Trade-offs

## 1. Architecture

The solution uses a modular Python pipeline with Pandas for data processing and SQLite for persistent storage.

Main modules:

- pipeline.py: Ingestion and validation.
- storage.py: Database persistence, run history, and idempotency.
- monitor.py: SLA and freshness monitoring.
- tests/: Automated and scenario-based tests.

## 2. Design Decisions

### Python and Pandas
Selected for straightforward CSV processing, validation, and transformation.

### SQLite
Selected for lightweight local persistence without requiring external database infrastructure.

### Modular design
Separates validation, storage, and monitoring to improve maintainability and testing.

## 3. Data Quality

- Required columns are validated.
- Missing values are rejected.
- Invalid timestamps and statuses are rejected.
- Negative row counts are rejected.
- Unknown dataset references are rejected.
- Duplicate event IDs are detected.
- Rejected records retain their original input and rejection reason.

## 4. Idempotency and Recovery

- Accepted events use event_id as a primary key.
- Duplicate accepted events are ignored.
- Rejected records use SHA256 fingerprints.
- Repeated rejected records are not inserted again.
- Pipeline runs are recorded independently.
- Partial failures and retries have been tested.

## 5. Monitoring

The monitoring module detects:

- Missing dataset loads.
- Late dataset loads.
- Failed loads.
- Stale data.

Alerts are created, updated, and resolved as monitoring conditions change.

## 6. Testing Evidence

- 25 automated tests passed.
- Empty input tested.
- Malformed input tested.
- Invalid timestamps tested.
- Duplicate events tested.
- Partial failure and retry tested.
- Out-of-order events tested.
- 100,000-record processing tested.

## 7. Reconciliation Evidence

Sample input:

- Total records: 8
- Valid records: 4
- Rejected records: 4

Database verification:

- Stored accepted records: 4
- Stored rejected records: 4
- Total stored records: 8

Reconciliation result: PASSED.

## 8. Performance

A 100,000-record synthetic dataset was processed locally.

- Valid records: 95,000
- Rejected records: 5,000
- First processing run: 2.73 seconds
- Repeated run: 2.14 seconds

These measurements are specific to the local test environment.

## 9. Limitations and Future Improvements

- SQLite is intended for this local demonstration, not high-concurrency production workloads.
- Scheduling currently assumes same-day arrival and SLA times.
- Advanced timezone and overnight schedule handling is not implemented.
- Unexpected additional input columns are currently tolerated.
- A production deployment could introduce a centralized database, distributed processing, and a dashboard.
