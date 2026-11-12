
# Database Data Dictionary

## 1. Overview

The project uses SQLite to store dataset load events, pipeline execution history, rejected records, and monitoring alerts.

Database file: `data/monitoring.db`

## 2. Database Tables

### 2.1 load_events

Stores successfully validated and accepted dataset load events.

| Column | Data Type | Description |
|---|---|---|
| event_id | TEXT | Unique identifier for a load event (Primary Key). |
| dataset_id | TEXT | Identifier of the dataset. |
| source_file | TEXT | Name of the source file. |
| load_completed_at | TEXT | Timestamp when the load completed, stored in ISO format. |
| status | TEXT | Load status: SUCCESS or FAILED. |
| row_count | INTEGER | Number of records in the dataset. |
| run_id | TEXT | Identifier of the pipeline execution that processed the event. |

### 2.2 pipeline_runs

Stores execution history and summary counts for each pipeline run.

| Column | Data Type | Description |
|---|---|---|
| run_id | TEXT | Unique identifier for a pipeline execution (Primary Key). |
| started_at | TEXT | Timestamp when the pipeline execution started. |
| total_records | INTEGER | Total number of input records processed. |
| accepted_records | INTEGER | Number of newly accepted records. |
| rejected_records | INTEGER | Number of newly stored rejected records. |

### 2.3 rejected_records

Stores records that failed validation, together with their rejection reasons.

| Column | Data Type | Description |
|---|---|---|
| reject_id | INTEGER | Auto-generated rejection identifier (Primary Key). |
| run_id | TEXT | Pipeline execution that processed the rejected record. |
| event_id | TEXT | Event identifier from the rejected input, when available. |
| reject_reason | TEXT | Explanation of why the record was rejected. |
| record_json | TEXT | Original rejected record stored as JSON. |
| record_hash | TEXT | SHA256 fingerprint used to identify duplicate rejected records. |

### 2.4 monitoring_alerts

Stores dataset SLA and freshness monitoring alerts.

| Column | Data Type | Description |
|---|---|---|
| alert_id | INTEGER | Auto-generated alert identifier (Primary Key). |
| dataset_id | TEXT | Dataset associated with the alert. |
| alert_type | TEXT | Type of alert, such as SLA or freshness. |
| message | TEXT | Actionable alert description. |
| first_seen_at | TEXT | Timestamp when the alert was first detected. |
| last_seen_at | TEXT | Most recent timestamp when the alert was detected. |
| resolved_at | TEXT | Timestamp when the alert was resolved. NULL indicates an active alert. |

## 3. Data Integrity and Idempotency

- `event_id` uniquely identifies accepted load events.
- Duplicate event IDs are not inserted again.
- Rejected records use SHA256 hashes to prevent duplicate rejection entries.
- Pipeline execution IDs help associate records with individual runs.
- Active monitoring alerts are identified using a NULL `resolved_at` value.
- Previously active alerts can be updated or resolved during subsequent monitoring runs.

## 4. Data Flow

1. Incoming records are read from CSV files.
2. Records are validated against required fields and business rules.
3. Valid records are stored in `load_events`.
4. Invalid records and rejection reasons are stored in `rejected_records`.
5. Each execution is recorded in `pipeline_runs`.
6. The monitoring module evaluates SLA and freshness conditions.
7. Alerts are created, updated, or resolved in `monitoring_alerts`.

## 5. Database Technology

- Database: SQLite
- Data format: Relational tables
- Timestamp representation: ISO-formatted TEXT
- Rejection fingerprint: SHA256
