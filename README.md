

# Dataset SLA and Freshness Monitoring System

## 1. Project Overview

This project is a Data Engineering solution that monitors dataset ingestion, completion SLAs, data freshness, and data quality.

It helps data engineers identify missing or late dataset loads, detect stale data, validate incoming records, and generate actionable monitoring alerts.

The solution uses Python, Pandas, and SQLite.

## 2. Business Objectives

- Track expected arrival times for multiple datasets.
- Monitor dataset completion against SLA deadlines.
- Detect missing, late, and failed loads.
- Calculate data freshness.
- Validate incoming records.
- Reject invalid records with reasons.
- Prevent duplicate event ingestion.
- Support safe pipeline reruns.
- Maintain pipeline run history and monitoring alerts.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.10+ | Core application logic |
| Pandas | Data ingestion and validation |
| SQLite | Persistent storage |
| Pytest | Automated testing |
| Streamlit | Optional dashboard framework |
| Plotly | Optional visualizations |

## 4. Project Architecture

The application follows this workflow:

```text
Source CSV Files
       |
       v
Data Ingestion
       |
       v
Schema and Business Validation
       |
       +------ Invalid Records
       |             |
       |             v
       |       Rejected Records
       |
       v
Valid Records
       |
       v
SQLite Storage
       |
       v
SLA and Freshness Monitoring
       |
       v
Alert Generation and Pipeline Reports
```

## 5. Project Structure

```text
CandidateWorkspace/
|
|-- data/
|   |-- sample_loads.csv
|   |-- dataset_config.csv
|   |-- monitoring.db
|   |-- empty_loads.csv
|   |-- large_loads.csv
|   |-- malformed_loads.csv
|
|-- src/
|   |-- __init__.py
|   |-- pipeline.py
|   |-- monitor.py
|   |-- storage.py
|
|-- tests/
|   |-- test_pipeline.py
|   |-- test_monitor.py
|   |-- test_storage.py
|   |-- test_partial_failure.py
|   |-- retry_test.py
|   |-- generate_large_data.py
|   |-- test_malformed_input.py
|   |-- test_invalid_timestamp_batch.py
|
|-- outputs/
|
|-- requirements.txt
|-- README.md
|-- .gitignore
```

## 6. Core Features

### 6.1 Data Validation

- Required field validation.
- Timestamp validation.
- Status validation.
- Negative row-count rejection.
- Duplicate event detection.
- Unknown dataset detection.
- Rejected records stored with rejection reasons.

### 6.2 Pipeline Processing

- CSV ingestion.
- Valid and rejected record separation.
- SQLite persistence.
- Idempotent event insertion.
- Duplicate event prevention.
- Pipeline run tracking.

### 6.3 SLA Monitoring

The monitoring system identifies:

- ON_TIME: Dataset completed within its SLA.
- LATE: Dataset completed after its SLA deadline.
- MISSING: No load was recorded after the SLA deadline.
- PENDING: Expected load is still within its scheduled time.
- FAILED: Latest recorded load attempt failed.

### 6.4 Freshness Monitoring

The system calculates freshness using the latest successful load timestamp.

Freshness is compared against the configured threshold.

- FRESH: Data is within the freshness threshold.
- STALE: Data exceeds the freshness threshold.
- UNKNOWN: No successful load is available.

### 6.5 Alert Generation

The monitoring system generates alerts for SLA violations and stale datasets.

Alerts are stored in SQLite and support active-alert tracking and resolution.

## 7. Installation and Setup

### Prerequisites

- Python 3.10 or later.
- pip.
- Visual Studio Code (recommended).

### Step 1: Open the project

Open the CandidateWorkspace folder in VS Code.

### Step 2: Create a virtual environment

```bash
python -m venv venv
```

### Step 3: Activate the environment

Windows PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

### Step 4: Install dependencies

```bash
pip install -r requirements.txt
```

## 8. Running the Pipeline

Run the following command from the project root:

```bash
python -c "from src.pipeline import run_pipeline; print(run_pipeline('data/sample_loads.csv'))"
```

The pipeline reads the input CSV, validates records, separates valid and rejected records, and saves processing results to SQLite.

## 9. Running Dataset Monitoring

```bash
python -c "from src.monitor import monitor_datasets; print(monitor_datasets())"
```

The monitoring process evaluates dataset arrival status, SLA deadlines, freshness, and alerts using the configured dataset information.

## 10. Running Automated Tests

Run the complete test suite:

```bash
python -m pytest -v
```

Latest verified result:

**25 passed, 0 failed.**

Additional validation scripts cover:

- Empty input.
- Malformed input.
- Invalid timestamps.
- Large-volume data.
- Out-of-order events.
- Partial failures and recovery.

## 11. Generating Large Test Data

Run:

```bash
python tests/generate_large_data.py
```

The generator creates a synthetic dataset containing 100,000 records for performance testing.

## 12. Idempotency and Recovery

The pipeline uses unique event identifiers to prevent duplicate event insertion.

Repeated execution of the same input does not create unintended duplicate load events.

Invalid records are stored separately with rejection reasons.

Partial failures are handled by processing valid and rejected records independently.

## 13. Performance Measurement

A 100,000-record synthetic dataset was tested.

| Metric | Result |
|---|---:|
| Total records | 100,000 |
| Valid records | 95,000 |
| Rejected records | 5,000 |
| First-run processing time | 2.73 seconds |
| Repeat-run processing time | 2.14 seconds |

These measurements were obtained on the development machine and may vary across environments.

### Optimization

Duplicate prevention is implemented using SQLite uniqueness constraints and `INSERT OR IGNORE` operations.

This avoids unnecessary duplicate insertions during pipeline reruns.

## 14. Data and Business Rules

- Event IDs are used to identify individual load events.
- Dataset IDs identify configured datasets.
- Timestamps use ISO-8601 format.
- Only SUCCESS and FAILED are accepted as load statuses.
- Row counts must be non-negative.
- Dataset IDs must exist in the configuration.
- Duplicate event IDs are rejected.
- Invalid records are retained in rejected-record storage with reasons.

## 15. Assumptions and Limitations

- Input data is provided as CSV files.
- SQLite is used for local persistent storage.
- Dataset schedules currently use same-day time values.
- Advanced timezone and overnight SLA scheduling are not implemented.
- Monitoring uses the latest recorded load events.
- Production-scale distributed processing is outside the current scope.
- A graphical monitoring dashboard is not yet implemented.

## 16. Future Improvements

- Interactive monitoring dashboard.
- Advanced scheduling and timezone support.
- Distributed processing for larger datasets.
- Integration with external notification systems.
- Cloud-based deployment and monitoring.

## 17. Conclusion

This project provides a tested foundation for dataset SLA and freshness monitoring.

It demonstrates data ingestion, validation, rejection handling, monitoring, alert generation, idempotency, recovery, and performance testing using Python and SQLite.
=======
# dataset-sla-freshness-monitoring-pipeline
