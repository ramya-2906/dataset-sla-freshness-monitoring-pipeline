
import sqlite3
import pandas as pd

from src.storage import (
    save_pipeline_results,
    sync_monitoring_alerts
)


def test_save_valid_records(tmp_path):
    db_path = str(tmp_path / "test.db")

    valid = pd.DataFrame([{
        "event_id": "EVT001",
        "dataset_id": "DS001",
        "source_file": "sales.csv",
        "load_completed_at": pd.Timestamp(
            "2026-11-12T09:00:00Z"
        ),
        "status": "SUCCESS",
        "row_count": 100
    }])

    rejected = pd.DataFrame()

    result = save_pipeline_results(
        valid, rejected, db_path
    )

    assert result["total_records"] == 1
    assert result["accepted_records"] == 1
    assert result["rejected_records"] == 0

    connection = sqlite3.connect(db_path)

    count = connection.execute(
        "SELECT COUNT(*) FROM load_events"
    ).fetchone()[0]

    connection.close()

    assert count == 1


def test_duplicate_event_is_not_inserted_twice(tmp_path):
    db_path = str(tmp_path / "test.db")

    valid = pd.DataFrame([{
        "event_id": "EVT001",
        "dataset_id": "DS001",
        "source_file": "sales.csv",
        "load_completed_at": pd.Timestamp(
            "2026-11-12T09:00:00Z"
        ),
        "status": "SUCCESS",
        "row_count": 100
    }])

    rejected = pd.DataFrame()

    first = save_pipeline_results(
        valid, rejected, db_path
    )

    second = save_pipeline_results(
        valid, rejected, db_path
    )

    assert first["accepted_records"] == 1
    assert second["accepted_records"] == 0

    connection = sqlite3.connect(db_path)

    count = connection.execute(
        "SELECT COUNT(*) FROM load_events"
    ).fetchone()[0]

    connection.close()

    assert count == 1


def test_rejected_records_are_saved(tmp_path):
    db_path = str(tmp_path / "test.db")

    valid = pd.DataFrame()

    rejected = pd.DataFrame([{
        "event_id": "EVT002",
        "dataset_id": "DS001",
        "source_file": "sales.csv",
        "load_completed_at": "invalid-date",
        "status": "SUCCESS",
        "row_count": 100,
        "reject_reason": "Invalid timestamp"
    }])

    result = save_pipeline_results(
        valid, rejected, db_path
    )

    assert result["rejected_records"] == 1

    connection = sqlite3.connect(db_path)

    count = connection.execute(
        "SELECT COUNT(*) FROM rejected_records"
    ).fetchone()[0]

    connection.close()

    assert count == 1


def test_alert_lifecycle_and_duplicate_prevention(tmp_path):
    db_path = str(tmp_path / "alerts.db")

    late_result = [{
        "dataset_id": "customers",
        "status": "LATE",
        "freshness_status": "FRESH"
    }]

    # First run: create alert
    first = sync_monitoring_alerts(
        late_result, db_path
    )

    assert first["active_alerts"] == 1

    # Second run: update existing alert
    second = sync_monitoring_alerts(
        late_result, db_path
    )

    assert second["active_alerts"] == 1

    connection = sqlite3.connect(db_path)

    count = connection.execute(
        "SELECT COUNT(*) FROM monitoring_alerts"
    ).fetchone()[0]

    connection.close()

    assert count == 1

    # Third run: resolve alert
    resolved_result = [{
        "dataset_id": "customers",
        "status": "ON_TIME",
        "freshness_status": "FRESH"
    }]

    third = sync_monitoring_alerts(
        resolved_result, db_path
    )

    assert third["resolved_alerts"] == 1

    # Fourth run: create a new alert after resolution
    fourth = sync_monitoring_alerts(
        late_result, db_path
    )

    assert fourth["active_alerts"] == 1

    connection = sqlite3.connect(db_path)

    total = connection.execute(
        "SELECT COUNT(*) FROM monitoring_alerts"
    ).fetchone()[0]

    active = connection.execute(
        "SELECT COUNT(*) FROM monitoring_alerts "
        "WHERE resolved_at IS NULL"
    ).fetchone()[0]

    connection.close()

    assert total == 2
    assert active == 1


def test_stale_data_creates_freshness_alert(tmp_path):
    db_path = str(tmp_path / "freshness.db")

    results = [{
        "dataset_id": "customers",
        "status": "ON_TIME",
        "freshness_status": "STALE"
    }]

    result = sync_monitoring_alerts(
        results, db_path
    )

    assert result["active_alerts"] == 1

    connection = sqlite3.connect(db_path)

    alert = connection.execute(
        "SELECT alert_type FROM monitoring_alerts"
    ).fetchone()

    connection.close()

    assert alert[0] == "FRESHNESS"
