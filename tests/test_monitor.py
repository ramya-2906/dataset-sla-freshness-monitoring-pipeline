from datetime import datetime, timedelta, timezone
from datetime import datetime, timezone
from src.monitor import evaluate_dataset_status

from src.monitor import (
    calculate_load_status,
    calculate_freshness,
    check_freshness
)


def test_on_time_load():
    expected = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
    actual = datetime(2026, 11, 12, 9, 50, tzinfo=timezone.utc)
    current = datetime(2026, 11, 12, 10, 30, tzinfo=timezone.utc)

    assert calculate_load_status(expected, actual, current) == "ON_TIME"


def test_late_load():
    expected = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
    actual = datetime(2026, 11, 12, 10, 15, tzinfo=timezone.utc)
    current = datetime(2026, 11, 12, 10, 30, tzinfo=timezone.utc)

    assert calculate_load_status(expected, actual, current) == "LATE"


def test_missing_load():
    expected = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
    current = datetime(2026, 11, 12, 11, 0, tzinfo=timezone.utc)

    assert calculate_load_status(expected, None, current) == "MISSING"


def test_pending_load():
    expected = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
    current = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)

    assert calculate_load_status(expected, None, current) == "PENDING"


def test_freshness():
    current = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
    data_time = current - timedelta(hours=2)

    assert calculate_freshness(data_time, current) == 2.0
    assert check_freshness(data_time, current, 3) == "FRESH"

def test_dataset_on_time():
    deadline = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)
    actual = datetime(2026, 11, 12, 8, 45, tzinfo=timezone.utc)

    result = evaluate_dataset_status(
        "customers",
        deadline,
        {
            "status": "SUCCESS",
            "load_completed_at": actual
        },
        deadline
    )

    assert result["status"] == "ON_TIME"
    assert result["alert"] is None


def test_dataset_late():
    deadline = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)
    actual = datetime(2026, 11, 12, 9, 15, tzinfo=timezone.utc)

    result = evaluate_dataset_status(
        "orders",
        deadline,
        {
            "status": "SUCCESS",
            "load_completed_at": actual
        },
        actual
    )

    assert result["status"] == "LATE"
    assert result["alert"] is not None


def test_dataset_missing():
    deadline = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)
    current = datetime(2026, 11, 12, 9, 30, tzinfo=timezone.utc)

    result = evaluate_dataset_status(
        "payments",
        deadline,
        None,
        current
    )

    assert result["status"] == "MISSING"
    assert result["alert"] is not None


def test_dataset_pending():
    deadline = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)
    current = datetime(2026, 11, 12, 8, 30, tzinfo=timezone.utc)

    result = evaluate_dataset_status(
        "customers",
        deadline,
        None,
        current
    )

    assert result["status"] == "PENDING"
    assert result["alert"] is None


def test_dataset_failed():
    deadline = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)

    result = evaluate_dataset_status(
        "payments",
        deadline,
        {"status": "FAILED", "load_completed_at": None},
        deadline
    )

    assert result["status"] == "FAILED"
    assert result["alert"] is not None

def test_monitor_detects_stale_dataset(tmp_path):
    import sqlite3
    import pandas as pd
    from datetime import datetime, timezone
    from src.monitor import monitor_datasets

    # Create a temporary database
    db_path = tmp_path / "test_monitoring.db"

    connection = sqlite3.connect(db_path)

    connection.execute("""
        CREATE TABLE load_events (
            dataset_id TEXT,
            status TEXT,
            load_completed_at TEXT
        )
    """)

    connection.execute("""
        INSERT INTO load_events
        VALUES (?, ?, ?)
    """, (
        "customers",
        "SUCCESS",
        "2026-11-12T08:00:00+00:00"
    ))

    connection.commit()
    connection.close()

    # Create a temporary dataset configuration
    config_path = tmp_path / "dataset_config.csv"

    config = pd.DataFrame([{
        "dataset_id": "customers",
        "expected_arrival_time": "07:00",
        "sla_deadline_time": "07:30",
        "freshness_threshold_hours": 1
    }])

    config.to_csv(config_path, index=False)

    # Run monitoring at 10:00 UTC
    results = monitor_datasets(
        config_file=str(config_path),
        db_path=str(db_path),
        current_time=datetime(
            2026, 11, 12, 10, 0,
            tzinfo=timezone.utc
        )
    )

    # Verify freshness and alert
    assert results[0]["freshness_hours"] == 2.0
    assert results[0]["freshness_status"] == "STALE"
    assert "stale" in results[0]["alert"].lower()
