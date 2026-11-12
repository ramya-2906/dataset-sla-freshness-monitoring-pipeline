import pandas as pd
import pytest
import pandas as pd
from src.pipeline import validate_records
from src.pipeline import validate_records


def sample_data():
    return pd.DataFrame([
        {
            "event_id": "EVT001",
            "dataset_id": "DS001",
            "source_file": "sales.csv",
            "load_completed_at": "2026-11-12T09:00:00Z",
            "status": "SUCCESS",
            "row_count": 100
        }
    ])


def test_valid_record():
    data = sample_data()

    valid, rejected = validate_records(data)

    assert len(valid) == 1
    assert len(rejected) == 0


def test_duplicate_event_id():
    data = sample_data()
    duplicate = data.copy()

    data = pd.concat(
        [data, duplicate],
        ignore_index=True
    )

    valid, rejected = validate_records(data)

    assert len(valid) == 1
    assert len(rejected) == 1
    assert "Duplicate event_id" in rejected.iloc[0]["reject_reason"]


def test_missing_required_value():
    data = sample_data()
    data.loc[0, "dataset_id"] = None

    valid, rejected = validate_records(data)

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "Missing dataset_id" in rejected.iloc[0]["reject_reason"]


def test_invalid_status():
    data = sample_data()
    data.loc[0, "status"] = "UNKNOWN"

    valid, rejected = validate_records(data)

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "Invalid status" in rejected.iloc[0]["reject_reason"]


def test_invalid_timestamp():
    data = sample_data()
    data.loc[0, "load_completed_at"] = "not-a-date"

    valid, rejected = validate_records(data)

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "Invalid load_completed_at" in rejected.iloc[0]["reject_reason"]


def test_negative_row_count():
    data = sample_data()
    data.loc[0, "row_count"] = -10

    valid, rejected = validate_records(data)

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "Invalid row_count" in rejected.iloc[0]["reject_reason"]


def test_missing_column():
    data = sample_data().drop(columns=["dataset_id"])

    with pytest.raises(ValueError):
        validate_records(data)

def test_unknown_dataset_is_rejected():
    data = pd.DataFrame([{
        "event_id": "EVT100",
        "dataset_id": "unknown_dataset",
        "source_file": "sales.csv",
        "load_completed_at": "2026-11-12T09:00:00Z",
        "status": "SUCCESS",
        "row_count": 100
    }])

    valid, rejected = validate_records(
        data,
        allowed_dataset_ids={"customers", "orders", "payments"}
    )

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "Unknown dataset_id" in rejected.iloc[0]["reject_reason"]