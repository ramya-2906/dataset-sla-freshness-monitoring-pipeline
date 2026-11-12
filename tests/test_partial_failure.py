import pandas as pd
from src.pipeline import run_pipeline


def test_partial_failure_and_recovery(tmp_path):

    data = pd.DataFrame([
        {
            "event_id": "PF001",
            "dataset_id": "customers",
            "source_file": "customers.csv",
            "load_completed_at": "2026-11-12T08:00:00Z",
            "status": "SUCCESS",
            "row_count": 100
        },
        {
            "event_id": "PF002",
            "dataset_id": "orders",
            "source_file": "orders.csv",
            "load_completed_at": "invalid_timestamp",
            "status": "SUCCESS",
            "row_count": 200
        }
    ])

    input_file = tmp_path / "partial_failure_loads.csv"
    db_file = tmp_path / "partial_failure_test.db"

    data.to_csv(input_file, index=False)

    result = run_pipeline(
        str(input_file),
        db_path=str(db_file)
    )

    print("PIPELINE RESULT:", result)

    assert result["total_records"] == 2
    assert result["valid_records"] == 1
    assert result["rejected_input_records"] == 1
    assert result["accepted_records"] == 1
    assert result["newly_rejected_records"] == 1

    print("TEST PASSED: Valid record saved and invalid record rejected")