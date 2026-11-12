
import pandas as pd
from src.storage import save_pipeline_results

REQUIRED_COLUMNS = [
    "event_id",
    "dataset_id",
    "source_file",
    "load_completed_at",
    "status",
    "row_count"
]

VALID_STATUSES = ["SUCCESS", "FAILED"]


def validate_records(data, allowed_dataset_ids=None):
    """
    Validate incoming dataset load records.

    Returns:
        valid_records: Records that pass validation.
        rejected_records: Records that fail validation.
    """

    # Check whether all required columns exist
    missing_columns = [
        column for column in REQUIRED_COLUMNS
        if column not in data.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    # Work on a copy to avoid modifying original data
    records = data.copy()

    # Store rejection reasons
    records["reject_reason"] = ""

    # Check missing required values
    for column in REQUIRED_COLUMNS:
        missing = (
            records[column].isna()
            | records[column].astype(str).str.strip().eq("")
        )

        records.loc[missing, "reject_reason"] += (
            f"Missing {column}; "
        )

    # Check configured dataset IDs
    if allowed_dataset_ids is not None:
        unknown_dataset = (
            records["dataset_id"].notna()
            & ~records["dataset_id"].isin(allowed_dataset_ids)
        )

        records.loc[unknown_dataset, "reject_reason"] += (
            "Unknown dataset_id; "
        )

    # Check duplicate event IDs
    duplicate = records["event_id"].duplicated(keep="first")

    records.loc[duplicate, "reject_reason"] += (
        "Duplicate event_id; "
    )

    # Check valid status values
    invalid_status = ~records["status"].isin(VALID_STATUSES)

    records.loc[invalid_status, "reject_reason"] += (
        "Invalid status; "
    )

    # Check row counts
    numeric_rows = pd.to_numeric(
        records["row_count"],
        errors="coerce"
    )

    invalid_row_count = (
        numeric_rows.isna() | (numeric_rows < 0)
    )

    records.loc[invalid_row_count, "reject_reason"] += (
        "Invalid row_count; "
    )

    records["row_count"] = numeric_rows

    # Validate timestamps
    timestamps = pd.to_datetime(
        records["load_completed_at"],
        errors="coerce",
        utc=True
    )

    invalid_timestamp = timestamps.isna()

    records.loc[invalid_timestamp, "reject_reason"] += (
        "Invalid load_completed_at; "
    )

    records["load_completed_at"] = timestamps

    # Separate valid and rejected records
    valid_records = records[
        records["reject_reason"] == ""
    ].copy()

    rejected_records = records[
        records["reject_reason"] != ""
    ].copy()

    return valid_records, rejected_records


def run_pipeline(
    input_file,
    db_path="data/monitoring.db",
    config_file="data/dataset_config.csv"
):
    """
    Execute the complete data processing workflow.
    """

    # Step 1: Read input CSV
    data = pd.read_csv(input_file)

    # Step 2: Read configured datasets
    config = pd.read_csv(config_file)

    if "dataset_id" not in config.columns:
        raise ValueError(
            "Configuration file must contain dataset_id column"
        )

    allowed_dataset_ids = set(
        config["dataset_id"].dropna().astype(str)
    )

    # Step 3: Validate incoming records
    valid_records, rejected_records = validate_records(
        data,
        allowed_dataset_ids
    )

    # Step 4: Save results in SQLite
    summary = save_pipeline_results(
        valid_records,
        rejected_records,
        db_path
    )

    # Step 5: Return processing summary
    summary["input_file"] = input_file

    return summary
