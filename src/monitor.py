
import sqlite3
from datetime import datetime, timezone

import pandas as pd

from src.storage import sync_monitoring_alerts


def calculate_load_status(expected_arrival, actual_arrival, current_time):
    """
    Determine whether a dataset arrived on time, late,
    is missing, or is still pending.
    """

    if actual_arrival is not None:
        if actual_arrival <= expected_arrival:
            return "ON_TIME"
        return "LATE"

    if current_time > expected_arrival:
        return "MISSING"

    return "PENDING"


def calculate_freshness(data_timestamp, current_time):
    """
    Calculate how old the dataset is in hours.
    """

    if data_timestamp is None:
        return None

    freshness = current_time - data_timestamp

    return freshness.total_seconds() / 3600


def check_freshness(data_timestamp, current_time, threshold_hours):
    """
    Check whether the dataset is fresh or stale.
    """

    freshness_hours = calculate_freshness(
        data_timestamp,
        current_time
    )

    if freshness_hours is None:
        return "UNKNOWN"

    if freshness_hours <= threshold_hours:
        return "FRESH"

    return "STALE"


def evaluate_dataset_status(
    dataset_id,
    sla_deadline,
    latest_load,
    current_time
):
    """
    Evaluate a dataset against its SLA deadline.

    latest_load should contain:
        status: SUCCESS or FAILED
        load_completed_at: datetime
    """

    # Check whether a load attempt exists
    if latest_load is None:
        if current_time > sla_deadline:
            return {
                "dataset_id": dataset_id,
                "status": "MISSING",
                "alert": (
                    f"{dataset_id} missed its SLA deadline. "
                    "No load attempt was recorded."
                )
            }

        return {
            "dataset_id": dataset_id,
            "status": "PENDING",
            "alert": None
        }

    load_status = latest_load["status"]

    # Check failed load attempts
    if load_status == "FAILED":
        return {
            "dataset_id": dataset_id,
            "status": "FAILED",
            "alert": (
                f"{dataset_id} load failed. "
                "Check the ingestion job and retry."
            )
        }

    # Check successful load completion time
    if load_status == "SUCCESS":

        actual_arrival = latest_load["load_completed_at"]

        if actual_arrival <= sla_deadline:
            return {
                "dataset_id": dataset_id,
                "status": "ON_TIME",
                "alert": None
            }

        return {
            "dataset_id": dataset_id,
            "status": "LATE",
            "alert": (
                f"{dataset_id} completed after its SLA deadline. "
                "Investigate the ingestion delay."
            )
        }

    raise ValueError(
        f"Unsupported load status: {load_status}"
    )


def monitor_datasets(
    config_file="data/dataset_config.csv",
    db_path="data/monitoring.db",
    current_time=None
):
    """
    Monitor configured datasets against their SLA deadlines
    and freshness thresholds.

    Automatically creates, updates, and resolves alerts
    in the SQLite database.

    Returns a list of dataset monitoring results.
    """

    if current_time is None:
        current_time = datetime.now(timezone.utc)

    if current_time.tzinfo is None:
        raise ValueError("current_time must be timezone-aware")

    # Step 1: Read dataset configuration
    config = pd.read_csv(config_file)

    required_columns = [
        "dataset_id",
        "expected_arrival_time",
        "sla_deadline_time",
        "freshness_threshold_hours"
    ]

    missing_columns = [
        column for column in required_columns
        if column not in config.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing configuration columns: {missing_columns}"
        )

    if config["dataset_id"].isna().any():
        raise ValueError("Dataset IDs cannot be empty")

    if config["dataset_id"].duplicated().any():
        raise ValueError("Duplicate dataset IDs in configuration")

    # Step 2: Read load events from SQLite
    connection = sqlite3.connect(db_path)

    try:
        events = pd.read_sql_query(
            """
            SELECT dataset_id, status, load_completed_at
            FROM load_events
            ORDER BY load_completed_at DESC
            """,
            connection
        )
    finally:
        connection.close()

    # Step 3: Find the latest load attempt
    # and latest successful load for each dataset
    latest_loads = {}
    latest_successful_loads = {}

    for _, event in events.iterrows():

        dataset_id = event["dataset_id"]

        load_time = pd.to_datetime(
            event["load_completed_at"],
            utc=True
        ).to_pydatetime().astimezone(current_time.tzinfo)

        if dataset_id not in latest_loads:
            latest_loads[dataset_id] = {
                "status": event["status"],
                "load_completed_at": load_time
            }

        if (
            event["status"] == "SUCCESS"
            and dataset_id not in latest_successful_loads
        ):
            latest_successful_loads[dataset_id] = load_time

    # Step 4: Evaluate each configured dataset
    results = []

    for _, dataset in config.iterrows():

        dataset_id = str(dataset["dataset_id"])

        deadline_time = datetime.strptime(
            str(dataset["sla_deadline_time"]),
            "%H:%M"
        ).time()

        expected_time = datetime.strptime(
            str(dataset["expected_arrival_time"]),
            "%H:%M"
        ).time()

        sla_deadline = datetime.combine(
            current_time.date(),
            deadline_time,
            tzinfo=current_time.tzinfo
        )

        expected_arrival = datetime.combine(
            current_time.date(),
            expected_time,
            tzinfo=current_time.tzinfo
        )

        # Evaluate SLA status
        result = evaluate_dataset_status(
            dataset_id=dataset_id,
            sla_deadline=sla_deadline,
            latest_load=latest_loads.get(dataset_id),
            current_time=current_time
        )

        result["expected_arrival"] = expected_arrival.isoformat()
        result["sla_deadline"] = sla_deadline.isoformat()

        threshold_hours = float(
            dataset["freshness_threshold_hours"]
        )

        result["freshness_threshold_hours"] = threshold_hours

        # Step 5: Calculate freshness using the latest
        # successful load, not a failed attempt
        last_successful_load = latest_successful_loads.get(
            dataset_id
        )

        freshness_hours = calculate_freshness(
            last_successful_load,
            current_time
        )

        freshness_status = check_freshness(
            last_successful_load,
            current_time,
            threshold_hours
        )

        result["freshness_hours"] = freshness_hours
        result["freshness_status"] = freshness_status

        # Step 6: Generate a freshness alert if data is stale
        if freshness_status == "STALE":

            freshness_alert = (
                f"{dataset_id} data is stale. "
                "Check the source data timestamp."
            )

            if result["alert"]:
                result["alert"] += " " + freshness_alert
            else:
                result["alert"] = freshness_alert

        results.append(result)

    # Step 7: Save and synchronize monitoring alerts
    alert_summary = sync_monitoring_alerts(
        results,
        db_path=db_path
    )

    print("Monitoring alert summary:", alert_summary)

    return results
