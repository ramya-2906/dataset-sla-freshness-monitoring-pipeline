
import sqlite3
import json
import uuid
import hashlib
from datetime import datetime, timezone


def initialize_database(db_path="data/monitoring.db"):
    """Create database tables and update existing schema."""

    connection = sqlite3.connect(db_path)

    try:
        cursor = connection.cursor()

        # Pipeline execution history
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                total_records INTEGER,
                accepted_records INTEGER,
                rejected_records INTEGER
            )
        """)

        # Monitoring alert history
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monitoring_alerts (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                message TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                resolved_at TEXT
            )
        """)

        # Prevent duplicate active alerts
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_active_monitoring_alerts
            ON monitoring_alerts(dataset_id, alert_type)
            WHERE resolved_at IS NULL
        """)

        # Successfully processed load events
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS load_events (
                event_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                source_file TEXT NOT NULL,
                load_completed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                run_id TEXT NOT NULL
            )
        """)

        # Rejected records
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rejected_records (
                reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                event_id TEXT,
                reject_reason TEXT NOT NULL,
                record_json TEXT NOT NULL,
                record_hash TEXT UNIQUE
            )
        """)

        # Migrate existing databases if record_hash is missing
        cursor.execute("PRAGMA table_info(rejected_records)")

        columns = [
            column[1] for column in cursor.fetchall()
        ]

        if "record_hash" not in columns:
            cursor.execute("""
                ALTER TABLE rejected_records
                ADD COLUMN record_hash TEXT
            """)

        # Prevent duplicate rejected records
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_rejected_record_hash
            ON rejected_records(record_hash)
        """)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def sync_monitoring_alerts(
    results,
    db_path="data/monitoring.db"
):
    """
    Create, update, and resolve monitoring alerts.

    Avoids duplicate active alerts for the same
    dataset and alert type.
    """

    initialize_database(db_path)

    connection = sqlite3.connect(db_path)

    try:
        cursor = connection.cursor()

        now = datetime.now(timezone.utc).isoformat()

        # Collect alerts that should currently be active
        desired_alerts = {}

        for result in results:

            dataset_id = result["dataset_id"]
            status = result["status"]

            # SLA-related alerts
            if status in ["LATE", "MISSING", "FAILED"]:

                messages = {
                    "LATE": (
                        f"{dataset_id} completed after its SLA deadline. "
                        "Investigate the ingestion delay."
                    ),
                    "MISSING": (
                        f"{dataset_id} missed its SLA deadline. "
                        "Check the ingestion schedule and source."
                    ),
                    "FAILED": (
                        f"{dataset_id} load failed. "
                        "Check the ingestion job and retry."
                    )
                }

                desired_alerts[
                    (dataset_id, "SLA")
                ] = messages[status]

            # Freshness-related alerts
            if result["freshness_status"] == "STALE":

                desired_alerts[
                    (dataset_id, "FRESHNESS")
                ] = (
                    f"{dataset_id} data is stale. "
                    "Check the source data timestamp."
                )

        # Read currently active alerts
        cursor.execute("""
            SELECT alert_id, dataset_id, alert_type
            FROM monitoring_alerts
            WHERE resolved_at IS NULL
        """)

        active_rows = cursor.fetchall()

        active_alerts = {
            (row[1], row[2]): row[0]
            for row in active_rows
        }

        # Create new alerts or update existing alerts
        for alert_key, message in desired_alerts.items():

            dataset_id, alert_type = alert_key

            if alert_key in active_alerts:

                # Update an existing active alert
                cursor.execute("""
                    UPDATE monitoring_alerts
                    SET message = ?,
                        last_seen_at = ?
                    WHERE alert_id = ?
                """, (
                    message,
                    now,
                    active_alerts[alert_key]
                ))

            else:

                # Create a new alert
                cursor.execute("""
                    INSERT INTO monitoring_alerts (
                        dataset_id,
                        alert_type,
                        message,
                        first_seen_at,
                        last_seen_at,
                        resolved_at
                    )
                    VALUES (?, ?, ?, ?, ?, NULL)
                """, (
                    dataset_id,
                    alert_type,
                    message,
                    now,
                    now
                ))

        # Resolve alerts that are no longer present
        resolved_count = 0

        for alert_key, alert_id in active_alerts.items():

            if alert_key not in desired_alerts:

                cursor.execute("""
                    UPDATE monitoring_alerts
                    SET resolved_at = ?
                    WHERE alert_id = ?
                """, (
                    now,
                    alert_id
                ))

                resolved_count += cursor.rowcount

        connection.commit()

        return {
            "active_alerts": len(desired_alerts),
            "resolved_alerts": resolved_count
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def save_pipeline_results(
    valid_records,
    rejected_records,
    db_path="data/monitoring.db"
):
    """
    Save accepted and rejected records in SQLite.

    Tracks total, valid, rejected, inserted,
    and duplicate record counts.
    """

    initialize_database(db_path)

    connection = sqlite3.connect(db_path)

    try:
        cursor = connection.cursor()

        run_id = str(uuid.uuid4())

        started_at = datetime.now(
            timezone.utc
        ).isoformat()

        # Calculate input counts
        total_count = (
            len(valid_records) + len(rejected_records)
        )

        valid_count = len(valid_records)
        rejected_input_count = len(rejected_records)

        accepted_count = 0
        duplicate_count = 0
        newly_rejected_count = 0

        # Save valid records
        for record in valid_records.to_dict("records"):

            cursor.execute("""
                INSERT OR IGNORE INTO load_events (
                    event_id,
                    dataset_id,
                    source_file,
                    load_completed_at,
                    status,
                    row_count,
                    run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                str(record["event_id"]),
                str(record["dataset_id"]),
                str(record["source_file"]),
                record["load_completed_at"].isoformat(),
                str(record["status"]),
                int(record["row_count"]),
                run_id
            ))

            if cursor.rowcount == 1:
                accepted_count += 1
            else:
                duplicate_count += 1

        # Save rejected records with unique fingerprints
        for record in rejected_records.to_dict("records"):

            record_json = json.dumps(
                record,
                default=str,
                sort_keys=True
            )

            # Generate a stable fingerprint
            record_hash = hashlib.sha256(
                record_json.encode("utf-8")
            ).hexdigest()

            cursor.execute("""
                INSERT OR IGNORE INTO rejected_records (
                    run_id,
                    event_id,
                    reject_reason,
                    record_json,
                    record_hash
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                run_id,
                str(record.get("event_id", "")),
                str(record["reject_reason"]),
                record_json,
                record_hash
            ))

            newly_rejected_count += cursor.rowcount

        # Save execution history
        cursor.execute("""
            INSERT INTO pipeline_runs (
                run_id,
                started_at,
                total_records,
                accepted_records,
                rejected_records
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            run_id,
            started_at,
            total_count,
            accepted_count,
            newly_rejected_count
        ))

        connection.commit()

        return {
            "run_id": run_id,
            "total_records": total_count,
            "valid_records": valid_count,
            "rejected_input_records": rejected_input_count,
            "accepted_records": accepted_count,
            "duplicate_records": duplicate_count,
            "rejected_records": newly_rejected_count,
            "newly_rejected_records": newly_rejected_count
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()
