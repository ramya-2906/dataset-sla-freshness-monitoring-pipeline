import sqlite3

connection = sqlite3.connect("data/idempotency_test.db")

connection.execute(
    """
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
    """,
    (
        "EVT009",
        "payments",
        "payments_retry.csv",
        "2026-11-12T09:30:00+00:00",
        "SUCCESS",
        500,
        "retry-test"
    )
)

connection.commit()
connection.close()

print("Successful retry inserted")