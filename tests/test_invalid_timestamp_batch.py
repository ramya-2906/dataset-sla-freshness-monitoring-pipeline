
import pandas as pd
from src.pipeline import validate_records

data = pd.DataFrame([
    {
        "event_id": "EVT201",
        "dataset_id": "customers",
        "source_file": "customers.csv",
        "load_completed_at": "not-a-valid-date",
        "status": "SUCCESS",
        "row_count": 100
    }
])

valid, rejected = validate_records(
    data,
    allowed_dataset_ids={"customers", "orders", "payments"}
)

print("Valid records:", len(valid))
print("Rejected records:", len(rejected))
print("Rejection reason:", rejected.iloc[0]["reject_reason"])

assert len(valid) == 0
assert len(rejected) == 1
assert "Invalid load_completed_at" in rejected.iloc[0]["reject_reason"]

print("TEST PASSED: Invalid timestamp correctly rejected")
