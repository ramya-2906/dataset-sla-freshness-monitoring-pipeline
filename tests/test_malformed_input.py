
import pandas as pd
from src.pipeline import run_pipeline

# Create a CSV with missing required columns
data = pd.DataFrame({
    "event_id": ["EVT100"],
    "dataset_id": ["customers"],
    "status": ["SUCCESS"]
})

data.to_csv("data/malformed_loads.csv", index=False)

# Run the pipeline and verify that it rejects the file
try:
    run_pipeline(
        "data/malformed_loads.csv",
        db_path="data/malformed_test.db"
    )
    print("TEST FAILED: Missing columns were not detected")

except ValueError as error:
    print("TEST PASSED:", error)
