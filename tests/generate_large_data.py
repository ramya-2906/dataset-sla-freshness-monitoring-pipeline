
import pandas as pd

def generate_data(total_records=100_000):
    records = []

    datasets = ["customers", "orders", "payments"]

    for i in range(total_records):
        dataset_id = datasets[i % 3]

        records.append({
            "event_id": f"EVT{i:07d}",
            "dataset_id": dataset_id,
            "source_file": f"{dataset_id}_{i}.csv",
            "load_completed_at": "2026-11-12T08:00:00Z",
            "status": "SUCCESS",
            "row_count": 1000
        })

    # Make 5% of records invalid
    for i in range(0, total_records, 20):
        records[i]["row_count"] = -10

    data = pd.DataFrame(records)

    data.to_csv(
        "data/large_loads.csv",
        index=False
    )

    print(f"Generated {len(data)} records")
    print(f"Invalid records: {total_records // 20}")
    print(f"Valid records: {total_records - total_records // 20}")


if __name__ == "__main__":
    generate_data()
