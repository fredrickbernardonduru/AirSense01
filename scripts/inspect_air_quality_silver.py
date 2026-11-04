
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RUN_ID = "20261104T143551_2cdda1f4"

SILVER_DIR = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "air_quality"
    / f"run_id={RUN_ID}"
)

OBSERVATIONS_FILE = SILVER_DIR / "observations.parquet"


def print_section(title: str) -> None:
    print(f"\n--- {title} ---")


def main() -> None:
    if not OBSERVATIONS_FILE.exists():
        raise FileNotFoundError(OBSERVATIONS_FILE)

    table = pq.read_table(OBSERVATIONS_FILE)

    print("\n========== SILVER DATASET INSPECTION ==========")

    print(f"\nRows: {table.num_rows}")
    print(f"Columns: {table.num_columns}")

    print_section("PARQUET SCHEMA")
    print(table.schema)

    print_section("MEASUREMENTS BY PARAMETER AND UNIT")

    parameter_counts = {}

    for row in table.select(["parameter", "unit"]).to_pylist():
        key = (row["parameter"], row["unit"])
        parameter_counts[key] = parameter_counts.get(key, 0) + 1

    for (parameter, unit), count in sorted(parameter_counts.items()):
        print(f"{parameter} ({unit}): {count}")

    print_section("MEASUREMENTS BY CATEGORY")

    # category_counts = pc.value_counts(table["category"]).as_py()

    # for item in category_counts:
    #     print(f"{item['values']}: {item['counts']}")
    category_counts = pc.value_counts(
    table["category"] ).to_pylist()
    for item in category_counts:
        print(f"{item['values']}: {item['counts']}")

    print_section("FRESHNESS")

    freshness_counts = pc.value_counts(table["is_stale"]).as_py()

    for item in freshness_counts:
        print(f"is_stale={item['values']}: {item['counts']}")

    print_section("OBSERVATION AGE")

    ages = table["observation_age_seconds"]

    print(f"Minimum age (seconds): {pc.min(ages).as_py()}")
    print(f"Maximum age (seconds): {pc.max(ages).as_py()}")
    print(f"Average age (seconds): {pc.mean(ages).as_py():.2f}")

    print_section("OLDEST OBSERVATIONS")

    sorted_table = table.sort_by([
        ("observed_at", "ascending")
    ])

    columns = [
        "parameter",
        "value",
        "unit",
        "observed_at",
        "observation_age_seconds",
        "location_name",
    ]

    for row in sorted_table.select(columns).slice(0, 5).to_pylist():
        print(row)

    print_section("NEWEST OBSERVATIONS")

    newest_table = table.sort_by([
        ("observed_at", "descending")
    ])

    for row in newest_table.select(columns).slice(0, 5).to_pylist():
        print(row)

    print_section("MISSING VALUES")

    for column_name in table.column_names:
        null_count = table[column_name].null_count
        print(f"{column_name}: {null_count}")

    print_section("OUTPUT FILES")

    for path in sorted(SILVER_DIR.iterdir()):
        if path.is_file():
            print(f"{path.name}: {path.stat().st_size:,} bytes")

    print("\n================================================")


if __name__ == "__main__":
    main()
