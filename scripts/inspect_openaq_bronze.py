
import json
from pathlib import Path
from datetime import datetime
from collections import Counter

BRONZE = Path("data/bronze")

sensor_lookup = {}

# 1. Build sensor metadata lookup.
for path in (BRONZE / "openaq_locations").rglob("*.jsonl"):
    with path.open(encoding="utf-8") as file:
        for line in file:
            record = json.loads(line)

            for location in record.get("payload", []):
                for sensor in location.get("sensors", []):
                    parameter = sensor.get("parameter") or {}

                    sensor_lookup[sensor.get("id")] = {
                        "parameter": parameter.get("name", "unknown"),
                        "unit": parameter.get("units", "unknown"),
                    }

# 2. Inspect raw measurements.
total = 0
mapped = 0
unmapped = 0

parameter_counts = Counter()
age_counts = Counter()
ages = []

for path in (BRONZE / "openaq_latest").rglob("*.jsonl"):
    with path.open(encoding="utf-8") as file:
        for line in file:
            record = json.loads(line)
            metadata = record.get("metadata", {})

            for measurement in record.get("payload", []):
                total += 1

                sensor_id = measurement.get("sensorsId")
                sensor = sensor_lookup.get(sensor_id)

                if sensor:
                    mapped += 1
                    parameter_counts[
                        (sensor["parameter"], sensor["unit"])
                    ] += 1
                else:
                    unmapped += 1

                timestamp = (
                    measurement.get("datetime", {}).get("utc")
                )

                if not timestamp:
                    age_counts["missing_timestamp"] += 1
                    continue

                try:
                    observed_at = datetime.fromisoformat(
                        timestamp.replace("Z", "+00:00")
                    )
                    ingested_at = datetime.fromisoformat(
                        metadata["ingested_at"]
                    )

                    age_hours = (
                        ingested_at - observed_at
                    ).total_seconds() / 3600

                    ages.append(age_hours)

                    category = (
                        "older_than_30_days"
                        if age_hours > 30 * 24
                        else "within_30_days"
                    )
                    age_counts[category] += 1

                except (ValueError, KeyError):
                    age_counts["invalid_timestamp"] += 1

print("\n========== OPENAQ DATA QUALITY REPORT ==========")
print(f"Raw measurements: {total}")
print(f"Mapped measurements: {mapped}")
print(f"Unmapped measurements: {unmapped}")

print("\nMeasurements by parameter and unit:")
for (parameter, unit), count in parameter_counts.items():
    print(f"  {parameter} ({unit}): {count}")

print("\nObservation age:")
for category, count in age_counts.items():
    print(f"  {category}: {count}")

if ages:
    print(f"\nOldest observation: {max(ages) / 24:.1f} days old")
    print(f"Newest observation: {min(ages) / 24:.1f} days old")
