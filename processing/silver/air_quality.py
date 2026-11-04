
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Project paths and processing configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "air_quality"

# An observation older than this is marked stale.
# Stale observations are still valid historical data.
STALE_AFTER_HOURS = 6


# ---------------------------------------------------------------------------
# Parameter standardisation
# ---------------------------------------------------------------------------

PARAMETER_ALIASES = {
    "pm25": "pm2_5",
    "pm2.5": "pm2_5",
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "pm1": "pm1",
    "no2": "no2",
    "o3": "o3",
    "co": "co",
    "so2": "so2",
    "relativehumidity": "relative_humidity",
    "relative_humidity": "relative_humidity",
    "temperature": "temperature",
    "pressure": "pressure",
    "um003": "um003",
}

POLLUTANT_PARAMETERS = {
    "pm2_5",
    "pm10",
    "pm1",
    "no2",
    "o3",
    "co",
    "so2",
}

ENVIRONMENTAL_PARAMETERS = {
    "relative_humidity",
    "temperature",
    "pressure",
}

PARTICLE_PARAMETERS = {"um003"}


def canonical_parameter(name: str | None) -> str | None:
    """Map source parameter names to AirSense canonical names."""
    if not name:
        return None

    cleaned = re.sub(r"[\s-]+", "", name.strip().lower())
    return PARAMETER_ALIASES.get(cleaned)


def normalize_unit(unit: str | None) -> str:
    """Normalise common unit spelling differences."""
    if not unit:
        return ""

    cleaned = unit.strip().lower()
    cleaned = cleaned.replace("μ", "µ")
    cleaned = cleaned.replace("³", "3")
    cleaned = cleaned.replace(" ", "")
    return cleaned


def standardize_value(
    parameter: str,
    value: float,
    source_unit: str | None,
) -> tuple[float, str]:
    """
    Convert supported measurements into canonical units.

    Raises ValueError when the parameter's unit is unsupported.
    """
    unit = normalize_unit(source_unit)

    # Particulate matter: micrograms per cubic metre.
    if parameter in {"pm2_5", "pm10", "pm1"}:
        if unit not in {"µg/m3", "ug/m3"}:
            raise ValueError(f"Unsupported particulate matter unit: {source_unit}")
        return value, "µg/m³"

    # Temperature: Celsius.
    if parameter == "temperature":
        if unit in {"c", "°c", "degc", "celsius"}:
            return value, "°C"

        if unit in {"f", "°f", "degf", "fahrenheit"}:
            return (value - 32) * 5 / 9, "°C"

        raise ValueError(f"Unsupported temperature unit: {source_unit}")

    # Relative humidity: percentage.
    if parameter == "relative_humidity":
        if unit not in {"%", "percent", "percentage"}:
            raise ValueError(f"Unsupported humidity unit: {source_unit}")
        return value, "%"

    # Atmospheric pressure: hectopascals.
    if parameter == "pressure":
        if unit in {"hpa", "mbar"}:
            return value, "hPa"

        if unit == "pa":
            return value / 100, "hPa"

        if unit == "kpa":
            return value * 10, "hPa"

        raise ValueError(f"Unsupported pressure unit: {source_unit}")

    # Gas concentrations: preserve a recognised concentration unit.
    gas_units = {
        "ppb": "ppb",
        "ppm": "ppm",
        "µg/m3": "µg/m³",
        "ug/m3": "µg/m³",
        "mg/m3": "mg/m³",
    }

    if parameter in {"no2", "o3", "co", "so2"}:
        if unit not in gas_units:
            raise ValueError(f"Unsupported gas concentration unit: {source_unit}")
        return value, gas_units[unit]

    # Particle count: preserve the source unit.
    if parameter in PARTICLE_PARAMETERS:
        if not source_unit or not source_unit.strip():
            raise ValueError("Particle count measurement has no source unit")
        return value, source_unit.strip()

    raise ValueError(f"No unit standardisation rule for: {parameter}")


# ---------------------------------------------------------------------------
# Timestamp handling
# ---------------------------------------------------------------------------

def parse_utc_timestamp(value: Any) -> datetime:
    """
    Parse a timestamp that explicitly identifies its timezone.

    Naive timestamps are rejected rather than silently interpreted as UTC.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing observation timestamp")

    cleaned = value.strip().replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value}") from exc

    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp has no timezone information: {value}")

    return parsed.astimezone(timezone.utc)


def parse_ingested_at(value: Any) -> datetime:
    """Read the original Bronze ingestion timestamp."""
    if not value:
        raise ValueError("Bronze record has no ingested_at timestamp")

    return parse_utc_timestamp(value)


# ---------------------------------------------------------------------------
# Bronze discovery and metadata lookup
# ---------------------------------------------------------------------------

def read_jsonl(path: Path) -> list[dict]:
    """Read JSON objects from a JSON Lines file."""
    records = []

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON in {path}, line {line_number}"
                ) from exc

    return records


def discover_bronze_files(source: str) -> list[Path]:
    """Find every Bronze JSONL batch for a source."""
    source_dir = BRONZE_DIR / source

    if not source_dir.exists():
        return []

    return sorted(source_dir.rglob("*.jsonl"))


def build_sensor_lookup() -> dict[tuple[str, str], dict]:
    """
    Build a lookup from stored OpenAQ location batches.

    Key: (location_id, sensor_id)
    Value: sensor metadata
    """
    sensor_lookup = {}

    location_files = discover_bronze_files("openaq_locations")

    for path in location_files:
        for record in read_jsonl(path):
            payload = record.get("payload")

            if not isinstance(payload, list):
                continue

            for location in payload:
                location_id = location.get("id")

                if location_id is None:
                    continue

                for sensor in location.get("sensors") or []:
                    sensor_id = sensor.get("id")

                    if sensor_id is None:
                        continue

                    sensor_lookup[
                        (str(location_id), str(sensor_id))
                    ] = {
                        **sensor,
                        "_location_name": location.get("name"),
                        "_location_coordinates": location.get("coordinates"),
                    }

    return sensor_lookup


# ---------------------------------------------------------------------------
# Measurement transformation
# ---------------------------------------------------------------------------

def reject_record(
    reason: str,
    raw: dict,
    metadata: dict,
    processed_at: datetime,
) -> dict:
    """Build a standard rejected-record structure."""
    return {
        "source": "openaq",
        "city_id": metadata.get("city_id"),
        "location_id": metadata.get("location_id"),
        "sensor_id": raw.get("sensorsId") or raw.get("sensors_id"),
        "rejection_reason": reason,
        "raw_record": json.dumps(raw, ensure_ascii=False, default=str),
        "bronze_ingested_at": metadata.get("ingested_at"),
        "processed_at": processed_at,
    }


def transform_measurement(
    raw: dict,
    bronze_metadata: dict,
    sensor_lookup: dict[tuple[str, str], dict],
    processed_at: datetime,
) -> tuple[dict | None, dict | None]:
    """
    Transform one raw OpenAQ measurement.

    Returns:
        (valid_record, None) when accepted
        (None, rejected_record) when rejected
    """
    location_id = (
        raw.get("locationsId")
        or raw.get("locations_id")
        or bronze_metadata.get("location_id")
    )

    sensor_id = raw.get("sensorsId") or raw.get("sensors_id")

    if location_id is None or sensor_id is None:
        return None, reject_record(
            "Missing location_id or sensor_id",
            raw,
            bronze_metadata,
            processed_at,
        )

    sensor = sensor_lookup.get((str(location_id), str(sensor_id)))

    if sensor is None:
        return None, reject_record(
            "Sensor metadata not found",
            raw,
            bronze_metadata,
            processed_at,
        )

    parameter_metadata = sensor.get("parameter") or {}

    if isinstance(parameter_metadata, dict):
        original_parameter = (
            parameter_metadata.get("name")
            or parameter_metadata.get("displayName")
        )
        source_unit = parameter_metadata.get("units")
    else:
        original_parameter = str(parameter_metadata)
        source_unit = sensor.get("unit")

    parameter = canonical_parameter(original_parameter)

    if parameter is None:
        return None, reject_record(
            f"Unsupported parameter: {original_parameter}",
            raw,
            bronze_metadata,
            processed_at,
        )

    raw_value = raw.get("value")

    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None, reject_record(
            "Measurement value is missing or not numeric",
            raw,
            bronze_metadata,
            processed_at,
        )

    if not math.isfinite(value):
        return None, reject_record(
            "Measurement value is not finite",
            raw,
            bronze_metadata,
            processed_at,
        )

    raw_datetime = raw.get("datetime") or {}

    if isinstance(raw_datetime, dict):
        timestamp_value = raw_datetime.get("utc")
    else:
        timestamp_value = raw_datetime

    try:
        observed_at = parse_utc_timestamp(timestamp_value)
    except ValueError as exc:
        return None, reject_record(
            str(exc),
            raw,
            bronze_metadata,
            processed_at,
        )

    try:
        standardized_value, standardized_unit = standardize_value(
            parameter,
            value,
            source_unit,
        )
    except ValueError as exc:
        return None, reject_record(
            str(exc),
            raw,
            bronze_metadata,
            processed_at,
        )

    try:
        bronze_ingested_at = parse_ingested_at(
            bronze_metadata.get("ingested_at")
        )
    except ValueError as exc:
        return None, reject_record(
            str(exc),
            raw,
            bronze_metadata,
            processed_at,
        )

    age = processed_at - observed_at
    is_stale = age > timedelta(hours=STALE_AFTER_HOURS)

    coordinates = (
        raw.get("coordinates")
        or sensor.get("_location_coordinates")
        or {}
    )

    category = (
        "pollutant"
        if parameter in POLLUTANT_PARAMETERS
        else "environmental"
        if parameter in ENVIRONMENTAL_PARAMETERS
        else "particle_count"
    )

    observed_at_string = observed_at.isoformat()

    return {
        "event_id": f"openaq:{sensor_id}:{observed_at_string}",
        "event_type": "air_quality_observation",
        "source": "openaq",
        "city_id": bronze_metadata.get("city_id"),
        "parameter": parameter,
        "parameter_original": original_parameter,
        "category": category,
        "value": standardized_value,
        "unit": standardized_unit,
        "source_unit": source_unit,
        "observed_at": observed_at,
        "bronze_ingested_at": bronze_ingested_at,
        "processed_at": processed_at,
        "observation_age_seconds": int(age.total_seconds()),
        "is_stale": is_stale,
        "location_id": str(location_id),
        "location_name": (
            sensor.get("_location_name")
            or bronze_metadata.get("location_name")
        ),
        "sensor_id": str(sensor_id),
        "latitude": coordinates.get("latitude"),
        "longitude": coordinates.get("longitude"),
        "country_code": bronze_metadata.get("country_code"),
    }, None


# ---------------------------------------------------------------------------
# Deduplication and Parquet output
# ---------------------------------------------------------------------------

def deduplicate(records: list[dict]) -> list[dict]:
    """
    Keep the most recently ingested copy of each sensor observation.

    The stable observation key is sensor_id + observed_at.
    """
    unique_records = {}

    for record in records:
        key = (
            record["sensor_id"],
            record["observed_at"],
        )

        existing = unique_records.get(key)

        if (
            existing is None
            or record["bronze_ingested_at"]
            > existing["bronze_ingested_at"]
        ):
            unique_records[key] = record

    return list(unique_records.values())


def write_parquet(records: list[dict], output_path: Path) -> None:
    """Write records as a Parquet file."""
    if not records:
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(records)
    pq.write_table(
        table,
        output_path,
        compression="snappy",
    )


# ---------------------------------------------------------------------------
# Main processing entry point
# ---------------------------------------------------------------------------

def process_air_quality_bronze() -> dict:
    """Transform all available OpenAQ Bronze batches into Silver Parquet."""
    processed_at = datetime.now(timezone.utc)

    print("\n========== AIRSENSE OPENAQ SILVER PROCESSING ==========")

    if not BRONZE_DIR.exists():
        raise FileNotFoundError(f"Bronze directory not found: {BRONZE_DIR}")

    sensor_lookup = build_sensor_lookup()

    if not sensor_lookup:
        raise RuntimeError(
            "No sensor metadata found. Run OpenAQ Bronze ingestion first."
        )

    print(f"Sensor metadata entries: {len(sensor_lookup)}")

    latest_files = discover_bronze_files("openaq_latest")

    if not latest_files:
        raise FileNotFoundError(
            "No openaq_latest Bronze batches found."
        )

    valid_records = []
    rejected_records = []
    raw_measurement_count = 0

    for path in latest_files:
        for bronze_record in read_jsonl(path):
            metadata = bronze_record.get("metadata") or {}
            payload = bronze_record.get("payload")

            if not isinstance(payload, list):
                rejected_records.append(
                    reject_record(
                        "Bronze payload is not a list",
                        {"payload": payload},
                        metadata,
                        processed_at,
                    )
                )
                continue

            for raw in payload:
                raw_measurement_count += 1

                valid, rejected = transform_measurement(
                    raw=raw,
                    bronze_metadata=metadata,
                    sensor_lookup=sensor_lookup,
                    processed_at=processed_at,
                )

                if valid is not None:
                    valid_records.append(valid)

                if rejected is not None:
                    rejected_records.append(rejected)

    before_deduplication = len(valid_records)
    valid_records = deduplicate(valid_records)
    duplicates_removed = before_deduplication - len(valid_records)

    run_id = processed_at.strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]
    output_dir = SILVER_DIR / f"run_id={run_id}"

    observations_path = output_dir / "observations.parquet"
    rejected_path = output_dir / "rejected.parquet"

    write_parquet(valid_records, observations_path)
    write_parquet(rejected_records, rejected_path)

    stale_count = sum(record["is_stale"] for record in valid_records)

    summary = {
        "run_id": run_id,
        "processed_at": processed_at.isoformat(),
        "bronze_batches_processed": len(latest_files),
        "sensor_metadata_entries": len(sensor_lookup),
        "raw_measurements": raw_measurement_count,
        "valid_measurements": len(valid_records),
        "rejected_measurements": len(rejected_records),
        "duplicates_removed": duplicates_removed,
        "stale_observations": stale_count,
        "fresh_observations": len(valid_records) - stale_count,
        "observations_path": str(observations_path),
        "rejected_path": str(rejected_path),
    }

    print("\nProcessing summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")

    print("\nSilver processing completed.")
    print("========================================================\n")

    return summary


if __name__ == "__main__":
    process_air_quality_bronze()
