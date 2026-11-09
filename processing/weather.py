"""
AirSense Weather Silver Processing

Reads raw Open-Meteo Bronze batches and produces:
1. Current weather observations.
2. Hourly weather forecasts.
3. Rejected records for data-quality investigation.

Important:
- Source timestamps are interpreted using the timezone in each payload.
- All normalized timestamps are stored in UTC.
- Forecast retrievals are preserved as separate forecast runs.
- Reprocessing uses the original Bronze ingestion timestamp.
"""

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common.config import load_cities


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "open_meteo"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "weather"

WEATHER_FIELDS = {
    "temperature": {
        "source": "temperature_2m",
        "unit": "temperature_2m",
    },
    "relative_humidity": {
        "source": "relative_humidity_2m",
        "unit": "relative_humidity_2m",
    },
    "precipitation": {
        "source": "precipitation",
        "unit": "precipitation",
    },
    "wind_speed": {
        "source": "wind_speed_10m",
        "unit": "wind_speed_10m",
    },
    "wind_direction": {
        "source": "wind_direction_10m",
        "unit": "wind_direction_10m",
    },
    "pressure": {
        "source": "pressure_msl",
        "unit": "pressure_msl",
    },
}


def build_city_lookup() -> dict:
    """Build city metadata lookup from the project configuration."""
    return {
        city["city_id"]: city
        for city in load_cities()
    }


def parse_source_timestamp(
    value: str,
    timezone_name: str,
) -> datetime:
    """
    Parse a source timestamp and return an aware UTC datetime.

    Open-Meteo timestamps are local to the requested timezone unless
    an explicit UTC offset is included.
    """
    if not value or not isinstance(value, str):
        raise ValueError("Missing or invalid source timestamp.")

    try:
        local_timezone = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, TypeError) as exc:
        raise ValueError(
            f"Invalid source timezone: {timezone_name}"
        ) from exc

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=local_timezone)

    return parsed.astimezone(timezone.utc)


def parse_metadata_timestamp(value: str) -> datetime:
    """Parse a Bronze metadata timestamp and normalize it to UTC."""
    if not value:
        raise ValueError("Missing Bronze ingestion timestamp.")

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        raise ValueError(
            "Bronze ingestion timestamp must include a timezone."
        )

    return parsed.astimezone(timezone.utc)


def parse_numeric_value(value, field_name: str):
    """Validate a numeric weather value while allowing nulls."""
    if value is None:
        return None

    if isinstance(value, bool):
        raise ValueError(
            f"{field_name} cannot be a boolean."
        )

    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} is not numeric: {value}"
        ) from exc

    if not math.isfinite(result):
        raise ValueError(
            f"{field_name} must be finite: {value}"
        )

    return result


def build_weather_values(
    source_record: dict,
    units: dict,
) -> dict:
    """Extract weather values and their source units."""
    result = {}

    for field_name, definition in WEATHER_FIELDS.items():
        source_field = definition["source"]
        unit_field = definition["unit"]

        result[field_name] = parse_numeric_value(
            source_record.get(source_field),
            source_field,
        )

        result[f"{field_name}_unit"] = units.get(unit_field)

    return result


def make_rejected_record(
    source: str,
    city_id: str | None,
    bronze_ingested_at,
    processed_at: datetime,
    record_type: str,
    rejection_reason: str,
    raw_record: dict,
) -> dict:
    """Create a structured rejected-record entry."""
    return {
        "source": source,
        "city_id": city_id,
        "record_type": record_type,
        "rejection_reason": rejection_reason,
        "raw_record": json.dumps(
            raw_record,
            ensure_ascii=False,
            default=str,
        ),
        "bronze_ingested_at": bronze_ingested_at,
        "processed_at": processed_at,
    }


def normalize_current_weather(
    payload: dict,
    metadata: dict,
    city_lookup: dict,
    processed_at: datetime,
) -> dict:
    """Normalize one current-weather observation."""
    city_id = metadata.get("city_id")
    city = city_lookup.get(city_id)

    if city is None:
        raise ValueError(
            f"City '{city_id}' is missing from config/cities.yaml."
        )

    timezone_name = payload.get("timezone")

    if not timezone_name:
        raise ValueError("Current weather payload has no timezone.")

    current = payload.get("current") or {}

    observed_at_local = current.get("time")

    observed_at = parse_source_timestamp(
        observed_at_local,
        timezone_name,
    )

    bronze_ingested_at = parse_metadata_timestamp(
        metadata.get("ingested_at")
    )

    values = build_weather_values(
        current,
        payload.get("current_units") or {},
    )

    event_id = (
        f"open_meteo:current:{city_id}:"
        f"{observed_at.isoformat()}"
    )

    return {
        "event_id": event_id,
        "event_type": "weather_observation",
        "source": "open_meteo",
        "city_id": city_id,
        "city": city["city"],
        "country_code": city["country_code"],
        "observed_at": observed_at,
        "source_local_time": observed_at_local,
        "source_timezone": timezone_name,
        "retrieved_at": bronze_ingested_at,
        "bronze_record_id": metadata.get("record_id"),
        "processed_at": processed_at,
        "latitude": parse_numeric_value(
            payload.get("latitude"),
            "latitude",
        ),
        "longitude": parse_numeric_value(
            payload.get("longitude"),
            "longitude",
        ),
        **values,
    }


def normalize_hourly_forecasts(
    payload: dict,
    metadata: dict,
    city_lookup: dict,
    processed_at: datetime,
) -> list[dict]:
    """Normalize hourly forecasts while preserving each retrieval run."""
    city_id = metadata.get("city_id")
    city = city_lookup.get(city_id)

    if city is None:
        raise ValueError(
            f"City '{city_id}' is missing from config/cities.yaml."
        )

    timezone_name = payload.get("timezone")

    if not timezone_name:
        raise ValueError("Hourly forecast payload has no timezone.")

    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []

    if not isinstance(times, list) or not times:
        raise ValueError("Hourly forecast contains no timestamps.")

    bronze_ingested_at = parse_metadata_timestamp(
        metadata.get("ingested_at")
    )

    units = payload.get("hourly_units") or {}
    bronze_record_id = metadata.get("record_id")

    forecasts = []

    for index, forecast_local_time in enumerate(times):
        try:
            forecast_for = parse_source_timestamp(
                forecast_local_time,
                timezone_name,
            )

            source_record = {}

            for definition in WEATHER_FIELDS.values():
                source_field = definition["source"]
                values = hourly.get(source_field)

                if not isinstance(values, list):
                    raise ValueError(
                        f"Hourly field '{source_field}' is missing "
                        "or is not an array."
                    )

                if index >= len(values):
                    raise ValueError(
                        f"Hourly field '{source_field}' has no value "
                        f"at index {index}."
                    )

                source_record[source_field] = values[index]

            weather_values = build_weather_values(
                source_record,
                units,
            )

            event_id = (
                f"open_meteo:forecast:{bronze_record_id}:"
                f"{forecast_for.isoformat()}"
            )

            forecasts.append({
                "event_id": event_id,
                "event_type": "weather_forecast",
                "source": "open_meteo",
                "city_id": city_id,
                "city": city["city"],
                "country_code": city["country_code"],
                "forecast_for": forecast_for,
                "source_local_time": forecast_local_time,
                "source_timezone": timezone_name,
                "retrieved_at": bronze_ingested_at,
                "forecast_run_id": bronze_record_id,
                "processed_at": processed_at,
                "latitude": parse_numeric_value(
                    payload.get("latitude"),
                    "latitude",
                ),
                "longitude": parse_numeric_value(
                    payload.get("longitude"),
                    "longitude",
                ),
                **weather_values,
            })

        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"Invalid hourly forecast at index {index} "
                f"({forecast_local_time}): {exc}"
            ) from exc

    return forecasts


CURRENT_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("source", pa.string()),
    ("city_id", pa.string()),
    ("city", pa.string()),
    ("country_code", pa.string()),
    ("observed_at", pa.timestamp("us", tz="UTC")),
    ("source_local_time", pa.string()),
    ("source_timezone", pa.string()),
    ("retrieved_at", pa.timestamp("us", tz="UTC")),
    ("bronze_record_id", pa.string()),
    ("processed_at", pa.timestamp("us", tz="UTC")),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("temperature", pa.float64()),
    ("temperature_unit", pa.string()),
    ("relative_humidity", pa.float64()),
    ("relative_humidity_unit", pa.string()),
    ("precipitation", pa.float64()),
    ("precipitation_unit", pa.string()),
    ("wind_speed", pa.float64()),
    ("wind_speed_unit", pa.string()),
    ("wind_direction", pa.float64()),
    ("wind_direction_unit", pa.string()),
    ("pressure", pa.float64()),
    ("pressure_unit", pa.string()),
])

FORECAST_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("source", pa.string()),
    ("city_id", pa.string()),
    ("city", pa.string()),
    ("country_code", pa.string()),
    ("forecast_for", pa.timestamp("us", tz="UTC")),
    ("source_local_time", pa.string()),
    ("source_timezone", pa.string()),
    ("retrieved_at", pa.timestamp("us", tz="UTC")),
    ("forecast_run_id", pa.string()),
    ("processed_at", pa.timestamp("us", tz="UTC")),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("temperature", pa.float64()),
    ("temperature_unit", pa.string()),
    ("relative_humidity", pa.float64()),
    ("relative_humidity_unit", pa.string()),
    ("precipitation", pa.float64()),
    ("precipitation_unit", pa.string()),
    ("wind_speed", pa.float64()),
    ("wind_speed_unit", pa.string()),
    ("wind_direction", pa.float64()),
    ("wind_direction_unit", pa.string()),
    ("pressure", pa.float64()),
    ("pressure_unit", pa.string()),
])

REJECTED_SCHEMA = pa.schema([
    ("source", pa.string()),
    ("city_id", pa.string()),
    ("record_type", pa.string()),
    ("rejection_reason", pa.string()),
    ("raw_record", pa.string()),
    ("bronze_ingested_at", pa.timestamp("us", tz="UTC")),
    ("processed_at", pa.timestamp("us", tz="UTC")),
])


def write_parquet(
    records: list[dict],
    output_path: Path,
    schema: pa.Schema,
) -> None:
    """Write records to Parquet, including valid empty datasets."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(
        records,
        schema=schema,
    )

    pq.write_table(
        table,
        output_path,
        compression="snappy",
    )


def main() -> None:
    processed_at = datetime.now(timezone.utc)
    run_id = (
        f"{processed_at.strftime('%Y%m%dT%H%M%S')}_"
        f"{uuid4().hex[:8]}"
    )

    output_dir = SILVER_DIR / f"run_id={run_id}"

    print("\n========== AIRSENSE WEATHER SILVER PROCESSING ==========")

    if not BRONZE_DIR.exists():
        raise FileNotFoundError(
            f"Open-Meteo Bronze directory not found: {BRONZE_DIR}"
        )

    bronze_files = sorted(BRONZE_DIR.rglob("*.jsonl"))

    if not bronze_files:
        raise FileNotFoundError(
            "No Open-Meteo Bronze JSONL files found."
        )

    city_lookup = build_city_lookup()

    current_records = []
    forecast_records = []
    rejected_records = []

    bronze_batches_processed = 0
    current_duplicates_removed = 0

    for bronze_file in bronze_files:
        print(f"\n[BRONZE] Reading {bronze_file.name}")

        try:
            with bronze_file.open("r", encoding="utf-8") as file:
                for line_number, line in enumerate(file, start=1):
                    if not line.strip():
                        continue

                    raw_record = json.loads(line)
                    metadata = raw_record.get("metadata") or {}
                    payload = raw_record.get("payload") or {}

                    bronze_batches_processed += 1

                    city_id = metadata.get("city_id")

                    try:
                        current = normalize_current_weather(
                            payload,
                            metadata,
                            city_lookup,
                            processed_at,
                        )

                        forecasts = normalize_hourly_forecasts(
                            payload,
                            metadata,
                            city_lookup,
                            processed_at,
                        )

                        current_records.append(current)
                        forecast_records.extend(forecasts)

                    except (ValueError, TypeError, KeyError) as exc:
                        try:
                            bronze_ingested_at = parse_metadata_timestamp(
                                metadata.get("ingested_at")
                            )
                        except (ValueError, TypeError):
                            bronze_ingested_at = None

                        rejected_records.append(
                            make_rejected_record(
                                source="open_meteo",
                                city_id=city_id,
                                bronze_ingested_at=bronze_ingested_at,
                                processed_at=processed_at,
                                record_type="weather_batch",
                                rejection_reason=str(exc),
                                raw_record={
                                    "file": str(bronze_file),
                                    "line_number": line_number,
                                    "record": raw_record,
                                },
                            )
                        )

                        print(f"[REJECTED] {exc}")

        except (OSError, json.JSONDecodeError) as exc:
            rejected_records.append(
                make_rejected_record(
                    source="open_meteo",
                    city_id=None,
                    bronze_ingested_at=None,
                    processed_at=processed_at,
                    record_type="bronze_file",
                    rejection_reason=str(exc),
                    raw_record={
                        "file": str(bronze_file),
                    },
                )
            )

            print(f"[ERROR] Could not read {bronze_file}: {exc}")

    # Current observations have stable event IDs.
    # If the same observation appears in multiple batches,
    # retain the version from the latest Bronze retrieval.
    current_by_event_id = {}

    for record in current_records:
        event_id = record["event_id"]

        existing = current_by_event_id.get(event_id)

        if existing is not None:
            current_duplicates_removed += 1

            if record["retrieved_at"] > existing["retrieved_at"]:
                current_by_event_id[event_id] = record
        else:
            current_by_event_id[event_id] = record

    current_records = list(current_by_event_id.values())

    current_records.sort(
        key=lambda record: record["observed_at"]
    )

    forecast_records.sort(
        key=lambda record: (
            record["forecast_for"],
            record["retrieved_at"],
        )
    )

    current_path = output_dir / "current_weather.parquet"
    forecast_path = output_dir / "hourly_forecast.parquet"
    rejected_path = output_dir / "rejected.parquet"

    write_parquet(
        current_records,
        current_path,
        CURRENT_SCHEMA,
    )

    write_parquet(
        forecast_records,
        forecast_path,
        FORECAST_SCHEMA,
    )

    write_parquet(
        rejected_records,
        rejected_path,
        REJECTED_SCHEMA,
    )

    print("\nProcessing summary:")
    print(f"  run_id: {run_id}")
    print(f"  processed_at: {processed_at.isoformat()}")
    print(f"  bronze_batches_processed: {bronze_batches_processed}")
    print(f"  current_observations: {len(current_records)}")
    print(f"  current_duplicates_removed: {current_duplicates_removed}")
    print(f"  hourly_forecasts: {len(forecast_records)}")
    print(f"  rejected_records: {len(rejected_records)}")
    print(f"  current_weather_path: {current_path}")
    print(f"  hourly_forecast_path: {forecast_path}")
    print(f"  rejected_path: {rejected_path}")

    print("\nWeather Silver processing completed.")
    print("=========================================================\n")


if __name__ == "__main__":
    main()