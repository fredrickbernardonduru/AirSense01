
"""
AirSense Bronze Storage

Responsibilities:
- Preserve original API responses.
- Add ingestion metadata.
- Store data as JSON Lines (JSONL).
- Organize files by source, city and ingestion date.
"""

import json
import re

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


# Project root: the directory containing ingestion/
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Bronze storage directory
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"


def _safe_path_component(value: str) -> str:
    """
    Make a string safe to use as a directory name.
    """
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "_", str(value))

    if not cleaned:
        raise ValueError("Path component cannot be empty.")

    return cleaned.lower()


def write_bronze(
    source: str,
    city_id: str,
    payload: Any,
) -> Path:
    """
    Save an original API response to Bronze storage.

    Each API response is stored as one JSONL record.

    Args:
        source: Data source, e.g. openaq or open_meteo.
        city_id: City identifier, e.g. nairobi.
        payload: Original JSON response from the API.

    Returns:
        Path to the file containing the stored response.
    """

    if payload is None:
        raise ValueError("Cannot store an empty payload.")

    # Capture ingestion time in UTC.
    ingested_at = datetime.now(timezone.utc)

    # Partition directories.
    source_name = _safe_path_component(source)
    city_name = _safe_path_component(city_id)
    ingestion_date = ingested_at.strftime("%Y-%m-%d")

    output_dir = (
        BRONZE_DIR
        / source_name
        / f"city_id={city_name}"
        / f"ingest_date={ingestion_date}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate a unique file for this ingestion batch.
    filename = (
        f"batch_{ingested_at.strftime('%H%M%S_%f')}"
        f"_{uuid4().hex[:8]}.jsonl"
    )

    output_path = output_dir / filename

    # Wrap the untouched API response with ingestion metadata.
    record = {
        "metadata": {
            "source": source_name,
            "city_id": city_name,
            "ingested_at": ingested_at.isoformat(),
            "record_id": str(uuid4()),
        },
        "payload": payload,
    }

    # Write one JSON object per line.
    with output_path.open("w", encoding="utf-8") as file:
        file.write(
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        file.write("\n")

    print(f"[BRONZE] Stored {source_name} data: {output_path}")

    return output_path
