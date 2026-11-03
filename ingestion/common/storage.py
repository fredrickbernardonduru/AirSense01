
"""
AirSense Bronze Storage

Preserves source payloads with ingestion metadata in JSONL format.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"


def _safe_path_component(value: str) -> str:
    """Convert a value into a safe directory-name component."""
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "_", str(value))

    if not cleaned:
        raise ValueError("Path component cannot be empty.")

    return cleaned.lower()


def write_bronze(
    source: str,
    city_id: str,
    payload: Any,
    extra_metadata: dict[str, Any] | None = None,
) -> Path:
    """
    Persist one source payload as a JSONL record.

    The payload is preserved without cleaning or normalization.
    Extra metadata provides context without changing the payload.
    """

    if payload is None:
        raise ValueError("Cannot store an empty payload.")

    ingested_at = datetime.now(timezone.utc)

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

    filename = (
        f"batch_{ingested_at.strftime('%H%M%S_%f')}"
        f"_{uuid4().hex[:8]}.jsonl"
    )

    output_path = output_dir / filename

    metadata = {
        "source": source_name,
        "city_id": city_name,
        "ingested_at": ingested_at.isoformat(),
        "record_id": str(uuid4()),
    }

    if extra_metadata:
        metadata.update(extra_metadata)

    record = {
        "metadata": metadata,
        "payload": payload,
    }

    with output_path.open("w", encoding="utf-8") as file:
        file.write(
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        file.write("\n")

    print(f"[BRONZE] Stored {source_name}: {output_path}")

    return output_path
