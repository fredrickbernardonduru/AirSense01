from pathlib import Path
import os

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def get_setting(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def load_cities() -> list[dict]:
    path = PROJECT_ROOT / "config" / "cities.yaml"
    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}
    cities = [
        city for city in config.get("cities", [])
        if city.get("enabled", True)
    ]
    if not cities:
        raise RuntimeError(f"No enabled cities configured in {path}")
    return cities
