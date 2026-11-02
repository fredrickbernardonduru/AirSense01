from datetime import datetime, timezone

import httpx

BASE_URL = "https://api.openaq.org/v3"

PARAMETER_ALIASES = {
    "pm25": "pm2_5",
    "pm2.5": "pm2_5",
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "no2": "no2",
    "o3": "o3",
    "co": "co",
    "so2": "so2",
}


class OpenAQClient:
    def __init__(self, api_key: str, timeout_seconds: float = 30.0):
        if not api_key or not api_key.strip():
            raise ValueError(
                "OPENAQ_API_KEY is missing. Add your newly rotated key to the local .env file."
            )
        self.client = httpx.Client(
            base_url=BASE_URL,
            timeout=httpx.Timeout(timeout_seconds),
            headers={
                "X-API-Key": api_key.strip(),
                "User-Agent": "AirSense/0.1",
            },
        )

    def get_locations_near_city(
        self,
        latitude: float,
        longitude: float,
        radius_m: int = 25000,
        limit: int = 100,
    ) -> list[dict]:
        response = self.client.get(
            "/locations",
            params={
                "coordinates": f"{latitude},{longitude}",
                "radius": radius_m,
                "limit": limit,
                "page": 1,
            },
        )
        response.raise_for_status()
        return response.json().get("results", [])

    def get_latest_for_location(self, location_id: int) -> list[dict]:
        response = self.client.get(
            f"/locations/{location_id}/latest",
            params={"limit": 100, "page": 1},
        )
        response.raise_for_status()
        return response.json().get("results", [])

    def close(self) -> None:
        self.client.close()


def canonical_parameter(name: str | None) -> str | None:
    if not name:
        return None
    return PARAMETER_ALIASES.get(name.strip().lower())


def normalize_measurement(
    raw: dict,
    city: dict,
    location: dict,
    sensor_by_id: dict[int, dict],
) -> dict | None:
    # OpenAQ latest records identify the sensor with sensorsId. Parameter
    # names and units are available from the location's sensor metadata.
    sensor_id = raw.get("sensorsId") or raw.get("sensors_id")
    sensor = sensor_by_id.get(sensor_id)
    if sensor is None:
        return None

    parameter = sensor.get("parameter") or {}
    parameter_name = parameter.get("name") or parameter.get("displayName")
    canonical = canonical_parameter(parameter_name)
    if canonical is None:
        return None

    value = raw.get("value")
    if value is None:
        return None

    raw_datetime = raw.get("datetime") or {}
    observed_at = (
        raw_datetime.get("utc")
        if isinstance(raw_datetime, dict)
        else raw_datetime
    )
    if not observed_at:
        # Do not fabricate an observation timestamp; reject malformed source data.
        return None

    coordinates = raw.get("coordinates") or location.get("coordinates") or {}
    location_id = raw.get("locationsId") or raw.get("locations_id") or location.get("id")

    return {
        "event_type": "air_quality_observation",
        "event_id": f"openaq:{sensor_id}:{observed_at}",
        "source": "openaq",
        "city_id": city["city_id"],
        "city": city["city"],
        "country_code": city["country_code"],
        "location_id": location_id,
        "location_name": location.get("name"),
        "sensor_id": sensor_id,
        "parameter": canonical,
        "parameter_original": parameter_name,
        "value": float(value),
        "unit": parameter.get("units"),
        "observed_at": observed_at,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "latitude": coordinates.get("latitude"),
        "longitude": coordinates.get("longitude"),
    }
