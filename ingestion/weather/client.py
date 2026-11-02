from datetime import datetime, timezone

import httpx

BASE_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(city: dict, timeout_seconds: float = 30.0) -> dict:
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": (
            "temperature_2m,relative_humidity_2m,precipitation,"
            "wind_speed_10m,wind_direction_10m,pressure_msl"
        ),
        "hourly": (
            "temperature_2m,relative_humidity_2m,precipitation,"
            "wind_speed_10m,wind_direction_10m,pressure_msl"
        ),
        "forecast_days": 2,
        "timezone": city["timezone"],
        # The normalized field is explicitly in metres per second.
        "wind_speed_unit": "ms",
    }
    response = httpx.get(
        BASE_URL,
        params=params,
        timeout=httpx.Timeout(timeout_seconds),
        headers={"User-Agent": "AirSense/0.1"},
    )
    response.raise_for_status()
    return response.json()


def normalize_current_weather(payload: dict, city: dict) -> dict:
    current = payload.get("current") or {}
    observed_at = current.get("time")
    return {
        "event_type": "weather_observation",
        "event_id": f"open_meteo:current:{city['city_id']}:{observed_at}",
        "source": "open_meteo",
        "city_id": city["city_id"],
        "city": city["city"],
        "country_code": city["country_code"],
        "observed_at": observed_at,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "timezone": payload.get("timezone"),
        "temperature_2m_c": current.get("temperature_2m"),
        "relative_humidity_2m_pct": current.get("relative_humidity_2m"),
        "precipitation_mm": current.get("precipitation"),
        "wind_speed_10m_ms": current.get("wind_speed_10m"),
        "wind_direction_10m_deg": current.get("wind_direction_10m"),
        "pressure_msl_hpa": current.get("pressure_msl"),
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
    }


def normalize_hourly_forecast(payload: dict, city: dict) -> list[dict]:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    issued_at = datetime.now(timezone.utc).isoformat()
    events = []

    for index, forecast_time in enumerate(times):
        def value(name: str):
            values = hourly.get(name) or []
            return values[index] if index < len(values) else None

        events.append({
            "event_type": "weather_forecast",
            "event_id": f"open_meteo:forecast:{city['city_id']}:{forecast_time}",
            "source": "open_meteo",
            "city_id": city["city_id"],
            "city": city["city"],
            "country_code": city["country_code"],
            "forecast_for": forecast_time,
            "issued_at": issued_at,
            "timezone": payload.get("timezone"),
            "temperature_2m_c": value("temperature_2m"),
            "relative_humidity_2m_pct": value("relative_humidity_2m"),
            "precipitation_mm": value("precipitation"),
            "wind_speed_10m_ms": value("wind_speed_10m"),
            "wind_direction_10m_deg": value("wind_direction_10m"),
            "pressure_msl_hpa": value("pressure_msl"),
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
        })
    return events
