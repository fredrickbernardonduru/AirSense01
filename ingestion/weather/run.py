
"""
AirSense Weather Ingestion

Workflow:
1. Fetch weather data from Open-Meteo.
2. Persist the original response in Bronze.
3. Normalize current weather and hourly forecasts.
4. Optionally publish normalized events to Kafka.
"""

from ingestion.common.config import get_setting, load_cities
from ingestion.common.kafka import build_producer, publish
from ingestion.common.storage import write_bronze

from ingestion.weather.client import (
    fetch_weather,
    normalize_current_weather,
    normalize_hourly_forecast,
)


def kafka_enabled() -> bool:
    """Check whether Kafka publishing is enabled."""
    return get_setting(
        "ENABLE_KAFKA_PUBLISHING", "false"
    ).strip().lower() in {"true", "1", "yes"}


def main() -> None:
    timeout = float(
        get_setting("REQUEST_TIMEOUT_SECONDS", "30")
    )

    producer = None

    if kafka_enabled():
        bootstrap = get_setting(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        producer = build_producer(bootstrap)
        topic = get_setting(
            "KAFKA_WEATHER_TOPIC", "airsense.weather.raw"
        )
        print("[KAFKA] Publishing enabled.")
    else:
        topic = None
        print("[KAFKA] Publishing disabled. Bronze-only ingestion.")

    try:
        for city in load_cities():
            city_id = city["city_id"]

            print(f"\nFetching weather for {city['city']}...")

            try:
                # 1. EXTRACT
                payload = fetch_weather(city, timeout)

                # 2. BRONZE: preserve the original API response
                write_bronze(
                    source="open_meteo",
                    city_id=city_id,
                    payload=payload,
                )

                # 3. TRANSFORM: create normalized records
                current = normalize_current_weather(payload, city)

                forecasts = normalize_hourly_forecast(
                    payload, city
                )

                print(
                    f"[TRANSFORM] Created 1 current record "
                    f"and {len(forecasts)} forecast records."
                )

                # 4. OPTIONAL: publish normalized events
                if producer is not None:
                    publish(
                        producer,
                        topic,
                        f"{city_id}:current",
                        current,
                    )

                    for event in forecasts:
                        publish(
                            producer,
                            topic,
                            f"{city_id}:forecast:{event['forecast_for']}",
                            event,
                        )

                    print(
                        f"[KAFKA] Published current weather "
                        f"and {len(forecasts)} forecasts."
                    )

            except Exception as exc:
                print(
                    f"[ERROR] Weather ingestion failed for "
                    f"{city['city']}: {exc}"
                )
                continue

    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
