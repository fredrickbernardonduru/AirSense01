
"""
AirSense Air Quality Ingestion

Workflow:
1. Discover OpenAQ monitoring locations.
2. Store the location records in Bronze.
3. Fetch the latest measurements for each location.
4. Store the measurement records in Bronze.
5. Normalize measurements using sensor metadata.
6. Optionally publish normalized events to Kafka.
"""

import time

from ingestion.air_quality.client import (
    PARAMETER_ALIASES,
    OpenAQClient,
    normalize_measurement,
)

from ingestion.common.config import get_setting, load_cities
from ingestion.common.kafka import build_producer, publish
from ingestion.common.storage import write_bronze


def kafka_enabled() -> bool:
    """Check whether Kafka publishing is enabled."""
    return get_setting(
        "ENABLE_KAFKA_PUBLISHING", "false"
    ).strip().lower() in {"true", "1", "yes"}


def main() -> None:
    api_key = get_setting("OPENAQ_API_KEY")

    if not api_key:
        raise ValueError("OPENAQ_API_KEY is not configured.")

    timeout = float(
        get_setting("REQUEST_TIMEOUT_SECONDS", "30")
    )

    radius = int(
        get_setting("OPENAQ_RADIUS_METERS", "25000")
    )

    limit = int(
        get_setting("OPENAQ_PAGE_LIMIT", "100")
    )

    max_locations = int(
        get_setting("OPENAQ_MAX_LOCATIONS", "30")
    )

    request_delay = float(
        get_setting("OPENAQ_REQUEST_DELAY_SECONDS", "0.2")
    )

    producer = None

    if kafka_enabled():
        bootstrap = get_setting(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )

        producer = build_producer(bootstrap)

        topic = get_setting(
            "KAFKA_AIR_QUALITY_TOPIC",
            "airsense.air-quality.raw",
        )

        print("[KAFKA] Publishing enabled.")

    else:
        topic = None
        print("[KAFKA] Publishing disabled. Bronze-only ingestion.")

    client = OpenAQClient(api_key, timeout)

    try:
        for city in load_cities():
            city_id = city["city_id"]

            print(
                f"\nDiscovering OpenAQ locations near "
                f"{city['city']}..."
            )

            try:
                # 1. EXTRACT: discover monitoring locations
                locations = client.get_locations_near_city(
                    city["latitude"],
                    city["longitude"],
                    radius,
                    limit,
                )

                print(
                    f"OpenAQ returned {len(locations)} locations."
                )

                # 2. BRONZE: preserve location records
                write_bronze(
                    source="openaq_locations",
                    city_id=city_id,
                    payload=locations,
                )

            except Exception as exc:
                print(
                    f"[ERROR] Location discovery failed for "
                    f"{city['city']}: {exc}"
                )
                continue

            locations_processed = 0
            measurements_stored = 0
            measurements_normalized = 0

            for location in locations[:max_locations]:
                location_id = location.get("id")

                if not location_id:
                    continue

                # Build sensor metadata index.
                sensor_by_id = {}

                for sensor in location.get("sensors", []):
                    sensor_id = sensor.get("id")

                    parameter_name = (
                        sensor.get("parameter") or {}
                    ).get("name")

                    if (
                        sensor_id is not None
                        and parameter_name
                        and parameter_name.strip().lower()
                        in PARAMETER_ALIASES
                    ):
                        sensor_by_id[sensor_id] = sensor

                if not sensor_by_id:
                    continue

                try:
                    # 3. EXTRACT: fetch latest measurements
                    latest = client.get_latest_for_location(
                        location_id
                    )

                    # 4. BRONZE: preserve measurement records
                    write_bronze(
                        source="openaq_latest",
                        city_id=city_id,
                        payload=latest,
                        extra_metadata={
                            "location_id": location_id,
                            "location_name": location.get("name"),
                        },
                    )

                    locations_processed += 1
                    measurements_stored += len(latest)

                except Exception as exc:
                    print(
                        f"[ERROR] Could not fetch/store location "
                        f"{location_id}: {exc}"
                    )

                    time.sleep(request_delay)
                    continue

                # 5. TRANSFORM: normalize measurements
                for raw in latest:
                    event = normalize_measurement(
                        raw,
                        city,
                        location,
                        sensor_by_id,
                    )

                    if event is None:
                        continue

                    measurements_normalized += 1

                    # 6. OPTIONAL: publish to Kafka
                    if producer is not None:
                        key = (
                            f"{city_id}:{event['sensor_id']}:"
                            f"{event['parameter']}"
                        )

                        publish(
                            producer,
                            topic,
                            key,
                            event,
                        )

                print(
                    f"[OPENAQ] Location {location_id} "
                    f"({location.get('name')}): "
                    f"{len(latest)} raw measurements stored."
                )

                time.sleep(request_delay)

            print(f"\nIngestion summary for {city['city']}:")
            print(f"Locations discovered: {len(locations)}")
            print(f"Locations processed: {locations_processed}")
            print(f"Raw measurements stored: {measurements_stored}")
            print(
                f"Normalized measurements: "
                f"{measurements_normalized}"
            )

            if producer is not None:
                print(
                    "[KAFKA] Normalized events submitted for publishing."
                )

    finally:
        if producer is not None:
            producer.flush()
            producer.close()

        client.close()


if __name__ == "__main__":
    main()
