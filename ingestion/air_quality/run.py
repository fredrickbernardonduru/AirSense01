import time

from ingestion.air_quality.client import (
    PARAMETER_ALIASES,
    OpenAQClient,
    normalize_measurement,
)
from ingestion.common.config import get_setting, load_cities
from ingestion.common.kafka import build_producer, publish


def main() -> None:
    api_key = get_setting("OPENAQ_API_KEY")
    timeout = float(get_setting("REQUEST_TIMEOUT_SECONDS", "30"))
    radius = int(get_setting("OPENAQ_RADIUS_METERS", "25000"))
    limit = int(get_setting("OPENAQ_PAGE_LIMIT", "100"))
    max_locations = int(get_setting("OPENAQ_MAX_LOCATIONS", "30"))
    request_delay = float(get_setting("OPENAQ_REQUEST_DELAY_SECONDS", "0.2"))
    bootstrap = get_setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = get_setting("KAFKA_AIR_QUALITY_TOPIC", "airsense.air-quality.raw")

    client = OpenAQClient(api_key, timeout)
    producer = build_producer(bootstrap)
    try:
        for city in load_cities():
            print(f"\nDiscovering OpenAQ locations near {city['city']}...")
            locations = client.get_locations_near_city(
                city["latitude"], city["longitude"], radius, limit
            )
            print(f"OpenAQ returned {len(locations)} locations.")

            for location in locations[:max_locations]:
                location_id = location.get("id")
                if not location_id:
                    continue

                # The location response includes sensor metadata. Index it by
                # sensor ID so each latest measurement gets the right pollutant
                # name and unit.
                sensor_by_id = {}
                for sensor in location.get("sensors", []):
                    sensor_id = sensor.get("id")
                    parameter_name = (sensor.get("parameter") or {}).get("name")
                    if sensor_id is not None and parameter_name:
                        if parameter_name.strip().lower() in PARAMETER_ALIASES:
                            sensor_by_id[sensor_id] = sensor

                if not sensor_by_id:
                    continue

                try:
                    latest = client.get_latest_for_location(location_id)
                except Exception as exc:
                    print(f"Skipping location {location_id}: {exc}")
                    time.sleep(request_delay)
                    continue

                published = 0
                for raw in latest:
                    event = normalize_measurement(
                        raw, city, location, sensor_by_id
                    )
                    if event is None:
                        continue
                    key = (
                        f"{city['city_id']}:{event['sensor_id']}:"
                        f"{event['parameter']}"
                    )
                    publish(producer, topic, key, event)
                    published += 1

                print(
                    f"Location {location_id} ({location.get('name')}): "
                    f"{published} supported latest measurements published."
                )
                time.sleep(request_delay)
    finally:
        producer.flush()
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
