from ingestion.common.config import get_setting, load_cities
from ingestion.common.kafka import build_producer, publish
from ingestion.weather.client import fetch_weather, normalize_current_weather, normalize_hourly_forecast

def main():
    bootstrap = get_setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = get_setting("KAFKA_WEATHER_TOPIC", "airsense.weather.raw")
    producer = build_producer(bootstrap)
    try:
        for city in load_cities():
            print(f"Fetching weather for {city['city']}...")
            payload = fetch_weather(city, float(get_setting("REQUEST_TIMEOUT_SECONDS", "30")))
            current = normalize_current_weather(payload, city)
            publish(producer, topic, f"{city['city_id']}:current", current)
            forecasts = normalize_hourly_forecast(payload, city)
            for event in forecasts:
                publish(producer, topic, f"{city['city_id']}:forecast:{event['forecast_for']}", event)
            print(f"Published current conditions and {len(forecasts)} hourly forecast records.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
