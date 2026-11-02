from ingestion.common.config import get_setting, load_cities
from ingestion.air_quality.client import OpenAQClient
from ingestion.weather.client import fetch_weather

def main():
    cities = load_cities()
    key = get_setting("OPENAQ_API_KEY")
    if not key:
        raise SystemExit("OPENAQ_API_KEY is missing. Add your newly rotated key to .env.")
    client = OpenAQClient(key)
    try:
        for city in cities:
            print(f"\n=== {city['city']} ({city['latitude']}, {city['longitude']}) ===")
            locations = client.get_locations_near_city(
                city["latitude"], city["longitude"],
                int(get_setting("OPENAQ_RADIUS_METERS", "25000")),
                int(get_setting("OPENAQ_PAGE_LIMIT", "100")),
            )
            print(f"OpenAQ locations found: {len(locations)}")
            for loc in locations:
                params = sorted({
                    (sensor.get("parameter") or {}).get("name", "unknown")
                    for sensor in loc.get("sensors", [])
                })
                print(f"- id={loc.get('id')} name={loc.get('name')} parameters={','.join(params)}")
            weather = fetch_weather(city)
            print(f"Open-Meteo current time: {(weather.get('current') or {}).get('time')}")
            print(f"Open-Meteo timezone: {weather.get('timezone')}")
    finally:
        client.close()

if __name__ == "__main__":
    main()
