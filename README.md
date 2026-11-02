# AirSense — Real-time air quality platform for East Africa

AirSense ingests measured air-quality observations and weather data, processes streaming and batch events, serves live dashboards, forecasts PM2.5 24 hours ahead, and detects anomalous pollution events.

## Phase 1: ingestion

- OpenAQ v3 supplies measured pollutant readings.
- Open-Meteo supplies current weather and hourly weather forecasts.
- Python normalizes records and publishes JSON events to Kafka.

**Important:** OpenAQ's latest-measurement records identify a sensor through `sensorsId`. AirSense maps that ID to the sensor metadata returned with the location to obtain the pollutant name and unit. A missing source timestamp is rejected rather than replaced with the ingestion time.

## Prerequisites

- Python 3.11+
- Docker Desktop
- A valid OpenAQ API key

## Setup (macOS)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
```

Add your newly rotated OpenAQ key to `.env`. Never put a real key in `.env.example`.

## Source audit

Run before starting Kafka:

```bash
python -m scripts.audit_sources
```

## Start Kafka and run ingestion

```bash
docker compose config
docker compose up -d
docker compose ps

python -m ingestion.weather.run
python -m ingestion.air_quality.run
```

Kafka topics:
- `airsense.weather.raw`
- `airsense.air-quality.raw`

The initial producers are one-shot validation jobs. We will add scheduled polling, backoff, deduplication and dead-letter handling after validating source coverage and event quality.

## Security and Git

The supplied archive previously included credentials in `.env.example` and in Git history. This cleaned project archive excludes `.git` and all virtual environments. Rotate any key that was present in the old archive. Initialize a fresh Git history only after confirming no secrets remain; do not push the old history unchanged.
