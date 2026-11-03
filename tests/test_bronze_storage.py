
import json

from ingestion.common.storage import write_bronze


def test_bronze_storage():

    sample_payload = {
        "latitude": -1.30,
        "longitude": 36.82,
        "current": {
            "temperature_2m": 22.1,
            "relative_humidity_2m": 61,
        },
    }

    output_path = write_bronze(
        source="open_meteo",
        city_id="nairobi",
        payload=sample_payload,
    )

    assert output_path.exists()

    with output_path.open("r", encoding="utf-8") as file:
        stored_record = json.loads(file.readline())

    assert stored_record["metadata"]["source"] == "open_meteo"

    assert stored_record["metadata"]["city_id"] == "nairobi"

    assert stored_record["payload"] == sample_payload

    print("\nBronze storage test passed!")


if __name__ == "__main__":
    test_bronze_storage()
