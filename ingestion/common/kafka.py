import json
from kafka import KafkaProducer

def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        acks="all",
        retries=5,
        linger_ms=50,
        request_timeout_ms=30000,
    )

def publish(producer: KafkaProducer, topic: str, key: str, event: dict) -> None:
    future = producer.send(topic, key=key, value=event)
    metadata = future.get(timeout=30)
    print(f"Published topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}")
