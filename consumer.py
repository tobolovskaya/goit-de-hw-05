from confluent_kafka import Producer
from configs import kafka_config, topics_configurations
import json, random, time, uuid
from datetime import datetime, timezone

def _producer(kc):
    return Producer({
        "bootstrap.servers": ",".join(kc["bootstrap_servers"]),
        "security.protocol": kc["security_protocol"],
        "sasl.mechanism": kc["sasl_mechanism"],
        "sasl.username": kc["username"],
        "sasl.password": kc["password"],
        "acks": "all",
    })

topic = topics_configurations["topic_sensors"]["name"]
p = _producer(kafka_config)
sensor_id = str(uuid.uuid4())[:8]

def delivery(err, msg):
    if err:
        print("❌ delivery failed:", err)
    else:
        pass  # успішно доставлено

print(f"[producer] sensor {sensor_id} → {topic}")
try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85),
        }
        p.produce(topic, value=json.dumps(data), on_delivery=delivery)
        p.poll(0)     # обробити колбеки
        print("sent:", data)
        time.sleep(1.5)
except KeyboardInterrupt:
    pass
finally:
    p.flush()
