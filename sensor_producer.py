from confluent_kafka import Producer
from configs import kafka_config, topics_configurations
import json, random, time, uuid
from datetime import datetime, timezone

def producer(conf):
    return Producer({
        "bootstrap.servers": ",".join(conf["bootstrap_servers"]),
        "security.protocol": conf["security_protocol"],
        "sasl.mechanism": conf["sasl_mechanism"],
        "sasl.username": conf["username"],
        "sasl.password": conf["password"],
        "acks": "all",
    })

topic = topics_configurations["topic_sensors"]["name"]
p = producer(kafka_config)
sensor_id = str(uuid.uuid4())[:8]

print(f"[producer] sensor {sensor_id} → {topic}")
try:
    while True:
        payload = {
            "sensor_id": sensor_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85),
        }
        p.produce(topic, value=json.dumps(payload).encode("utf-8"))
        p.poll(0)              # обробити колбеки
        print("sent:", payload)
        time.sleep(1.5)
except KeyboardInterrupt:
    pass
finally:
    p.flush()
