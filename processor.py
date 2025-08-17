from confluent_kafka import Consumer, Producer
from configs import kafka_config, topics_configurations
import json

def base_client(conf):
    return {
        "bootstrap.servers": ",".join(conf["bootstrap_servers"]),
        "security.protocol": conf["security_protocol"],
        "sasl.mechanism": conf["sasl_mechanism"],
        "sasl.username": conf["username"],
        "sasl.password": conf["password"],
    }

src_topic = topics_configurations["topic_sensors"]["name"]
temp_topic = topics_configurations["topic_temp"]["name"]
hum_topic  = topics_configurations["topic_hum"]["name"]

c = Consumer({
    **base_client(kafka_config),
    "group.id": "processor_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})
p = Producer({**base_client(kafka_config), "acks": "all"})

c.subscribe([src_topic])
print(f"[processor] {src_topic} → ({temp_topic}, {hum_topic})")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌", msg.error())
            continue

        data = json.loads(msg.value().decode("utf-8"))
        temp = data.get("temperature")
        hum  = data.get("humidity")

        if isinstance(temp, (int, float)) and temp > 40:
            alert = {**data, "type": "TEMP_HIGH", "msg": f"T>{temp}°C > 40"}
            p.produce(temp_topic, json.dumps(alert).encode("utf-8"))
            print("TEMP_ALERT:", alert)

        if isinstance(hum, (int, float)) and (hum > 80 or hum < 20):
            kind = "HUM_HIGH" if hum > 80 else "HUM_LOW"
            alert = {**data, "type": kind, "msg": f"Humidity {hum}% out of range"}
            p.produce(hum_topic, json.dumps(alert).encode("utf-8"))
            print("HUM_ALERT:", alert)

        p.poll(0)
except KeyboardInterrupt:
    pass
finally:
    c.close()
    p.flush()
