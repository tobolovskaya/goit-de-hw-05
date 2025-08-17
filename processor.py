from confluent_kafka import Consumer, Producer
from configs import kafka_config, topics_configurations
import json

def _base_conf(kc):
    return {
        "bootstrap.servers": ",".join(kc["bootstrap_servers"]),
        "security.protocol": kc["security_protocol"],
        "sasl.mechanism": kc["sasl_mechanism"],
        "sasl.username": kc["username"],
        "sasl.password": kc["password"],
    }

src = topics_configurations["topic_sensors"]["name"]
temp_t = topics_configurations["topic_temp"]["name"]
hum_t  = topics_configurations["topic_hum"]["name"]

c = Consumer({**_base_conf(kafka_config),
              "group.id": "processor_group",
              "auto.offset.reset": "earliest",
              "enable.auto.commit": True})

p = Producer({**_base_conf(kafka_config), "acks": "all"})

c.subscribe([src])
print(f"[processor] {src} → ({temp_t}, {hum_t})")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None: 
            continue
        if msg.error():
            print("❌", msg.error())
            continue
        data = json.loads(msg.value().decode("utf-8"))
        temp = data.get("temperature", 0)
        hum  = data.get("humidity", 0)
        if temp > 40:
            alert = {**data, "type": "TEMP_HIGH", "msg": f"T>{temp}°C > 40"}
            p.produce(temp_t, json.dumps(alert))
            print("TEMP_ALERT:", alert)
        if hum > 80 or hum < 20:
            kind = "HUM_HIGH" if hum > 80 else "HUM_LOW"
            alert = {**data, "type": kind, "msg": f"Humidity {hum}% out of range"}
            p.produce(hum_t, json.dumps(alert))
            print("HUM_ALERT:", alert)
        p.poll(0)
except KeyboardInterrupt:
    pass
finally:
    c.close()
    p.flush()
