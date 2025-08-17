from confluent_kafka import Consumer
from configs import kafka_config, topics_configurations
import json

def _conf(kc):
    return {
        "bootstrap.servers": ",".join(kc["bootstrap_servers"]),
        "security.protocol": kc["security_protocol"],
        "sasl.mechanism": kc["sasl_mechanism"],
        "sasl.username": kc["username"],
        "sasl.password": kc["password"],
        "group.id": "alerts_reader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

topics = [topics_configurations["topic_temp"]["name"],
          topics_configurations["topic_hum"]["name"]]

c = Consumer(_conf(kafka_config))
c.subscribe(topics)
print("[alerts] listening:", topics)

try:
    while True:
        msg = c.poll(1.0)
        if msg is None: 
            continue
        if msg.error():
            print("❌", msg.error())
            continue
        print("🚨", msg.topic(), json.loads(msg.value().decode("utf-8")))
except KeyboardInterrupt:
    pass
finally:
    c.close()
