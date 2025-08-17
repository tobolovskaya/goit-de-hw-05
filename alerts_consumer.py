from confluent_kafka import Consumer
from configs import kafka_config, topics_configurations
import json

def consumer_conf(conf, group_id):
    return {
        "bootstrap.servers": ",".join(conf["bootstrap_servers"]),
        "security.protocol": conf["security_protocol"],
        "sasl.mechanism": conf["sasl_mechanism"],
        "sasl.username": conf["username"],
        "sasl.password": conf["password"],
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

topics = [
    topics_configurations["topic_temp"]["name"],
    topics_configurations["topic_hum"]["name"],
]

c = Consumer(consumer_conf(kafka_config, "alerts_reader"))
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
