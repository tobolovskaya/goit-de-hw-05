# configs.py
USER = "yevheniia"

kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

def make_topics(user: str) -> dict:
    return {
        "topic_sensors": {
            "name": f"building_sensors_{user}",
            "num_partitions": 1,
            "replication_factor": 1,
        },
        "topic_temp": {
            "name": f"temperature_alerts_{user}",
            "num_partitions": 1,
            "replication_factor": 1,
        },
        "topic_hum": {
            "name": f"humidity_alerts_{user}",
            "num_partitions": 1,
            "replication_factor": 1,
        },
    }

topics_configurations = make_topics(USER)
