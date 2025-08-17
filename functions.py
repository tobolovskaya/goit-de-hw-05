from confluent_kafka.admin import AdminClient, NewTopic

def _client_conf(kc: dict) -> dict:
    return {
        "bootstrap.servers": ",".join(kc["bootstrap_servers"]),
        "security.protocol": kc["security_protocol"],
        "sasl.mechanism": kc["sasl_mechanism"],
        "sasl.username": kc["username"],
        "sasl.password": kc["password"],
    }

def create_topics(kafka_config: dict, topic_configs: dict):
    admin = AdminClient(_client_conf(kafka_config))
    new_topics = []
    for _, cfg in topic_configs.items():
        new_topics.append(NewTopic(cfg["name"], num_partitions=cfg["num_partitions"],
                                   replication_factor=cfg["replication_factor"]))
    # повертається dict futures
    futures = admin.create_topics(new_topics)
    for name, f in futures.items():
        try:
            f.result()
            print(f"✅ Topic created: {name}")
        except Exception as e:
            print(f"⚠️  Topic '{name}': {e}")

def show_topics(kafka_config: dict):
    admin = AdminClient(_client_conf(kafka_config))
    md = admin.list_topics(timeout=10)
    print("📋 Topics:", ", ".join(sorted(md.topics.keys())))
