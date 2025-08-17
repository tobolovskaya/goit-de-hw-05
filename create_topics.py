from configs import kafka_config, topics_configurations
from functions import create_topics, show_topics

create_topics(kafka_config, topics_configurations)
show_topics(kafka_config)
