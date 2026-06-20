from kafka.admin import KafkaAdminClient, NewTopic
from config import *
admin=KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
topics=[
NewTopic(name=TOPIC_TRANSJAKARTA,num_partitions=1,replication_factor=1),
NewTopic(name=TOPIC_BMKG,num_partitions=1,replication_factor=1),
NewTopic(name=TOPIC_EVENTS,num_partitions=1,replication_factor=1)
]
try:
    admin.create_topics(topics)
    print('Topics created')
except Exception as e:
    print(e)
