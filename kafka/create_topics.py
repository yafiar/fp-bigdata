from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092"
)

topics = [
    NewTopic(
        name="suroboyo-bus-live",
        num_partitions=1,
        replication_factor=1
    ),
    NewTopic(
        name="holiday-data",
        num_partitions=1,
        replication_factor=1
    ),
    NewTopic(
        name="events-surabaya",
        num_partitions=1,
        replication_factor=1
    )
]

admin_client.create_topics(new_topics=topics)
print("Topic berhasil dibuat")