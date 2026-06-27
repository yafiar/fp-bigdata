from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

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
        name="bmkg-raw",
        num_partitions=1,
        replication_factor=1
    )

]

try:

    admin_client.create_topics(
        new_topics=topics
    )

    print("Topic berhasil dibuat")

except TopicAlreadyExistsError:

    print("Topic sudah ada")

except Exception as e:

    print("Error:", e)

finally:

    admin_client.close()