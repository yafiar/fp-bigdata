from kafka import KafkaProducer
import requests,json
from config import *
producer=KafkaProducer(bootstrap_servers=KAFKA_SERVER,value_serializer=lambda v: json.dumps(v).encode())
r=requests.get('https://api-harilibur.vercel.app/api?year=2025')
producer.send(TOPIC_EVENTS,value={'source':'holiday','data':r.json()})
print('Holiday data sent')
