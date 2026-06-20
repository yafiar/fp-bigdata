from kafka import KafkaProducer
import requests,json,time
from config import *
producer=KafkaProducer(bootstrap_servers=KAFKA_SERVER,value_serializer=lambda v: json.dumps(v).encode())
while True:
    try:
        r=requests.get('https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=31.74.01.1001')
        producer.send(TOPIC_BMKG,value=r.json())
        print('BMKG data sent')
    except Exception as e:
        print(e)
    time.sleep(5)
