from kafka import KafkaProducer
import json,random,time
from datetime import datetime
from config import *
producer=KafkaProducer(bootstrap_servers=KAFKA_SERVER,value_serializer=lambda v: json.dumps(v).encode())
corridors=['Koridor 1','Koridor 2','Koridor 3']
stops=['Blok M','Bundaran HI','Kota','Senen']
while True:
    data={
      'timestamp':datetime.now().isoformat(),
      'corridor':random.choice(corridors),
      'stop':random.choice(stops),
      'passenger_count':random.randint(20,300)
    }
    producer.send(TOPIC_BUS,value=data)
    print(data)
    time.sleep(5)
