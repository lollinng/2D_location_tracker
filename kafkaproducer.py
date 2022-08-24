import json
import requests
from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer( 
    bootstrap_servers = ['localhost:9092'],
    value_serializer = lambda x: json.dumps(x).encode('utf-8')
)

s = requests.Session()
s.headers={"User-Agent": "Opera/9.80 (X11; Linux x86_64; U; de) Presto/2.2.15 Version/10.00"}
# run 50 queries
for i in range(50):
    res = s.get("http://api.open-notify.org/iss-now.json")
    data = json.loads(res.content.decode('utf-8'))
    print(data)
    # here the we send the json data to topic name - IssLocation
    producer.send('IssLocation',value=data)
    sleep(5)        # run query every 5s
    producer.flush()  # flush all the contents of producer till now