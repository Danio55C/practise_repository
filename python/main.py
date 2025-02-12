import mysql.connector
from pymemcache.client.base import Client
from elasticsearch import Elasticsearch
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer




connection = mysql.connector.connect(
    user="root", password="root", host="mysql", port="3306", database="userdb")
print("DB connected")

cursor=connection.cursor()
cursor.execute("SELECT * FROM users")
users = cursor.fetchall()
connection.close()
print(users)


############################

client =Client(("memcached", 11211))
client.set("foo","Hello World!!")

print(client.get("foo"))


############################


client = Elasticsearch("http://elasticsearch:9200")

doc = {
    "author": "daniel",
    "text": "Elasticsearch:Hello World",
    "timestamp": datetime.now(),
}
resp = client.index(index="test-index", id=1, document=doc)
print(resp["result"])

resp = client.get(index="test-index", id=1)
print(resp["_source"])

client.indices.refresh(index="test-index")

resp = client.search(index="test-index", query={"match_all": {}})
print("Got {} hits:".format(resp["hits"]["total"]["value"]))
for hit in resp["hits"]["hits"]:
    print("{timestamp} {author} {text}".format(**hit["_source"]))
    print(" ")

###########################################################


producer = KafkaProducer(bootstrap_servers="kafka:9092")       ####producer
producer.send("test-topic", b"Kafka - Hello from Python!")
producer.flush()

consumer = KafkaConsumer("test-topic", bootstrap_servers="kafka:9092",auto_offset_reset="earliest")   ####consumer
for message in consumer:
    print(f"Kafka - Received message: {message.value}") 
    break 

###########################################################



