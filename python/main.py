import mysql.connector 
from mysql.connector import Error
from datetime import date, datetime
import random
import json
from kafka import KafkaProducer, KafkaConsumer
from elasticsearch import Elasticsearch
from pymemcache.client.base import Client
import time

def generate_alert():
    alert_name = random.choice(alert_names)
    severity_level = random.choice(severity_levels)
    timestamp = datetime.now()
    message = f"An {severity_level} level error has ocured: {alert_name} on {str(timestamp)}"

    return {
    "alert_name":alert_name, 
    "severity_level":severity_level, 
    "timestamp":timestamp, 
    "message":message
    }

def create_mysqlconnection(user_name, password, host, port, database_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            user=user_name, 
            password=password, 
            host = host, 
            port = port, 
            database=database_name
            ) 
        print("MySQL - Connection to DB successful")    
    except Error as e:
        print(f"MySQL - Error occcured while trying to connect to DB: '{e}'")

    return connection 

config = {
    "user_name": "root",
    "password": "root",
    "host":"mysql",
    "port":"3306",
    "database_name":"userdb"
}

db_connection = create_mysqlconnection(**config)
################

cursor = db_connection.cursor()

insert_alert_query = ("INSERT INTO alerts "
              "(AlertName, SeverityLevel, Timestamp, Message) "
              "VALUES (%(alert_name)s, %(severity_level)s, %(timestamp)s, %(message)s)")

alert_names = ["Memory Leak", "Network Issue","Too Many Connections","Database Connection Lost","Missing Index Warning","Inconsistent Data Found"]
severity_levels = ["Low", "Medium", "High", "Critical"]




# data_alert1 = {
#   'alert_name': random.choice(alert_names),
#   'severity_level': severity_levels[1],
#   'timestamp': today,
#   'Message': f"An {severity_levels[1]} level error has ocured: {alert_name[1]} on {str(today)}"
# }



######################################### #################################

Topic_Name = 'alerts'

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",  
    value_serializer=lambda m: json.dumps(m).encode('ascii')  
)

client = Elasticsearch("http://elasticsearch:9200")

for x in range(6):
    data_alerts = generate_alert()
    producer.send(Topic_Name, value = data_alerts["message"])
    cursor.execute(insert_alert_query, data_alerts)
    client.index(index="alerts", id=x, document = data_alerts)


producer.flush()


db_connection.commit()
print("MySQL - Data inserted successfully")


client.indices.refresh(index="alerts")

resp = client.search(index="alerts", query={"match_all": {}})
print("\n Elastic search: Got {} hits:".format(resp["hits"]["total"]["value"]))
for hit in resp["hits"]["hits"]:
    print("{message}".format(**hit["_source"]))

client.close()


########################


consumer = KafkaConsumer(Topic_Name,
    group_id= "alerts",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda m: json.loads(m.decode('ascii')),
    auto_offset_reset='earliest', enable_auto_commit=False,
    consumer_timeout_ms=5000
)

print("")
for message_alerts in consumer:
    print (f"Consumer received message: {message_alerts.value}")

     

producer.close()
consumer.close() 

######retrieve related data########

client_memcached = Client(("memcached", 11211))
alert_id = 3
cache_key = f"alerts_{alert_id}"
alerts_memcached = client_memcached.get(cache_key) 
sql_alerts_query = "SELECT * FROM alerts WHERE Alertid = %s"

if not alerts_memcached :
    print("\nData not found in cache, searching in mysql database: ") 
    cursor.execute(sql_alerts_query,(alert_id,)) 
    alerts_memcached = cursor.fetchall()
    client_memcached.set(cache_key, alerts_memcached, expire=300)
    print(alerts_memcached)
else: 
    print(f"\nData found in cache:\n{alerts_memcached}")

cursor.close()
db_connection.close()

###########################




print("\nalerts sent successfully")







