import mysql.connector 
from mysql.connector import Error
from datetime import datetime
import random
import json
from kafka import KafkaProducer, KafkaConsumer
from elasticsearch import Elasticsearch
from pymemcache.client.base import Client
import hashlib
import uuid
from loguru import logger
import os
import time
import pandas as pd
import matplotlib.pyplot as plt


start_time = time.time()

#mysql Database configuration
my_config = {
    "user_name": "root",
    "password": "root",
    "host": "mysql",
    "port": "3306",
    "database_name": "userdb"
}

#log file and config
log_file_path = "/usr/app/src/logs/file_logs.log"
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

logger.add(log_file_path, rotation="1 week", level="WARNING", format='{{"time": "{time}", "level": "{level}", "message": "{message}"}}')
logger.info("Logger initialized, writing logs to file_logs.log")

#generate alerts 
alert_names = ["Memory Leak", "Network Issue", "Too Many Connections", "Database Connection Lost", "Missing Index Warning", "Inconsistent Data Found"]
severity_levels = [ "Warning", "Error", "Critical Error"]

def generate_alert():
    alert_name = random.choice(alert_names)
    severity_level = random.choice(severity_levels)
    timestamp = datetime.now()
    message = f"An {severity_level} level error has occurred: {alert_name} on {str(timestamp)}"
    
    return {
        "alert_name": alert_name, 
        "severity_level": severity_level, 
        "timestamp": timestamp, 
        "message": message
    }

#mysql connection
def create_mysqlconnection(user_name, password, host, port, database_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            user=user_name, 
            password=password, 
            host=host, 
            port=port, 
            database=database_name
        ) 
        logger.info("MySQL - Connection to DB successful")    
    except Error as e:
        logger.error(f"MySQL - Error occurred while trying to connect to DB: '{e}'")
    return connection 

#kafka producer config and connection
try:
    Topic_Name = 'alerts'
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",  
        value_serializer=lambda m: json.dumps(m).encode('ascii')  
    )
    logger.info("Kafka - Success connectiong to broker")
except Exception as e:
    logger.error(f"Kafka - Error has ocured while connection to broker: {e}")

#elasticsearch config and connection
try:
    client_es = Elasticsearch("http://elasticsearch:9200")
    logger.info("Elsticsearch - Connection to DB successful")
except Error as e:
    logger.error(f"ElasticSearch - Error has ocured while trying to connect to DB: '{e}'")


# insert alerts sql querry
insert_alert_query = ("INSERT INTO alerts "
                      "(AlertName, SeverityLevel, Timestamp, Message) "
                      "VALUES (%(alert_name)s, %(severity_level)s, %(timestamp)s, %(message)s)")


db_connection = create_mysqlconnection(**my_config)
cursor = db_connection.cursor()

# **Generate random alerts and send them to mysql kafka and elasticsearch**
for x in range(6):
    data_alerts = generate_alert()
    producer.send(Topic_Name, value=data_alerts["message"])
    cursor.execute(insert_alert_query, data_alerts)
    client_es.index(index="alerts", id=str(uuid.uuid4()), document=data_alerts)

producer.flush()
db_connection.commit()
logger.info("Data inserted successfully\n")


# **Consuming messages kafka**
consumer = KafkaConsumer(
    Topic_Name,
    group_id="alerts",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda m: json.loads(m.decode('ascii')),
    auto_offset_reset='earliest', enable_auto_commit=False,
    consumer_timeout_ms=5000
)
# for message_alerts in consumer:
#     logger.error(f"Consumer received message: {message_alerts.value}\n")

    
# **Retrieve related data form MYSQL or Memcached**
retriving_related_data_process_start = time.time()

client_memcached = Client(("memcached", 11211))  ##memcached config
alert_id = 3
cache_key = f"alerts_{alert_id}"
alerts_memcached = client_memcached.get(cache_key) 
sql_alerts_query = "SELECT * FROM alerts WHERE Alertid = %s"

if not alerts_memcached:
    logger.info("\nData not found in cache, searching in MySQL database...") 
    cursor.execute(sql_alerts_query, (alert_id,)) 
    alerts_memcached = cursor.fetchall()
    client_memcached.set(cache_key, alerts_memcached, expire=300)
    logger.debug(alerts_memcached)
else: 
    logger.debug(f"\nData found in cache:\n{alerts_memcached}")

retriving_related_data_process_end = time.time()
logger.info(f"Retriving data took: {retriving_related_data_process_end - retriving_related_data_process_start:.4f} seconds.")

# **Enrichinh data - Adding risk score and hashfield**
cursor.execute("SHOW COLUMNS FROM alerts LIKE 'HashField'")
exists = cursor.fetchone()
if not exists:
    cursor.execute("ALTER TABLE alerts ADD COLUMN HashField VARCHAR(100)")

cursor.execute("SHOW COLUMNS FROM alerts LIKE 'RiskScore'")
exists = cursor.fetchone()
if not exists:
    cursor.execute("ALTER TABLE alerts ADD COLUMN RiskScore INT")

db_connection.commit()

cursor.execute("SELECT Alertid, AlertName, SeverityLevel, Timestamp, Message FROM alerts")
alerts_mysql = cursor.fetchall()

severity_mapping = {
    "Warning": 1,
    "Error": 4,
    "Critical": 8
}
logger.info(f"\nStarting alerts enrichment process: ")
alerts_processing_start = time.time()

for alert in alerts_mysql:
    alert_id, alert_name, severity_level, timestamp, message = alert
    cursor.execute("SELECT COUNT(*) FROM alerts WHERE AlertName = %s", (alert_name,))
    past_alerts_count = cursor.fetchone()[0]
    severity_score = severity_mapping.get(severity_level, 1)
    risk_score = severity_score + (past_alerts_count // 2)
    
    alert_data = {
        "alert_name": alert_name,
        "severity_level": severity_level,
        "risk_score": risk_score,  
        "timestamp": str(timestamp),
        "message": message
    }
    
    message = f"An {severity_level} has occurred: {alert_name} and {risk_score} risk score on {str(timestamp)}"
    hash_field = hashlib.sha256(json.dumps(alert_data, sort_keys=True).encode()).hexdigest()
    
    cursor.execute("UPDATE alerts SET HashField = %s, RiskScore = %s, Message = %s WHERE Alertid = %s",
                   (hash_field, risk_score, message, alert_id))  

db_connection.commit()

alerts_processing_end = time.time()
logger.info(f"Processing alerts took {alerts_processing_end - alerts_processing_start:.4f} seconds.")

# **Sending enriched data back to kafka and elasticsearch**
cursor.execute("SELECT * FROM alerts")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
enriched_data = [dict(zip(columns, row)) for row in rows]

print(" ")
logger.debug(enriched_data)


for alert in enriched_data:
    producer.send(Topic_Name, value=alert["Message"])
    client_es.index(index="alerts", id=str(uuid.uuid4()), document=alert)
    if alert["SeverityLevel"] == "Warning":
        logger.warning(f"{alert['Message']}\n")
    if alert["SeverityLevel"] == "Error":
        logger.error(f"{alert['Message']}\n")
    if alert["SeverityLevel"] == "Critical Error":
        logger.critical(f"{alert['Message']}\n")      

producer.flush()
logger.info("\nData enriched\n")


# **Exploring alert data**
enriched_data_df = pd.DataFrame.from_dict(enriched_data)
print(enriched_data_df)

fig, ax = plt.subplots(figsize=(8, 4))
ax.axis('tight')
ax.axis('off')

table = plt.table(cellText=enriched_data_df.values, 
                  colLabels=enriched_data_df.columns, 
                  cellLoc='center', 
                  loc='center')

vizualization_file_path = "/usr/app/src/output/dataframe_output.png"
os.makedirs(os.path.dirname(vizualization_file_path), exist_ok=True)
plt.savefig(vizualization_file_path, dpi=300)




# **closing connections**
producer.close()
consumer.close()
client_es.close()

cursor.close()
db_connection.close()

end_time = time.time()
logger.info("End of the script")
logger.info(f"\nTotal execution time: {end_time - start_time:.4f} seconds.")








