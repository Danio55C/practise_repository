
from datetime import datetime
import json
import hashlib
import uuid
from loguru import logger
import os
import time
import pandas as pd
import numpy as np
from mysqldb import create_mysql_connection
from kafka_handler import create_kafka_producer, create_kafka_consumer
from elasticsearch_handler import create_elasticsearch_client
from alert_generator import generate_alert
from memcache_handler import create_memcache_client
from fetching_data_from_cache import fetch_and_save_alerts_memcached
from visualization import create_table_png, create_bar_plot, create_linear_plot
from config import MYSQL_CONFIG




start_time = time.time()


mysql_conn = create_mysql_connection(**MYSQL_CONFIG)
kafka_producer = create_kafka_producer()
es_client = create_elasticsearch_client()


cursor = mysql_conn.cursor()


Topic_Name= "alert"
Group_id = "alerts"

# insert alerts sql querry
insert_alert_query = ("INSERT INTO alerts "
                      "(AlertName, SeverityLevel, Timestamp, Message) "
                      "VALUES (%(alert_name)s, %(severity_level)s, %(timestamp)s, %(message)s)")

# **Generate random alerts and send them to mysql kafka and elasticsearch**
for x in range(20):
    alert = generate_alert()
    kafka_producer.send(Topic_Name, value=alert["message"])
    cursor.execute(insert_alert_query, alert)
    es_client.index(index="alerts", id=str(uuid.uuid4()), document=alert)

kafka_producer.flush()
mysql_conn.commit()
logger.info("Data inserted successfully\n")


# **Consuming messages kafka**
kafka_consumer = create_kafka_consumer(Topic_Name,Group_id)

# for message_alerts in consumer:
#     logger.error(f"Consumer received message: {message_alerts.value}\n")


# **Retrieve related data form MYSQL or Memcached**
retriving_related_data_process_start = time.time()

fetch_and_save_alerts_memcached(3, cursor)

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

mysql_conn.commit()

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

mysql_conn.commit()

alerts_processing_end = time.time()
logger.info(f"Processing alerts took {alerts_processing_end - alerts_processing_start:.4f} seconds.")

# **Sending enriched data back to kafka and elasticsearch**

cursor.execute("SELECT * FROM alerts")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
enriched_data = [dict(zip(columns, row)) for row in rows]

logger.debug(enriched_data)


for alert in enriched_data:
    kafka_producer.send(Topic_Name, value=alert["Message"])
    es_client.index(index="alerts", id=str(uuid.uuid4()), document=alert)
    if alert["SeverityLevel"] == "Warning":
        logger.warning(f"{alert['Message']}\n")
    if alert["SeverityLevel"] == "Error":
        logger.error(f"{alert['Message']}\n")
    if alert["SeverityLevel"] == "Critical Error":
        logger.critical(f"{alert['Message']}\n")      

kafka_producer.flush()
logger.info("\nData enriched\n")


# **Exploring alert data making plots**


create_table_png(enriched_data)
create_bar_plot(enriched_data)
create_linear_plot(enriched_data)


# **closing connections**
kafka_producer.close()
kafka_consumer.close()
es_client.close()

cursor.close()
mysql_conn.close()

logger.info("End of the script")
end_time = time.time()
logger.info(f"\nTotal execution time: {end_time - start_time:.4f} seconds.")








