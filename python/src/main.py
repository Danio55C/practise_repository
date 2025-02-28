
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
from elasticsearch import helpers


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
for i in range(1,21):
    alert = generate_alert()
    kafka_producer.send(Topic_Name, value=alert["message"])
    cursor.execute(insert_alert_query, alert)
    es_client.index(index="alerts", id = i, document=alert)

kafka_producer.flush()
mysql_conn.commit()
logger.info("Data inserted successfully\n")

resp=es_client.get(index="alerts", id=3)
logger.info(resp)



# **Consuming messages kafka**
kafka_consumer = create_kafka_consumer(Topic_Name,Group_id)

# for message_alerts in consumer:
#     logger.error(f"Consumer received message: {message_alerts.value}\n")


# **Retrieve related data form MYSQL or Memcached**
retriving_related_data_process_start = time.time()
logger.info(f"Retriving data process start seconds.\n")
fetch_and_save_alerts_memcached(3, cursor)

retriving_related_data_process_end = time.time()
logger.info(f"Retriving data took: {retriving_related_data_process_end - retriving_related_data_process_start:.4f} seconds.\n")


# **Enrichinh data - Adding risk score and hashfield**           ##to do - maybe combine it with fetching data from memcached first
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

logger.info(f"Starting alerts enrichment process: \n")
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
logger.info(f"Processing alerts took {alerts_processing_end - alerts_processing_start:.4f} seconds.\n")

# **Sending enriched data back to kafka and elasticsearch**

cursor.execute("SELECT * FROM alerts")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
enriched_data = [dict(zip(columns, row)) for row in rows]

logger.debug(enriched_data[ :4])


actions_es = []
for alert in enriched_data:
    action = {
        "_op_type": "update",
        "_index": "alerts",
        "_id": alert["Alertid"],
        "doc": alert,
        "doc_as_upsert": True
    }
    actions_es.append(action)

helpers.bulk(es_client , actions_es)


for alert in enriched_data:
    kafka_producer.send(Topic_Name, value=alert["Message"])
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

#**Testing ES Querries**

resp= es_client.mget(index = "alerts", body={"ids": ["3","6"]})
logger.info(resp)

resp= es_client.count(index = "alerts")
count=resp["count"]
logger.info(count)

resp = es_client.search(index="alerts", body={
    "query": {
        "bool": {
            "filter": [  
                {"term": {"SeverityLevel.keyword": "Critical Error"}}
            ]
        }
    }
})
n_hits= resp['hits']['total']['value']
logger.info(f"Found {n_hits} documents in alerts")

retrieved_documets= resp["hits"]["hits"]

resp=es_client.search(index = "alerts", body={
    "query": {
        "bool": {
            "filter": [
                {"range": {"Timestamp": {"gte": "now-7d/d", "lte": "now/d"}}},  
                {"terms": {"SeverityLevel.keyword": ["Critical Error"]}}  
            ]
        }
    },
            "aggs": {
                "max_RiskScore": {
                    "max": {
                        "field": "RiskScore"
                    }
                }
            }
        } 
    
)
    
                 
n_hits= resp['hits']['total']['value']
retrieved_documets= resp["hits"]["hits"]
logger.info(retrieved_documets)
max_RiskScore= resp['aggregations']['max_RiskScore']['value']
logger.info(f"MAx Risk: {max_RiskScore}")

# **closing connections**
kafka_producer.close()
kafka_consumer.close()
es_client.close()

cursor.close()
mysql_conn.close()

logger.info("End of the script\n")
end_time = time.time()
logger.info(f"Total execution time: {end_time - start_time:.4f} seconds.")








