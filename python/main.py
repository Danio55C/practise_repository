import mysql.connector 
from mysql.connector import Error
from datetime import date, datetime
import random
import json
from kafka import KafkaProducer, KafkaConsumer


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


# data_alert1 = {
#   'alert_name': random.choice(alert_names),
#   'severity_level': severity_levels[1],
#   'timestamp': today,
#   'Message': f"An {severity_levels[1]} level error has ocured: {alert_name[1]} on {str(today)}"
# }

for x in range(6):
    data_alert1 = generate_alert()
    cursor.execute(insert_alert_query, data_alert1)


db_connection.commit()
print(f"MySQL - Data inserted successfully")

cursor.execute("SELECT * FROM alerts")

alerts = cursor.fetchall()
print(f"MySQL -alerts: {alerts}")
cursor.close()
db_connection.close()

######################################### KAFKA #################################

Topic_Name = 'alerts'

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",  
    value_serializer=lambda m: json.dumps(m).encode('ascii')  
)

for x in range(6):
    data_alert_kafka = generate_alert()
    producer.send(Topic_Name, value = data_alert_kafka["message"])

producer.flush()

print("\nalerts sent successfully")





