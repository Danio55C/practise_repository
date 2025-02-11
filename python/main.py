import mysql.connector


connection = mysql.connector.connect(
    user="root", password="root", host="mysql", port="3306", database="userdb")
print("DB connected")

cursor=connection.cursor()
cursor.execute("SELECT * FROM users")
users = cursor.fetchall()
connection.close()


print(users)
###########################################################
from kafka import KafkaProducer, KafkaConsumer


producer = KafkaProducer(bootstrap_servers="broker:19092")
producer.send("test-topic", b"Hello from Python!")
producer.flush()

consumer = KafkaConsumer("test-topic", bootstrap_servers="broker:19092")
for message in consumer:
    print(f"Received message: {message.value}")


producer.close()
consumer.close()


