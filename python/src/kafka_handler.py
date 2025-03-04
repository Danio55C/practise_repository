import json
from kafka import KafkaProducer, KafkaConsumer
from config import KAFKA_CONFIG
from logger import logger

def create_kafka_producer():
    try:
        
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],  
            value_serializer=lambda m: json.dumps(m).encode('ascii')  
        )
        logger.success("KafkaProducer - Success connectiong to broker")
        return producer
    except Exception as e:
        logger.error(f"KafkaProducer - Error has ocured while connection to broker: {e}")
        return None
    

def create_kafka_consumer(topic_name, group_id):
    try:
        consumer = KafkaConsumer(
            topic_name,
            group_id=group_id,
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            auto_offset_reset='earliest', enable_auto_commit=False,
            consumer_timeout_ms=5000
        )
        return consumer 
    except Exception as e:
        logger.error(f"KafkaConsumer - Error: {e}")
        return None