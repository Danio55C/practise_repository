from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_CONFIG
from logger import logger


def create_elasticsearch_client():
    try:
        client_es = Elasticsearch(ELASTICSEARCH_CONFIG["host"])
        logger.success("Elsticsearch - Connection to DB successful")
        return client_es
    except Exception  as e:
        logger.error(f"ElasticSearch - Error has ocured while trying to connect to DB: '{e}'")
        return None




