import os


MYSQL_CONFIG = {
    "user_name": "root",
    "password": "root",
    "host": "mysql",
    "port": "3306",
    "database_name": "userdb"
}

KAFKA_CONFIG = {
    "bootstrap_servers": "kafka:9092"
}

ELASTICSEARCH_CONFIG = {
    "host": "http://elasticsearch:9200"
}

MEMCACHED_CONFIG = {
    "host": "memcached",
    "port": 11211
}


LOG_FILE_PATH = "/usr/app/logs/file_logs.log"
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)