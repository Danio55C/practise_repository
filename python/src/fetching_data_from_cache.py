from memcache_handler import create_memcache_client
from loguru import logger
import json


# mysql_conn = create_mysql_connection()
# cursor=mysql_conn.cursor()

def fetch_and_save_alert_based_on_ID_memcached(alert_id, cursor):
    client_memcached = create_memcache_client()
    alert_id = alert_id
    cache_key = f"alerts_{alert_id}"
    sql_alerts_query = "SELECT * FROM alerts WHERE Alertid = %s"
    
    alerts_memcached = client_memcached.get(cache_key)
    
    try:
        if not alerts_memcached:
            logger.info("Data not found in cache, searching in MySQL database...\n") 
            cursor.execute(sql_alerts_query, (alert_id,)) 
            alerts_memcached = cursor.fetchall()
            client_memcached.set(cache_key, alerts_memcached, expire=400)
            return alerts_memcached
        else: 
            logger.info(f"Data found in cache")
            return alerts_memcached
            ##return alerts_memcached
    except Exception as e:
        logger.warning(f"Data not found anywhere: {e}")
        return None



def fetch_and_save_alerts_memcached(cursor):
    client_memcached = create_memcache_client()
    cache_key = f"alerts_all"
    sql_alerts_query = "SELECT * FROM alerts"
    alerts_memcached = client_memcached.get(cache_key) 

    try:
        if not alerts_memcached:
            logger.info("Data not found in cache, searching in MySQL database...\n") 
            cursor.execute(sql_alerts_query) 
            alerts_memcached = cursor.fetchall()
            client_memcached.set(cache_key, alerts_memcached, expire=400)
            return alerts_memcached
        else: 
            logger.info(f"Data found in cache")
            return alerts_memcached
            ##return alerts_memcached
    except Exception as e:
        logger.warning(f"Data not found anywhere: {e}")
        return None       