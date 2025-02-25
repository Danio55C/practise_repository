from memcache_handler import create_memcache_client
from loguru import logger


# mysql_conn = create_mysql_connection()
# cursor=mysql_conn.cursor()

def fetch_and_save_alerts_memcached(alert_id, cursor):
    client_memcached = create_memcache_client()
    alert_id = alert_id
    cache_key = f"alerts_{alert_id}"
    sql_alerts_query = "SELECT * FROM alerts WHERE Alertid = %s"
    
    alerts_memcached = client_memcached.get(cache_key) 

    if not alerts_memcached:
        logger.info("\nData not found in cache, searching in MySQL database...") 
        cursor.execute(sql_alerts_query, (alert_id,)) 
        alerts_memcached = cursor.fetchall()
        client_memcached.set(cache_key, alerts_memcached, expire=300)
        logger.debug(alerts_memcached)
    else: 
        logger.info(f"\nData found in cache:\n{alerts_memcached}")
    ##return alerts_memcached
    
    