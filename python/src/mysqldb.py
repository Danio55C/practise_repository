import mysql.connector
from mysql.connector import Error

from logger import logger



def create_mysql_connection(user_name, password, host, port, database_name):
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
        return connection 
    except Error as e:
        logger.error(f"MySQL - Error occurred while trying to connect to DB: '{e}'")
        return None