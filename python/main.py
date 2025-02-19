import mysql.connector 
from mysql.connector import Error
from datetime import date, datetime


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
        print("Connection to DB successful")    
    except Error as e:
        print(f"Error occcured while trying to connect to DB: '{e}'")

    return connection 

config = {
    "user_name": "root",
    "password": "root",
    "host":"mysql",
    "port":"3306",
    "database_name":"userdb"
}

db_connection = create_mysqlconnection(**config)


