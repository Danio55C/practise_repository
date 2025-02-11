import mysql.connector
from pymemcache.client.base import Client

connection = mysql.connector.connect(
    user="root", password="root", host="mysql", port="3306", database="userdb")
print("DB connected")

cursor=connection.cursor()
cursor.execute("SELECT * FROM users")
users = cursor.fetchall()
connection.close()


print(users)


############################

client =Client(("memcached", 11211))
client.set("foo","Hello World!!")

print(client.get("foo"))
