import mysql.connector

connection = mysql.connector.connect(
    user="root", password="root", host="mysql", port="3306", database="userdb")
print("DB connected")

cursor=connection.cursor()
cursor.execute("SELECT * FROM users")
users = cursor.fetchall()
connection.close()


print(users)