import mysql.connector
import uuid
import csv
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Create the database if it doesn't exist
def create_database(connection):
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
    cursor.close()

# Connect to the ALX_prodev database
def connect_to_prodev():
    connection = connect_db()
    connection.database = 'ALX_prodev'
    return connection

# Create table if it doesn't exist
def create_table(connection):
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_data (
            user_id CHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age DECIMAL NOT NULL
        )
    """)
    cursor.close()

# Insert data from CSV into the table
def insert_data(connection, data):
    cursor = connection.cursor()
    for row in data:
        cursor.execute("""
            INSERT INTO user_data (user_id, name, email, age)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE user_id=user_id
        """, (str(uuid.uuid4()), row['name'], row['email'], row['age']))
    connection.commit()
    cursor.close()

# Read CSV and return data as list of dicts
def read_csv(filename):
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]

def main():
    connection = connect_db()
    create_database(connection)
    
    connection = connect_to_prodev()

    create_table(connection)
    
    data = read_csv('user_data.csv')
    insert_data(connection, data)

    print("Data inserted successfully.")
    connection.close()

if __name__ == "__main__":
    main()