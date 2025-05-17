import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

def stream_users_in_batches(batch_size):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_data")

    try:
        while True:  # Loop 1
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            yield batch
    finally:
        cursor.close()
        conn.close()

def batch_processing(batch_size):
    for batch in stream_users_in_batches(batch_size):  # Loop 2
        for user in batch:                             # Loop 3
            if user['age'] > 25:
                yield user 