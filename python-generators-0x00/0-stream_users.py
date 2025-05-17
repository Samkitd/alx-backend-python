import mysql.connector
from dotenv import load_dotenv
import os
from itertools import islice

load_dotenv()


def stream_users():
    connection = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_data")

    for row in cursor:
        yield row

    cursor.close()
    connection.close()

# if __name__ == "__main__":
#     for user in islice(stream_users(), 20):
#         print(user)