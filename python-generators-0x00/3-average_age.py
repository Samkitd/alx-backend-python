from seed import connect_to_prodev

def stream_user_ages():
    connection = connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT age FROM user_data")
    
    for row in cursor:  # Loop 1
        yield row['age']
    
    cursor.close()
    connection.close()

def calculate_average_age():
    total = 0
    count = 0

    for age in stream_user_ages():  # Loop 2
        total += age
        count += 1

    if count == 0:
        print("No users found.")
    else:
        avg = total / count
        print(f"Average age of users: {avg:.2f}")