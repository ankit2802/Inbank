import mysql.connector
from datetime import datetime, timedelta

# Set up the connection parameters
host = 'localhost'
database = 'inbank'
user = 'developer'
password = 'user_1234'
port = 32001

# Establish the connection to MySQL server
try:
    conn = mysql.connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    print('Connected to MySQL server')
except mysql.connector.Error as e:
    print(f'Error connecting to MySQL server: {e}')

# Create a cursor to interact with the database
cursor = conn.cursor()

# Specify the start and end dates for the date dimension
start_date = datetime(2020, 1, 1)
end_date = datetime(2030, 12, 31)

# Generate a list of dates between the start and end dates
date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Prepare the data for bulk insert
bulk_insert_data = []
for date in date_list:
    date_key = date.strftime("%Y%m%d")
    date_value = date.strftime("%Y-%m-%d")
    year = date.year
    quarter = (date.month - 1) // 3 + 1
    month = date.strftime("%B")
    week = date.strftime("%U")
    day_of_week = date.strftime("%A")
    day_of_month = date.day

    # Append the data for bulk insert
    bulk_insert_data.append((date_key, date_value, year, quarter, month, week, day_of_week, day_of_month))

# Define the INSERT query with placeholders for bulk insert
insert_query = (
    "INSERT INTO CNF.DimDate (date_key, date, year, quarter, month, week, day_of_week, day_of_month) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
)

# Perform the bulk insert
cursor.executemany(insert_query, bulk_insert_data)

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
