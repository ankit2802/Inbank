import glob
import os

import mysql.connector
from datetime import datetime, timedelta

# Set up the connection parameters
host = 'localhost'
database = 'weather_feed_dwh'
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

directory_netflix_data_input = 'directory_netflix_data_input/'

# Find the latest CSV file in the directory
latest_city_file = max(glob.glob(os.path.join(directory_netflix_data_input, '*.csv')), key=os.path.getctime)
