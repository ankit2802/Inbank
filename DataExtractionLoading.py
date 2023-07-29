import glob
import os

import mysql.connector
from datetime import datetime, timedelta

# Set up the connection parameters
import pandas as pd

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

directory_netflix_data_input = 'directory_netflix_data_input/'

# Find the latest CSV file in the directory
netflix_data_file = max(glob.glob(os.path.join(directory_netflix_data_input, '*.csv')), key=os.path.getctime)

# Read the CSV file into a DataFrame
df1 = pd.read_csv(netflix_data_file)

# Add the 'RecordDate' column to the DataFrame with current date
df1['RecordDate'] = datetime.now().date()

# Convert the DataFrame to a list of tuples for insertion
data = [tuple(row) for row in df1.values]

# Prepare the query for inserting data
insert_query = '''
    INSERT INTO DWLoad.NetflixSubscription (
        UserID, SubscriptionType, MonthlyRevenue, JoinDate,
        LastPaymentDate, Country, Age, Gender, Device,
        PlanDuration, ActiveProfiles, HouseholdProfileInd,
        MoviesWatched, SeriesWatched, RecordDate
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Execute the insert query with the data
cursor.executemany(insert_query, data)

conn.commit()
conn.close()