import glob
import os

import mysql.connector
from datetime import datetime, timedelta

# Set up the connection parameters
import pandas as pd
import row as row
from IPython.core.display_functions import display

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

# Filter out records with NULL UserId
df1 = df1.dropna(subset=['User ID'])

# Convert the DataFrame to a list of tuples for insertion
data = [
    (
        row['User ID'],
        row['Subscription Type'],
        row['Monthly Revenue'],
        datetime.strptime(row['Join Date'], '%d.%m.%Y').strftime('%Y-%m-%d'),  # Convert to MySQL format
        datetime.strptime(row['Last Payment Date'], '%d.%m.%Y').strftime('%Y-%m-%d'),  # Convert to MySQL format
        row['Country'],
        row['Age'],
        row['Gender'],
        row['Device'],
        row['Plan Duration'],
        row['Active Profiles '],
        row['Household Profile Ind '],
        row['Movies Watched '],
        row['Series Watched'],
        datetime.now().date().strftime('%Y-%m-%d')  # RecordDate with current date in datetime format
    )
    for _, row in df1.iterrows()
]
print(data)
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
try:
    cursor.executemany(insert_query, data)
    conn.commit()
    print("Data inserted successfully ")
except mysql.connector.IntegrityError as e:
    print(f"Error: {e}")
    conn.rollback()

conn.commit()
conn.close()