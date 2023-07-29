from datetime import datetime

import mysql.connector
import pandas as pd


def read_data_from_table(table_name, conn):
    # Get the current month and year
    current_month = datetime.now().month
    current_year = datetime.now().year

    # Prepare the query to select data for the current month
    select_query = f'''
                SELECT *
                FROM DWLoad.NetflixSubscription
                WHERE MONTH(RecordDate) = {current_month}
                AND YEAR(RecordDate) = {current_year}
            '''
    df = pd.read_sql_query(select_query, conn)

    return df


def insert_data_DimSubscription(conn,source_data_df):

    # column name you want to get distinct values from
    selected_column = 'SubscriptionType'
    distinct_values = source_data_df[selected_column].drop_duplicates()

    cursor = conn.cursor()
    table_name = "CNF.DimSubscriptionType"
    column_name = "SubscriptionType"

    # Generate the INSERT INTO query with placeholders for the values while ignoring the existing values
    insert_query = f"INSERT IGNORE INTO {table_name} ({column_name}) VALUES (%s);"

    # Insert each distinct value into the table
    for value in distinct_values:
        cursor.execute(insert_query, (value,))
    conn.commit()

def insert_data_DimDevice(conn,source_data_df):

    # column name you want to get distinct values from
    selected_column = 'Device'
    distinct_values = source_data_df[selected_column].drop_duplicates()

    cursor = conn.cursor()
    table_name = "CNF.DimDeviceType"
    column_name = "DeviceType"

    # Generate the INSERT INTO query with placeholders for the values while ignoring the existing values
    insert_query = f"INSERT IGNORE INTO {table_name} ({column_name}) VALUES (%s);"

    # Insert each distinct value into the table
    for value in distinct_values:
        cursor.execute(insert_query, (value,))
    conn.commit()

if __name__ == "__main__":
    # connection parameters
    db_config = {
        "host": "localhost",
        "user": "developer",
        "password": "user_1234",
        "database": "inbank",
        "port": 32001
    }

    # name of the source table
    table_name = "NetflixSubscription"

    conn = mysql.connector.connect(**db_config)

    # Read data from the table into a DataFrame
    source_data_df = read_data_from_table(table_name, conn)
    insert_data_DimSubscription(conn,source_data_df)
    insert_data_DimDevice(conn, source_data_df)

    conn.close()
