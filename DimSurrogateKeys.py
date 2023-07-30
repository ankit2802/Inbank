#SCD Type 2 has been implementted for the Dimensions
from datetime import datetime, timedelta

import mysql.connector
import pandas as pd
from IPython.core.display_functions import display

def read_data_from_stage_dim_subscription(conn):
    select_subscription_query = "SELECT * FROM DWStage.DimSubscription"
    try:
        dataframe_for_stage_dim_subscription = pd.read_sql_query(select_subscription_query, conn)
        return dataframe_for_stage_dim_subscription
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def read_data_from_stage_dim_customers(conn):
    select_customers_query = "SELECT * FROM DWStage.DimCustomers"
    try:
        dataframe_for_stage_dim_customers = pd.read_sql_query(select_customers_query, conn)
        return dataframe_for_stage_dim_customers
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def insert_data_to_dwskey_dim_subscription(conn, dataframe_for_stage_dim_subscription):
    cursor = conn.cursor()
    insert_query = (
        "INSERT INTO DWSkey.DimSubscription (CustomerID, SubscriptionDuration, SubscriptionType, "
        "JoiningDate, Device, RecordDate, ValidFromDate, ValidToDate, IsCurrent) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )
    update_query = (
        "UPDATE DWSkey.DimSubscription "
        "SET ValidToDate = %s, IsCurrent = %s " 
        "WHERE CustomerID = %s AND IsCurrent IS NULL;"
    )

    # Create a list of tuples containing data for bulk insert
    data = []
    for _, row in dataframe_for_stage_dim_subscription.iterrows():
        customer_id = row['CustomerID']
        subscription_duration = row['SubscriptionDuration']
        subscription_type = row['SubscriptionType']
        joining_date = row['JoiningDate']
        device = row['Device']
        record_date = row['RecordDate']
        valid_from_date = joining_date
        valid_to_date = '9999-12-31'  # Set to default end date (e.g., '9999-12-31') for new records
        is_current = '1'  # Set to default value (e.g., 'TRUE') for new records

        cursor.execute(
            "SELECT * FROM DWSkey.DimSubscription WHERE CustomerID = %s AND IsCurrent IS NULL;",
            (customer_id,)
        )
        existing_record = cursor.fetchone()

        if existing_record:
            # Check if any of the fields have different values
            existing_values = existing_record[1:-3]  # Exclude RecordKey, ValidToDate, and IsCurrent columns
            new_values = (subscription_duration, subscription_type, joining_date, device)
            if existing_values != new_values:
                # Update the existing record with ValidToDate and IsCurrent
                record_key = existing_record[0]
                cursor.execute(update_query, (datetime.now() - timedelta(days=1), '0', record_key))
                # Insert a new record with valid_from_date as current date and default values for ValidToDate and IsCurrent
                values = (customer_id, subscription_duration, subscription_type, joining_date, device, record_date,
                          datetime.now(), valid_to_date, is_current)  # Set ValidFromDate to current date
                data.append(values)
            else:
                # No changes in the fields, keep the existing record as is
                pass
        else:
            # Insert a new record with default values for ValidToDate and IsCurrent
            values = (customer_id, subscription_duration, subscription_type, joining_date, device, record_date,
                      valid_from_date, valid_to_date, is_current)  # Set ValidFromDate to current date
            data.append(values)

    # Execute the bulk insert query with the data
    try:
        cursor.executemany(insert_query, data)
        conn.commit()
        print("Data inserted successfully for DWSkey.DimSubscription")
    except mysql.connector.IntegrityError as e:
        print(f"Error: {e}")
        conn.rollback()

    conn.commit()


def insert_data_to_dwskey_dim_customers(conn, dataframe_for_stage_dim_customers):
    cursor = conn.cursor()
    insert_query = (
        "INSERT INTO DWSkey.DimCustomers (CustomerID, Country, Gender, YearOfBirth, JoiningDate, RecordDate, "
        "ValidFromDate, ValidToDate, IsCurrent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )
    update_query = (
        "UPDATE DWSkey.DimCustomers "
        "SET ValidToDate = %s, IsCurrent = %s "
        "WHERE CustomerID = %s AND IsCurrent IS NULL;"
    )

    # Create a list of tuples containing data for bulk insert
    data = []
    for _, row in dataframe_for_stage_dim_customers.iterrows():
        customer_id = row['CustomerID']
        country = row['Country']
        gender = row['Gender']
        year_of_birth = row['YearOfBirth']
        joining_date = row['JoiningDate']
        record_date = row['RecordDate']
        valid_from_date = joining_date
        valid_to_date = '9999-12-31'  # Use the filled value from the DataFrame
        is_current = 1  # Use the filled value from the DataFrame

        cursor.execute(
            "SELECT * FROM DWSkey.DimCustomers WHERE CustomerID = %s AND IsCurrent IS NULL;",
            (customer_id,)
        )
        existing_record = cursor.fetchone()

        if existing_record:
            # Check if any of the fields have different values
            existing_values = existing_record[1:-4]  # Exclude RecordKey, ValidToDate, and IsCurrent columns
            new_values = (country, gender, year_of_birth, joining_date)
            if existing_values != new_values:
                # Update the existing record with ValidToDate and IsCurrent
                record_key = existing_record[0]
                cursor.execute(update_query, (datetime.now() - timedelta(days=1), '0', record_key))
                # Insert a new record with valid_from_date as current date and default values for ValidToDate and IsCurrent
                values = (customer_id, country, gender, year_of_birth, joining_date, record_date,
                          datetime.now(), valid_to_date, is_current)  # Set ValidFromDate to current date
                data.append(values)
            else:
                # No changes in the fields, keep the existing record as is
                pass
        else:
            # Insert a new record with default values for ValidToDate and IsCurrent
            values = (customer_id, country, gender, year_of_birth, joining_date, record_date,
                      valid_from_date, valid_to_date, is_current)  # Set ValidFromDate to current date
            data.append(values)

    # Execute the bulk insert query with the data
    try:
        cursor.executemany(insert_query, data)
        conn.commit()
        print("Data inserted successfully for DWSkey.DimCustomers ")
    except mysql.connector.IntegrityError as e:
        print(f"Error: {e}")
        conn.rollback()

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

    conn = mysql.connector.connect(**db_config)

    #Function call for reading data from staging tables
    dataframe_for_stage_dim_customers = read_data_from_stage_dim_customers(conn)
    dataframe_for_stage_dim_subscription = read_data_from_stage_dim_subscription(conn)

    #Function call for data insertion
    insert_data_to_dwskey_dim_customers(conn, dataframe_for_stage_dim_customers)
    insert_data_to_dwskey_dim_subscription(conn,dataframe_for_stage_dim_subscription)

    conn.close()

