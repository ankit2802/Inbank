from datetime import timedelta

import mysql.connector
import pandas as pd
import mysql.connector

def read_data_DWConform_DimCustomers(conn):
    try:
        # Define the SELECT query to fetch data from the table
        select_query = "SELECT * FROM DWConform.DimCustomers;"
        # Use pandas read_sql to fetch data into a DataFrame
        df_conform_dim_customers = pd.read_sql(select_query, conn)

        return df_conform_dim_customers

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None

def read_data_DWConform_DimSubscription(conn):
    try:
        # Define the SELECT query to fetch data from the table
        select_query = "SELECT * FROM DWConform.DimSubscription;"

        # Use pandas read_sql to fetch data into a DataFrame
        df_conform_dim_subscription = pd.read_sql(select_query, conn)

        return df_conform_dim_subscription

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None

def read_data_DWConform_FactCustomerNetflixMetrics(conn):
    try:
        # Define the SELECT query to fetch data from the table
        select_query = "SELECT * FROM DWConform.FactCustomerNetflixMetrics;"

        # Use pandas read_sql to fetch data into a DataFrame
        df_conform_fact_customer_metrics = pd.read_sql(select_query, conn)

        return df_conform_fact_customer_metrics

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None

def insert_data_DW_DimCustomers(conn, df_conform_dim_customers):
    try:
        cursor = conn.cursor()

        for _, row in df_conform_dim_customers.iterrows():
            customer_id = row['CustomerID']
            country = row['Country']
            gender = row['Gender']
            year_of_birth = row['YearOfBirth']
            joining_date = row['JoiningDate']
            record_date = row['RecordDate']

            # Check if the customer exists based on CustomerID and get the latest ValidFromDate
            select_query = (
                "SELECT MAX(ValidFromDate) FROM DW.DimCustomers "
                "WHERE CustomerID = %s;"
            )
            cursor.execute(select_query, (customer_id,))
            latest_valid_from_date = cursor.fetchone()[0]

            if latest_valid_from_date:
                # If the customer exists, update the existing record with ValidToDate
                update_query = (
                    "UPDATE DW.DimCustomers "
                    "SET ValidToDate = %s "
                    "WHERE CustomerID = %s AND ValidFromDate = %s;"
                )
                cursor.execute(update_query, (joining_date - timedelta(days=1), customer_id, latest_valid_from_date))

            # Insert the new record with default ValidToDate and IsCurrent
            insert_query = (
                "INSERT INTO DW.DimCustomers "
                "(CustomerID, Country, Gender, YearOfBirth, JoiningDate, JoiningDateID, RecordDate, RecordDateKey, ValidFromDate, ValidToDate)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            )
            cursor.execute(insert_query, (customer_id, country, gender, year_of_birth, joining_date, 0, record_date, 0, joining_date, '9999-12-31'))

        conn.commit()
        print("Data inserted successfully for DW.DimCustomers")

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        conn.rollback()


def insert_data_DW_DimSubscription(conn, df_conform_dim_subscription):
    try:
        cursor = conn.cursor()

        for _, row in df_conform_dim_subscription.iterrows():
            customer_id = row['CustomerID']
            subscription_duration = row['SubscriptionDuration']
            subscription_type = row['SubscriptionType']
            subscription_type_id = row['SubscriptionTypeID']
            device = row['Device']
            joining_date = row['JoiningDate']
            record_date = row['RecordDate']

            # Check if the customer exists based on CustomerID and get the latest ValidFromDate
            select_query = (
                "SELECT MAX(ValidFromDate) FROM DW.DimSubscription "
                "WHERE CustomerID = %s;"
            )
            cursor.execute(select_query, (customer_id,))
            latest_valid_from_date = cursor.fetchone()[0]

            if latest_valid_from_date:
                # If the customer exists, update the existing record with ValidToDate
                update_query = (
                    "UPDATE DW.DimSubscription "
                    "SET ValidToDate = %s "
                    "WHERE CustomerID = %s AND ValidFromDate = %s;"
                )
                cursor.execute(update_query, (joining_date - timedelta(days=1), customer_id, latest_valid_from_date))

            # Insert the new record with default ValidToDate and IsCurrent
            insert_query = (
                "INSERT INTO DW.DimSubscription "
                "(CustomerID, SubscriptionDuration, SubscriptionType, SubscriptionTypeID, Device, DeviceID, JoiningDate, JoiningDateID, RecordDate, RecordDateKey, ValidFromDate, ValidToDate)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            )
            cursor.execute(insert_query, (customer_id, subscription_duration, subscription_type, subscription_type_id, device, 0, joining_date, 0, record_date, 0, joining_date, '9999-12-31'))

        conn.commit()
        print("Data inserted successfully for DW.DimSubscription")

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        conn.rollback()

def insert_data_DW_FactCustomerNetflixMetrics(conn, df_conform_fact_customer_metrics):
    cursor = conn.cursor()

    # Define the INSERT query with placeholders for the values
    insert_query = (
        "INSERT INTO DW.FactCustomerNetflixMetrics "
        "(CustomerID, MonthlyRevenue, ActiveProfiles, HouseholdProfileInd, MoviesWatched, SeriesWatched, LastPaymentDate,"
        " LastPaymentDateID, RecordDate, RecordDateKey)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )

    # Convert DataFrame rows to tuples for bulk insert
    values_list = [tuple(row) for _, row in df_conform_fact_customer_metrics.iterrows()]

    # Execute the insert query with the data
    try:
        cursor.executemany(insert_query, values_list)
        conn.commit()
        print("Data inserted successfully for DW.FactCustomerNetflixMetrics")
    except mysql.connector.IntegrityError as e:
        print(f"Error: {e}")
        conn.rollback()


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

    #read the data from the conform stage
    df_conform_dim_customers = read_data_DWConform_DimCustomers(conn)
    df_conform_dim_subscription = read_data_DWConform_DimSubscription(conn)
    df_conform_fact_customer_metrics = read_data_DWConform_FactCustomerNetflixMetrics(conn)

    #pushing the data to our Target presentation layer tables
    insert_data_DW_DimCustomers(conn, df_conform_dim_customers)
    insert_data_DW_DimSubscription(conn, df_conform_dim_subscription)
    insert_data_DW_FactCustomerNetflixMetrics(conn, df_conform_fact_customer_metrics)

    conn.close()
