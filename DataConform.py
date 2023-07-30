import mysql.connector
import pandas as pd
from datetime import datetime

def read_data_from_dwskey_dim_subscription(conn):
    # Get the current month and year
    current_month = datetime.now().month
    current_year = datetime.now().year

    # Define the SELECT query to retrieve data from the table for the current month
    select_query = (
        "SELECT CustomerID, SubscriptionDuration, SubscriptionType, "
        "JoiningDate, Device, RecordDate, ValidFromDate, ValidToDateKey"
        "FROM DWSkey.DimSubscription "
        "WHERE MONTH(RecordDate) = %s AND YEAR(RecordDate) = %s;"
    )

    # Execute the SELECT query with the current month and year as parameters
    df_dim_subscription = pd.read_sql(select_query, conn, params=(current_month, current_year))

    return df_dim_subscription

def read_data_from_dwskey_dim_customers(conn):
    # Get the current month and year
    current_month = datetime.now().month
    current_year = datetime.now().year

    # Define the SELECT query to retrieve data from the table for the current month
    select_query = (
        "SELECT CustomerID, Country, Gender, YearOfBirth, JoiningDate, RecordDate, "
        "ValidFromDate, ValidToDateKey"
        "FROM DWSkey.DimCustomers "
        "WHERE MONTH(RecordDate) = %s AND YEAR(RecordDate) = %s;"
    )

    # Execute the SELECT query with the current month and year as parameters
    df_dim_customers = pd.read_sql(select_query, conn, params=(current_month, current_year))

    return df_dim_customers

def read_data_from_stage_dim_subscription(conn):
    select_subscription_query = "SELECT * FROM DWStage.FactCustomerNetflixMetrics"
    try:
        dataframe_for_stage_dim_subscription = pd.read_sql_query(select_subscription_query, conn)
        return dataframe_for_stage_dim_subscription
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def read_data_from_dim_date(conn):
    # Define the SELECT query to retrieve data from the CNF.DimDate table
    select_query = "SELECT date,date_key FROM CNF.DimDate;"

    try:
        # Execute the query and fetch the results into a DataFrame
        df_dim_date = pd.read_sql_query(select_query, conn)
        return df_dim_date
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None

def truncate_tables(conn):
    cursor = conn.cursor()

    # TRUNCATE TABLE queries
    truncate_dim_customers_query = "TRUNCATE TABLE DWConform.DimCustomers;"
    truncate_dim_subscription_query = "TRUNCATE TABLE DWConform.DimSubscription;"
    truncate_fact_customer_query = "TRUNCATE TABLE DWConform.FactCustomerNetflixMetrics;"

    try:
        # Execute the TRUNCATE TABLE queries
        cursor.execute(truncate_fact_customer_query)
        cursor.execute(truncate_dim_subscription_query)
        cursor.execute(truncate_dim_customers_query)
        conn.commit()
        print("Tables truncated successfully.")
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        conn.rollback()

def insert_data_to_dwconform_dim_customers(conn, df_skey_dim_customers,df_cnf_dim_date):

    # Merge the dataframes based on the 'ID' column in df1 and 'CustomerID' column in df2
    merged_df = pd.merge(df_skey_dim_customers, df_cnf_dim_date, left_on='ID', right_on='CustomerID', how='inner')


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

    #Truncate tables - Conform step
    truncate_tables(conn)

    # Read data from the table into a DataFrame
    df_skey_dim_customers = read_data_from_dwskey_dim_customers(conn)
    df_skey_dim_subscription = read_data_from_dwskey_dim_subscription(conn)
    df_stage_dim_subscription = read_data_from_stage_dim_subscription(conn)
    df_cnf_dim_date = read_data_from_dim_date(conn)

    conn.close()
