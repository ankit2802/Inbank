import mysql.connector
import pandas as pd
from datetime import datetime


def read_data_dwstage_fact_customer_netflix_metrics(conn):
    select_query = "SELECT * FROM DWStage.FactCustomerNetflixMetrics;"

    try:
        # Use pandas read_sql to fetch data into a DataFrame
        df_cnf_fact_customer = pd.read_sql(select_query, conn)
        return df_cnf_fact_customer

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None


def read_data_DimDeviceType(conn):
    select_query = "SELECT * FROM CNF.DimDeviceType;"

    try:
        # Use pandas read_sql to fetch data into a DataFrame
        DimDeviceType = pd.read_sql(select_query, conn)
        return DimDeviceType

    except mysql.connector.Error as e:
        print(f"Error: {e}")


def read_data_DimSubscriptionType(conn):
    # Define the SELECT query
    select_query = "SELECT * FROM CNF.DimSubscriptionType;"

    try:
        # Use pandas read_sql to fetch data into a DataFrame
        DimSubscriptionType = pd.read_sql(select_query, conn)
        return DimSubscriptionType

    except mysql.connector.Error as e:
        print(f"Error: {e}")


def read_data_from_dwskey_dim_subscription(conn):
    # Get the current month and year
    current_month = datetime.now().month
    current_year = datetime.now().year

    # Define the SELECT query to retrieve data from the table for the current month
    select_query = (
        "SELECT RecordKey, CustomerID, SubscriptionDuration, SubscriptionType, "
        "JoiningDate, Device, RecordDate, ValidFromDate, ValidToDate "
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
        "SELECT RecordKey, CustomerID, Country, Gender, YearOfBirth, JoiningDate, RecordDate, "
        "ValidFromDate, ValidToDate "
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


def insert_data_to_dwconform_dim_customers(conn, df_skey_dim_customers, df_cnf_dim_date):
    # Here in steps the datframe has been built on top of each step (basically its the lookup for getting the dates in form of int)

    # Step 1 getting JoiningDateID into dataframe from DimDate
    dimcustomers_dimdate_JoiningDate = pd.merge(df_skey_dim_customers, df_cnf_dim_date, left_on='JoiningDate',
                                                right_on='date', how='inner')
    # Dropping the extra columns
    dimcustomers_dimdate_JoiningDate.drop(columns=['date'], inplace=True)
    # Renaming the column as per the need
    dimcustomers_dimdate_JoiningDate.rename(columns={'date_key': 'JoiningDateID'}, inplace=True)

    # Step 2 getting RecordDateKey into dataframe from DimDate
    dimcustomers_dimdate_RecordDate = pd.merge(dimcustomers_dimdate_JoiningDate, df_cnf_dim_date, left_on='RecordDate',
                                               right_on='date', how='inner')
    # Dropping the extra columns
    dimcustomers_dimdate_RecordDate.drop(columns=['date'], inplace=True)
    # Renaming the column as per the need
    dimcustomers_dimdate_RecordDate.rename(columns={'date_key': 'RecordDateKey'}, inplace=True)

    # setting defaults for the fields added in this step:
    default_values = {
        'JoiningDateID': 0,  # Default value for 'JoiningDateID'
        'RecordDateKey': 0,  # Default value for 'RecordDateKey'
        # Add more fields and their default values as needed
    }
    # Set default values for missing fields in the DataFrame
    dimcustomers_dimdate_RecordDate.fillna(default_values, inplace=True)

    cursor = conn.cursor()
    dimcustomers_dimdate_RecordDate = dimcustomers_dimdate_RecordDate.loc[:, [
                                                                                 'CustomerID',
                                                                                 'Country',
                                                                                 'Gender',
                                                                                 'YearOfBirth',
                                                                                 'JoiningDate',
                                                                                 'JoiningDateID',
                                                                                 'RecordDate',
                                                                                 'RecordDateKey',
                                                                                 'ValidFromDate',
                                                                                 'ValidToDate'
                                                                             ]]

    # list of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in dimcustomers_dimdate_RecordDate.iterrows()]

    # Define the INSERT query
    insert_query = (
        "INSERT INTO DWConform.DimCustomers "
        "(CustomerID, Country, Gender, YearOfBirth, JoiningDate, JoiningDateID, RecordDate, RecordDateKey,"
        " ValidFromDate, ValidToDate)"
        "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )

    # Perform the bulk insert
    # Execute the insert query with the data
    try:
        cursor.executemany(insert_query, values_list)
        conn.commit()
        print("Data inserted successfully for DWConform.DimCustomers")
    except mysql.connector.IntegrityError as e:
        print(f"Error: {e}")
        conn.rollback()

    conn.commit()


def insert_data_to_dwconform_dim_subscription(conn, df_skey_dim_subscription, df_cnf_dim_date, df_cnf_dim_device,
                                              df_cnf_dim_subscription):
    # Step 1: Getting JoiningDateID into dataframe from DimDate
    dimsubscription_dimdate_JoiningDate = pd.merge(df_skey_dim_subscription, df_cnf_dim_date, left_on='JoiningDate',
                                                   right_on='date', how='inner')
    # Dropping the extra columns
    dimsubscription_dimdate_JoiningDate.drop(columns=['date'], inplace=True)
    # Renaming the column as per the need
    dimsubscription_dimdate_JoiningDate.rename(columns={'date_key': 'JoiningDateID'}, inplace=True)

    # Step 2: Getting RecordDateKey into dataframe from DimDate
    dimsubscription_dimdate_RecordDate = pd.merge(dimsubscription_dimdate_JoiningDate, df_cnf_dim_date,
                                                  left_on='RecordDate', right_on='date', how='inner')
    # Dropping the extra columns
    dimsubscription_dimdate_RecordDate.drop(columns=['date'], inplace=True)
    # Renaming the column as per the need
    dimsubscription_dimdate_RecordDate.rename(columns={'date_key': 'RecordDateKey'}, inplace=True)

    # Step 3: Getting JoiningDateID into dataframe from DimDate
    dimsubscription_dimdate_Device = pd.merge(dimsubscription_dimdate_RecordDate, df_cnf_dim_device, left_on='Device',
                                              right_on='DeviceType', how='left')
    # Dropping the extra columns
    dimsubscription_dimdate_Device.drop(columns=['DeviceType'], inplace=True)
    # Renaming the column as per the need
    dimsubscription_dimdate_Device.rename(columns={'DeviceTypeKey': 'DeviceID'}, inplace=True)

    # Step 4: Getting JoiningDateID into dataframe from DimDate
    dimsubscription_dimdate_SubscriptionType = pd.merge(dimsubscription_dimdate_Device, df_cnf_dim_subscription,
                                                        left_on='SubscriptionType', right_on='SubscriptionType',
                                                        how='left')
    # Dropping the extra columns
    # dimsubscription_dimdate_SubscriptionType.drop(columns=['SubscriptionType'], inplace=True)
    # Renaming the column as per the need
    dimsubscription_dimdate_SubscriptionType.rename(columns={'SubscriptionTypeKey': 'SubscriptionTypeID'}, inplace=True)

    cursor = conn.cursor()
    dimsubscription_dimdate_SubscriptionType = dimsubscription_dimdate_SubscriptionType.loc[:, [
                                                                                                   'CustomerID',
                                                                                                   'SubscriptionDuration',
                                                                                                   'SubscriptionType',
                                                                                                   'SubscriptionTypeID',
                                                                                                   'Device',
                                                                                                   'DeviceID',
                                                                                                   'JoiningDate',
                                                                                                   'JoiningDateID',
                                                                                                   'RecordDate',
                                                                                                   'RecordDateKey',
                                                                                                   'ValidFromDate',
                                                                                                   'ValidToDate'
                                                                                                   # Add more fields as needed
                                                                                               ]]

    # Set default values for missing fields in the DataFrame
    default_values = {
        'SubscriptionTypeID': 0,
        'DeviceID': 0,
        'JoiningDateID': 0,  # Default value for 'JoiningDateID'
        'RecordDateKey': 0,  # Default value for 'RecordDateKey'
        # Add more fields and their default values as needed
    }
    dimsubscription_dimdate_SubscriptionType.fillna(default_values, inplace=True)

    # list of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in dimsubscription_dimdate_SubscriptionType.iterrows()]

    # Define the INSERT query
    insert_query = (
        "INSERT INTO DWConform.DimSubscription "
        "(CustomerID, SubscriptionDuration, SubscriptionType, SubscriptionTypeID, Device, DeviceID, JoiningDate,"
        " JoiningDateID, RecordDate, RecordDateKey, ValidFromDate, ValidToDate )"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    # Perform the bulk insert
    # Execute the insert query with the data
    try:
        cursor.executemany(insert_query, values_list)
        conn.commit()
        print("Data inserted successfully for DWConform.DimSubscription")
    except mysql.connector.IntegrityError as e:
        print(f"Error: {e}")
        conn.rollback()

    conn.commit()


def insert_data_to_dwconform_fact_customer_netflix_metrics(conn, df_skey_fact_customer_metrics, df_cnf_dim_date):
    # Step 1: Getting LastPaymentDateID into the DataFrame from DimDate
    fact_customer_metrics_dimdate_LastPaymentDate = pd.merge(df_skey_fact_customer_metrics, df_cnf_dim_date,
                                                             left_on='LastPaymentDate', right_on='date', how='inner')
    # Dropping the extra columns
    fact_customer_metrics_dimdate_LastPaymentDate.drop(columns=['date'], inplace=True)
    # Renaming the column as per the need
    fact_customer_metrics_dimdate_LastPaymentDate.rename(columns={'date_key': 'LastPaymentDateID'}, inplace=True)

    # Step 2: Getting RecordDateKey into the DataFrame from DimDate
    fact_customer_metrics_dimdate_RecordDate = pd.merge(fact_customer_metrics_dimdate_LastPaymentDate, df_cnf_dim_date,
                                                        left_on='RecordDate', right_on='date', how='inner')
    # Dropping the extra columns
    fact_customer_metrics_dimdate_RecordDate.drop(columns=['date'], inplace=True)
    # Renaming the column as per the need
    fact_customer_metrics_dimdate_RecordDate.rename(columns={'date_key': 'RecordDateKey'}, inplace=True)

    # Setting defaults for the fields added in this step:
    default_values = {
        'LastPaymentDateID': 0,  # Default value for 'LastPaymentDateID'
        'RecordDateKey': 0,  # Default value for 'RecordDateKey'
        # Add more fields and their default values as needed
    }
    # Set default values for missing fields in the DataFrame
    fact_customer_metrics_dimdate_RecordDate.fillna(default_values, inplace=True)

    cursor = conn.cursor()
    fact_customer_metrics_dimdate_RecordDate = fact_customer_metrics_dimdate_RecordDate.loc[:, [
                                                                                                   'CustomerID',
                                                                                                   'MonthlyRevenue',
                                                                                                   'ActiveProfiles',
                                                                                                   'HouseholdProfileInd',
                                                                                                   'MoviesWatched',
                                                                                                   'SeriesWatched',
                                                                                                   'LastPaymentDate',
                                                                                                   'LastPaymentDateID',
                                                                                                   'RecordDate',
                                                                                                   'RecordDateKey'
                                                                                               ]]

    # List of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in fact_customer_metrics_dimdate_RecordDate.iterrows()]

    # Define the INSERT query
    insert_query = (
        "INSERT INTO DWConform.FactCustomerNetflixMetrics "
        "(CustomerID, MonthlyRevenue, ActiveProfiles, HouseholdProfileInd, MoviesWatched, SeriesWatched, "
        "LastPaymentDate, LastPaymentDateID, RecordDate, RecordDateKey) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )

    # Perform the bulk insert
    # Execute the insert query with the data
    try:
        cursor.executemany(insert_query, values_list)
        conn.commit()
        print("Data inserted successfully for DWConform.FactCustomerNetflixMetrics")
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

    # Truncate tables - Conform step
    truncate_tables(conn)

    # Set the option to display all columns
    pd.set_option('display.max_columns', None)

    # Read data from the table into a DataFrame
    df_skey_dim_customers = read_data_from_dwskey_dim_customers(conn)
    df_skey_dim_subscription = read_data_from_dwskey_dim_subscription(conn)
    df_stage_dim_subscription = read_data_from_stage_dim_subscription(conn)
    df_cnf_dim_date = read_data_from_dim_date(conn)
    df_cnf_dim_device = read_data_DimDeviceType(conn)
    df_cnf_dim_subscription = read_data_DimSubscriptionType(conn)
    df_cnf_fact_customer = read_data_dwstage_fact_customer_netflix_metrics(conn)

    # Data insertion in Conform tables
    insert_data_to_dwconform_dim_customers(conn, df_skey_dim_customers, df_cnf_dim_date)
    insert_data_to_dwconform_dim_subscription(conn, df_skey_dim_subscription, df_cnf_dim_date, df_cnf_dim_device,
                                              df_cnf_dim_subscription)
    insert_data_to_dwconform_fact_customer_netflix_metrics(conn, df_cnf_fact_customer, df_cnf_dim_date)
    conn.close()
