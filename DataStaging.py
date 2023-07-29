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


def truncate_tables(conn):
    cursor = conn.cursor()

    # TRUNCATE TABLE queries
    truncate_dim_customers_query = "TRUNCATE TABLE DWStage.DimCustomers;"
    truncate_dim_subscription_query = "TRUNCATE TABLE DWStage.DimSubscription;"
    truncate_fact_customer_query = "TRUNCATE TABLE DWStage.FactCustomerNetflixMetrics;"

    # Execute the TRUNCATE TABLE queries
    cursor.execute(truncate_fact_customer_query)
    cursor.execute(truncate_dim_subscription_query)
    cursor.execute(truncate_dim_customers_query)
    conn.commit()


def insert_data_DimCustomers(conn, source_data_df):
    required_columns = source_data_df.loc[:, [
                                                 'UserID',
                                                 'Country',
                                                 'Gender',
                                                 'YearOfBirth',
                                                 'JoinDate',
                                                 'RecordDate'
                                             ]]

    # table to be inserted data in
    tableName = "DWStage.DimCustomers"

    cursor = conn.cursor()

    # Generate the INSERT INTO query with placeholders for the values
    insert_query = (
        f"INSERT INTO {tableName} (CustomerID, Country, Gender,YearOfBirth,JoiningDate, RecordDate ) "
        f"VALUES (%s, %s, %s, %s, %s, %s);"
    )

    # Create a list of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in required_columns.iterrows()]

    # Perform the bulk insert
    # Execute the insert query with the data
    try:
        cursor.executemany(insert_query, values_list)
        conn.commit()
        print("Data inserted successfully for DWStage.DimCustomers")
    except mysql.connector.IntegrityError as e:
        print(f"Error: {e}")
        conn.rollback()

    conn.commit()


def insert_data_FactCustomerNetflixMetrices(conn, source_data_df):

    required_columns = source_data_df.loc[:, [
                                                 'UserID',
                                                 'MonthlyRevenue',
                                                 'ActiveProfiles',
                                                 'HouseholdProfileInd',
                                                 'MoviesWatched',
                                                 'SeriesWatched',
                                                 'LastPaymentDate',
                                                 'RecordDate'
                                             ]]

    # table to be inserted data in
    tableName = "DWStage.FactCustomerNetflixMetrics"

    cursor = conn.cursor()

    # Generate the INSERT INTO query with placeholders for the values
    insert_query = (
        f"INSERT INTO {tableName} (CustomerID, MonthlyRevenue, ActiveProfiles, HouseholdProfileInd, MoviesWatched, SeriesWatched, LastPaymentDate, RecordDate)"
        f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
    )

    # list of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in required_columns.iterrows()]


    # Perform the bulk insert
    # Execute the insert query with the data
    try:
        cursor.executemany(insert_query, values_list)
        conn.commit()
        print("Data inserted successfully for DWStage.FactCustomerNetflixMetrics")
    except mysql.connector.IntegrityError as e:
        print(f"Error: {e}")
        conn.rollback()

    conn.commit()

def insert_data_DimSubscription(conn, source_data_df):

    required_columns = source_data_df.loc[:, [
                                                 'UserID',
                                                 'SubscriptionType',
                                                 'PlanDuration',
                                                 'Device',
                                                 'JoinDate',
                                                 'RecordDate'
                                             ]]

    # table to be inserted data in
    tableName = "DWStage.DimSubscription"

    cursor = conn.cursor()

    # Generate the INSERT INTO query with placeholders for the values
    insert_query = (
        f"INSERT INTO {tableName} (CustomerID, SubscriptionType, SubscriptionDuration, Device, JoiningDate, RecordDate)"
        f"VALUES ( %s,%s, %s, %s, %s, %s);"
    )

    # list of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in required_columns.iterrows()]

    # Perform the bulk insert
    # Execute the insert query with the data
    try:
        cursor.executemany(insert_query, values_list)
        conn.commit()
        print("Data inserted successfully for DWStage.DimSubscription")
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

    # name of the source table
    table_name = "NetflixSubscription"

    conn = mysql.connector.connect(**db_config)

    # Read data from the table into a DataFrame
    source_data_df = read_data_from_table(table_name, conn)

    # Define default values
    defaults = {
        'SubscriptionType': 'N/D',
        'MonthlyRevenue': 0,
        'JoinDate': '1900-01-01',
        'LastPaymentDate': '1900-01-01',
        'Country': 'N/D',
        'Age': 0,
        'Gender': 'N/D',
        'Device': 'N/D',
        'PlanDuration': 'N/D',
        'ActiveProfiles': 0,
        'HouseholdProfileInd': 0,
        'MoviesWatched': 0,
        'SeriesWatched': 0
    }

    # Fill NULL values with default values
    source_data_df.fillna(value=defaults, inplace=True)
    # Add a new column 'YearOfBirth' to the DataFrame by calculating it from the 'Age'
    current_year = datetime.now().year
    source_data_df['YearOfBirth'] = current_year - source_data_df['Age']

    # Drop the 'Age' column from the DataFrame
    source_data_df.drop(columns=['Age'], inplace=True)

    # Truncate the staging tables before inserting the data for next run
    truncate_tables(conn)

    # Insert the data into the table
    insert_data_DimCustomers(conn, source_data_df)
    insert_data_DimSubscription(conn, source_data_df)
    insert_data_FactCustomerNetflixMetrices(conn, source_data_df)

    conn.close()
