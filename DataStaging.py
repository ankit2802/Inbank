from datetime import datetime

import mysql.connector
import pandas as pd


def read_data_from_table(table_name, conn):
    # Create a DataFrame by fetching data from the table
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, conn)

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
                                                 'Age',
                                                 'Gender'
                                             ]]

    # Add a new column 'YearOfBirth' to the DataFrame by calculating it from the 'Age'
    current_year = datetime.now().year
    required_columns['YearOfBirth'] = current_year - required_columns['Age']

    # Drop the 'Age' column from the DataFrame
    required_columns.drop(columns=['Age'], inplace=True)

    # table to be inserted data in
    tableName = "DWStage.DimCustomers"

    cursor = conn.cursor()

    # Generate the INSERT INTO query with placeholders for the values
    insert_query = (
        f"INSERT INTO {tableName} (CustomerID, Country, Gender, YearOfBirth) "
        f"VALUES (%s, %s, %s, %s);"
    )

    # Create a list of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in required_columns.iterrows()]

    # Perform the bulk insert
    cursor.executemany(insert_query, values_list)

    conn.commit()


def insert_data_FactCustomerNetflixMetrices(conn, source_data_df):

    required_columns = source_data_df.loc[:, [
                                                 'UserID',
                                                 'MonthlyRevenue',
                                                 'ActiveProfiles',
                                                 'HouseholdProfileInd',
                                                 'MoviesWatched',
                                                 'SeriesWatched',
                                                 'LastPaymentDate'
                                             ]]

    # table to be inserted data in
    tableName = "DWStage.FactCustomerNetflixMetrics"

    cursor = conn.cursor()

    # Generate the INSERT INTO query with placeholders for the values
    insert_query = (
        f"INSERT INTO {tableName} (CustomerID, MonthlyRevenue, ActiveProfiles, HouseholdProfileInd, MoviesWatched, SeriesWatched, LastPaymentDate)"
        f"VALUES (%s, %s, %s, %s, %s, %s, %s);"
    )

    # list of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in required_columns.iterrows()]

    # Perform the bulk insert
    cursor.executemany(insert_query, values_list)

    conn.commit()

def insert_data_DimSubscription(conn, source_data_df):

    required_columns = source_data_df.loc[:, [
                                                 'UserID',
                                                 'SubscriptionType',
                                                 'PlanDuration',
                                                 'JoinDate',
                                                 'Device'
                                             ]]

    print(required_columns)
    # table to be inserted data in
    tableName = "DWStage.DimSubscription"

    cursor = conn.cursor()

    # Generate the INSERT INTO query with placeholders for the values
    insert_query = (
        f"INSERT INTO {tableName} (CustomerID, SubscriptionType, SubscriptionDuration, JoiningDate, Device)"
        f"VALUES (%s, %s, %s, %s, %s);"
    )

    # list of tuples containing the values to be inserted
    values_list = [tuple(row) for _, row in required_columns.iterrows()]

    # Perform the bulk insert
    cursor.executemany(insert_query, values_list)

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

    # Truncate the staging tables before inserting the data for next run
    truncate_tables(conn)

    # Insert the data into the table
    #    insert_data_DimSubscription(conn,source_data_df)
    #    insert_data_DimDevice(conn, source_data_df)
    insert_data_DimCustomers(conn, source_data_df)
    insert_data_DimSubscription(conn, source_data_df)
    insert_data_FactCustomerNetflixMetrices(conn, source_data_df)

    conn.close()
