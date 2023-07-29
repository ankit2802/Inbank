#SCD Type 2 has been implementted for the Dimensions
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
    result = read_data_from_stage_dim_customers(conn)
    print(result)
    result = read_data_from_stage_dim_subscription(conn)
    print(result)
