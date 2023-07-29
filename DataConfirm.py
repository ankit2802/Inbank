def insert_data_DimSubscription(conn,source_data_df):

    # column name you want to get distinct values from
    selected_column = 'SubscriptionType'
    distinct_values = source_data_df[selected_column].drop_duplicates()

    cursor = conn.cursor()
    table_name = "DWStage.DimSubscription"
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
    table_name = "DWStage.DimDevice"
    column_name = "DeviceType"

    # Generate the INSERT INTO query with placeholders for the values while ignoring the existing values
    insert_query = f"INSERT IGNORE INTO {table_name} ({column_name}) VALUES (%s);"

    # Insert each distinct value into the table
    for value in distinct_values:
        cursor.execute(insert_query, (value,))
    conn.commit()
