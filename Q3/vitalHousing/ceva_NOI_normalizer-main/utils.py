import pyodbc
import os
from dotenv import load_dotenv

load_dotenv('.env')

def connect_to_database():
    server = "q3solutions-dev.database.windows.net"
    database = "PropertyManager-dev"
    username =  "q3solutions-dev"
    password = "Q3testMcQ"
    driver = '{ODBC Driver 17 for SQL Server}'
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    
    return conn

def load_data_in_database(conn, deal_id, df_out):

    cursor = conn.cursor()

    for date in df_out["Date"].unique():
        cursor.execute(f"SELECT * FROM RA_IncomeStatement WHERE DealID = {deal_id} AND Date = '{date}'")
        if cursor.fetchone():
            df_out = df_out[df_out["Date"]!=date]
            continue

        print(df_out)

        print("inserting data to database...")

        data_to_insert = df_out.values.tolist()

        sql_query = "INSERT INTO RA_IncomeStatement (DealID, Date, CodeName, Period_to_Date) VALUES (?, ?, ?, ?)"
        cursor.executemany(sql_query, data_to_insert)

        conn.commit()
        conn.close()

        print("data inserted to database...")

