#!/usr/bin/env python
# coding: utf-8

# ## Gold Layer Spark read/write
# 
# New notebook

# In[5]:


import pyodbc
import pandas as pd
from pyspark.sql import SparkSession
import adal
import pandas as pd

# Service Principal details
tenant_id = ""
client_id = ""
client_secret =  ""

# SQL Endpoint details
server = "<>.datawarehouse.fabric.microsoft.com"
database = "SDPWH"

# Get OAuth token
authority_url = f"https://login.microsoftonline.com/{tenant_id}"
context = adal.AuthenticationContext(authority_url)
token = context.acquire_token_with_client_credentials(
    resource="https://database.windows.net/",
    client_id=client_id,
    client_secret=client_secret
)



# ODBC connection string
conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    "Authentication=ActiveDirectoryServicePrincipal;"
    f"UID={client_id};"
    f"PWD={client_secret};"
)

def execute_query(query):
    try:
        with pyodbc.connect(conn_str, timeout=30) as conn:
            df = pd.read_sql(query, conn)
        return spark.createDataFrame(df)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


def execute_query(query, params=None):
    try:
        with pyodbc.connect(conn_str, timeout=30) as conn:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Fetch all results
                columns = [column[0] for column in cursor.description]
                results = cursor.fetchall()
                
                # Convert to pandas DataFrame
                df = pd.DataFrame.from_records(results, columns=columns)
        
        # Convert pandas DataFrame to Spark DataFrame
        return spark.createDataFrame(df)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

# # Example 1: Execute a stored procedure
# def execute_stored_procedure(proc_name, params=None):
#     query = f"EXEC {proc_name}"
#     if params:
#         query += f" {', '.join(params)}"
#     return execute_query(query)

# Example 2: Select from a table
def select_from_table(table_name, columns=None):
    if columns:
        column_str = ", ".join(columns)
    else:
        column_str = "*"
    query = f"SELECT {column_str} FROM {table_name}"
    return execute_query(query)

# Usage example:
df_table = select_from_table("MainWH.dimdate")
if df_table is not None:
    df_table.show(5)  # Display first 5 rows


# In[2]:


display(df_table)


# In[6]:


def execute_stored_procedure(proc_name, params=None):
    param_placeholders = ', '.join(['?' for _ in params]) if params else ''
    query = f"EXEC {proc_name} {param_placeholders}"
    return execute_query(query, params)

# Example usage:
def run_example_procedure():
    proc_name = "ServiceWH.sp_MNG_TriggerMNGSPGatewayRun"
    params = (2, 'spark_user', 'BBG00NNJM2H5,BBG00M6PPWH3,BBG0010JZ1W9,BBG01H3QMQF2', '2023,2024', '')
    result_df = execute_stored_procedure(proc_name, params)
    
    if result_df is not None:
        print(f"Results from {proc_name}:")
        result_df.show()  # Show up to 10 rows without truncating
        
        # print("\nDataFrame Schema:")
        # result_df.printSchema()
        
        # print(f"\nTotal number of rows: {result_df.count()}")
        
        # # Optional: If you want to perform further operations on the data
        # # result_df.createOrReplaceTempView("temp_result_view")
        # # spark.sql("SELECT * FROM temp_result_view WHERE some_column > some_value").show()
    else:
        print("Failed to execute the stored procedure or no results returned.")

# Run the example
run_example_procedure()


# In[7]:


from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def run_example_procedure(run_id):
    proc_name = "ServiceWH.sp_MNG_TriggerMNGSPGatewayRun"
    params = (2, f'spark_user_{run_id}', 'BBG00NNJM2H5,BBG00M6PPWH3,BBG0010JZ1W9,BBG01H3QMQF2', '2023,2024', '')
    start_time = time.time()
    result_df = execute_stored_procedure(proc_name, params)
    end_time = time.time()
    
    if result_df is not None:
        row_count = result_df.count()
        print(f"Run {run_id}: Executed in {end_time - start_time:.2f} seconds. Returned {row_count} rows.")
    else:
        print(f"Run {run_id}: Failed to execute the stored procedure or no results returned.")
    
    return run_id, result_df

def stress_test(num_concurrent_calls):
    with ThreadPoolExecutor(max_workers=num_concurrent_calls) as executor:
        futures = [executor.submit(run_example_procedure, i) for i in range(num_concurrent_calls)]
        
        for future in as_completed(futures):
            try:
                run_id, result = future.result()
                if result is not None:
                    print(f"Run {run_id} completed successfully.")
                else:
                    print(f"Run {run_id} failed or returned no results.")
            except Exception as e:
                print(f"An error occurred in one of the runs: {str(e)}")

# Run the stress test
if __name__ == "__main__":
    num_concurrent_calls = 20  # You can adjust this number
    print(f"Starting stress test with {num_concurrent_calls} concurrent calls...")
    stress_test(num_concurrent_calls)
    print("Stress test completed.")

