#!/usr/bin/env python
# coding: utf-8

# ## BronzeExchangeRateEvents
# 
# New notebook

# # **Bronze ExchangeRate Events**

# In[30]:


from pyspark.sql.functions import col, from_json,input_file_name, regexp_extract, lit
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


# ### **Parameters**

# In[31]:


path = "Files/exchangerateevents/0"
destination = "exchangerateevents"


# ### **Create Empty Del Table**

# In[5]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TABLE BronzeLakehouse.exchangerateevents 
# (	Id varchar(8000),
# 	CurrencyCode varchar(8000),
# 	IssueDate varchar(8000),
# 	CreatedDate varchar(8000),
# 	ExchangeRate decimal(18, 6)
# )


# ### **Event Data Function**

# In[33]:


def eventsdata(path_param, year_param, month_param, day_param, hour_param):    
    folder_path = f"{path_param}/{year_param}/{month_param}/{day_param}/{hour_param}"
    try:
        avro_df = spark.read.format("avro").option("recursiveFileLookup", "true").load(folder_path)
        print(f"Loading data from: {folder_path}")

        # Add a column with the source file path
        avro_df = avro_df.withColumn("FilePath", input_file_name())

        # Extract partition, year, month, day from the file path
        avro_df = avro_df.withColumn("PartitionEventFolder", regexp_extract("FilePath", r"/([^/]+)/\d{4}/", 1)) \
                        .withColumn("YearEventFolder", regexp_extract("FilePath", r"/(\d{4})/", 1)) \
                        .withColumn("MonthEventFolder", regexp_extract("FilePath", r"/\d{4}/(\d{2})/", 1)) \
                        .withColumn("DayEventFolder", regexp_extract("FilePath", r"/\d{4}/\d{2}/(\d{2})/", 1)) \
                        .withColumn("HourEventFolder", regexp_extract("FilePath", r"/\d{4}/\d{2}/\d{2}/(\d{2})/", 1))

        return avro_df
    except Exception as e:
        print(f"Error loading data from {folder_path}: {e}")
        return None


# ### **Load Data Into DataFrame**

# In[34]:


query = f"""
SELECT a.BronzeTable,
       COLLECT_LIST(ARRAY(YEAR(b.Date), LPAD(MONTH(b.Date), 2, '0'), LPAD(DAY(b.Date), 2, '0'),
       CASE WHEN CAST(b.Date AS TIMESTAMP) = CAST(a.DateEventLoaded AS TIMESTAMP) THEN a.HourEventFolder ELSE 0 END,
       CASE WHEN b.Date = CAST(current_date() AS DATE) THEN HOUR(current_timestamp())+2
       ELSE 24 END
       )) AS Parameters
FROM MNGEventsLoad a
JOIN dimdate b
    ON b.Date BETWEEN a.DateEventLoaded AND CAST(current_timestamp() AS TIMESTAMP) 
WHERE a.BronzeTable = '{destination}'
GROUP BY a.BronzeTable"""

Parameters_df = spark.sql(query)

# Collect the parameters DataFrame
params_list = Parameters_df.collect()

union_df = None

# Iterate over the rows of the DataFrame
for row in params_list:
    bronze_table = row["BronzeTable"]
    parameters = row["Parameters"]
    
    # Print the bronze table
    print(f"BronzeTable: {bronze_table}")
    
    # Iterate over the parameters array
    for parameter in parameters:
        year, month, day, hour, TotalHours = parameter

        # Iterate over the hours array
        for h in range(int(hour), int(hour) + int(TotalHours)):
            hr = f"{h:02d}"
            print(f"YearEventFolder: {year}, MonthEventFolder: {month}, DayEventFolder: {day}, HourEventFolder: {hr}, TotalHours: {TotalHours}")
        
            df = eventsdata(path, year, month, day, hr)        
            if df is not None:
                if union_df is None:
                    union_df = df
                else:
                    union_df = union_df.unionByName(df, allowMissingColumns=True)
            if h >= 23:
                break


# ### **Convert Body Field from Arvo to JSON**

# In[35]:


# Cast Body it to string
body_df = union_df.withColumn("Body", union_df["Body"].cast("string").alias("Body"))

# Convert Body to JSON
schema = spark.read.json(body_df.select("Body").rdd.map(lambda x: x.Body)).schema
body_df = body_df.withColumn("Body", from_json("Body", schema).alias("Body"))


# In[36]:


display(body_df)


# ### **Flattern DF**

# In[38]:


# Function to extract all nested column names
def get_column_names(schema, prefix=""):
    columns = []
    for field in schema.fields:
        name = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            columns.extend(get_column_names(field.dataType, name))
        else:
            columns.append(name)
    return columns

# Desired columns with their aliases
desired_columns_with_aliases = {
    "Body.Data.IssueDate": "IssueDate",
    "Body.CreatedDate": "CreatedDate",
    "Body.CurrencyCode": "CurrencyCode",
    "Body.Id": "Id",
    "Body.Data.ExchangeRate": "ExchangeRate",
    "YearEventFolder": "YearEventFolder",
    "MonthEventFolder": "MonthEventFolder",
    "DayEventFolder": "DayEventFolder",
    "HourEventFolder": "HourEventFolder"
}

# Get existing columns in the DataFrame, considering nested columns
existing_columns = set(get_column_names(body_df.schema))

# Create a list of columns to select, replacing missing columns with null
select_expr = [
    F.col(col).alias(alias) if col in existing_columns else F.lit(None).cast("string").alias(alias)
    for col, alias in desired_columns_with_aliases.items()
]


# Select the columns
body_df_update = body_df.select(*select_expr)



# ### **Load Data Into Table**

# In[40]:


body_df_update.write.format("delta").mode("overwrite").option("OverwriteSchema","true").option("parquet.vorder.enabled ","true").saveAsTable("exchangerateevents",path="Tables/exchangerateevents")


# In[41]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# select count(*) from exchangerateevents


# In[ ]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# with deduped_ExchangeRate as (
#     SELECT
#         CAST(a.IssueDate AS TIMESTAMP) AS IssueDate,
#         CAST(DATE_FORMAT(CAST(a.IssueDate AS TIMESTAMP), 'yyyyMMdd') AS INT) AS IssueDateKey,
#         a.CurrencyCode as CurrencyCode,
#         a.Id as Id,
#         a.ExchangeRate as ExchangeRate,
#         CAST(a.CreatedDate AS TIMESTAMP) AS CreatedDate,
#         ROW_NUMBER() OVER (PARTITION BY a.CurrencyCode,a.IssueDate  ORDER BY a.CreatedDate DESC) AS rownum
#     FROM BronzeLakeHouse.exchangerateevents a
# )

# select count(*) from deduped_ExchangeRate WHERE rownum = 1

