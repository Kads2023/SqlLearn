#!/usr/bin/env python
# coding: utf-8

# ## SilverFactMarketData_Latest
# 
# New notebook

# **Input Params**

# In[2]:


# in_env='sales'


# In[3]:


# in_env is an optional input parameters 
# if in_env is sales, then table_and_file_prefix is set to sales_

table_and_file_prefix = ""
try:
    if in_env.lower() == "sales":
        table_and_file_prefix = "sales_"
    print(f"in_env --> {in_env}, table_and_file_prefix --> {table_and_file_prefix}")
except:
    print(f"in_env, not passed, table_and_file_prefix --> {table_and_file_prefix}")


# In[4]:


spark.conf.set("spark.sql.parquet.vorder.enabled", "true")


# **Run Utilities notebook to use the functions**

# In[5]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run SilverFactRerateRecommendationsUtilities


# In[6]:


# table_prefix comes from SilverFactRerateRecommendationsUtilities notebook
# table_and_file_prefix is formed based on input params

final_table_prefix = f"{table_and_file_prefix}{table_prefix}"
print(f"final_table_prefix --> {final_table_prefix}")


# In[7]:


source_table = f"{database}.{final_table_prefix}factmarketdata"
target_table = f"{database}.{table_and_file_prefix}factmarketdata_latest"
source_table_alias = "fmd"

# columns based on which we dedupe and get the latest value
partitionBy_columns = "InstrumentId,ProviderKey"
orderBy_column = "MarketDate"
orderBy_clause = "desc"

# list of columns for which we need to get the latest available value
columns_list_for_which_we_need_latest = ['Price', 'BenchmarkFee', 'VWAF', 'VWAF1Day', 'VWAF3Days', 'VWAF7Days', 'Utilisation', 'ShortLoanValue', 'SharesOutstanding']


# In[8]:


print(f"source_table --> {source_table}, target_table --> {target_table}")


# In[9]:


# template sqls to be formed for each of the columns provided

with_clause_template_sql = f"""
latest_value_each_column_name as (
    SELECT {partitionBy_columns}, {orderBy_column} AS each_column_nameLastUpdate, each_column_name AS each_column_nameLastValue
    FROM
    (
        select {partitionBy_columns}, {orderBy_column}, each_column_name,
                count(each_column_name) over (partition by {partitionBy_columns} order by {orderBy_column} {orderBy_clause}) as grp
        from {source_table}
        WHERE 
        each_column_name IS NOT NULL
    ) t
    WHERE grp = 1
)"""

select_columns_template_sql = f"each_column_nameLastValue AS each_column_name, each_column_nameLastUpdate"

with_clause_tables_template_sql = "latest_value_each_column_name"


# In[10]:


final_select_columns_list = []
final_with_clause_list = []
final_with_clause_tables_list = []
final_joining_clause_list = []


# In[11]:


# forming the joining clause template sqls based on the partitionBy_columns provided

joining_clause_list = []
for each_partitionBy_columns in partitionBy_columns.split(','):
    joining_clause_list.append(f"lv_each_column_name.{each_partitionBy_columns} = {source_table_alias}.{each_partitionBy_columns}")
    final_select_columns_list.append(f"{source_table_alias}.{each_partitionBy_columns}")

joining_clause_template_sql = "\nLEFT JOIN latest_value_each_column_name lv_each_column_name\nON\n(\n\t" + ' AND \n\t'.join(joining_clause_list) + '\n)'
print(f"joining_clause_template_sql --> {joining_clause_template_sql}")


# In[12]:


# loop through each one of the columns or which we need the latest and form the different clauses based on templates

for each_column_name in columns_list_for_which_we_need_latest:
    print(f"each_column_name --> {each_column_name}")
    each_column_with_clause_template_sql = with_clause_template_sql.replace("each_column_name", each_column_name)
    # print(f"each_column_with_clause_template_sql --> {each_column_with_clause_template_sql}")
    final_with_clause_list.append(each_column_with_clause_template_sql)

    each_column_select_columns_template_sql = select_columns_template_sql.replace("each_column_name", each_column_name)
    # print(f"each_column_select_columns_template_sql --> {each_column_select_columns_template_sql}")
    final_select_columns_list.append(each_column_select_columns_template_sql)

    each_column_with_clause_tables_template_sql = with_clause_tables_template_sql.replace("each_column_name", each_column_name)
    # print(f"each_column_with_clause_tables_template_sql --> {each_column_with_clause_tables_template_sql}")
    final_with_clause_tables_list.append(each_column_with_clause_tables_template_sql)

    each_column_joining_clause_template_sql = joining_clause_template_sql.replace("each_column_name", each_column_name)
    # print(f"each_column_joining_clause_template_sql --> {each_column_joining_clause_template_sql}")
    final_joining_clause_list.append(each_column_joining_clause_template_sql)


# In[13]:


# form the final select columns

final_select_columns = f'SELECT '+ ', '.join(final_select_columns_list) 
print(final_select_columns)


# In[ ]:


# form the final with clause

final_with_clause_tables = ','.join(final_with_clause_tables_list) 
print(final_with_clause_tables)


# In[ ]:


# add the columns from source table for joining with the final with clause

final_with_clause = f"""WITH 
{source_table_alias}_distinct_values
(
    SELECT DISTINCT {partitionBy_columns}
    FROM 
    {source_table}
),
"""+ ',\n'.join(final_with_clause_list)
# print(final_with_clause)


# In[ ]:


# form the final joining clause

final_joining_clause = ''.join(final_joining_clause_list)
# print(final_joining_clause)


# In[ ]:


# form the final sql

final_sql_to_get_values = f"""
CREATE OR REPLACE TABLE {target_table} AS

{final_with_clause}

{final_select_columns}
FROM 
{source_table_alias}_distinct_values {source_table_alias}
{final_joining_clause}
"""


# In[1]:


# print(f"final_sql_to_get_values --> {final_sql_to_get_values}")


# In[19]:


# execute the formed final sql
spark.sql(final_sql_to_get_values)


# In[20]:


# Refresh the target table
spark.sql(f"REFRESH TABLE {target_table}")


# In[21]:


# Display the counts of the source and target table
display(spark.sql(f"select count(*) from {source_table}"))
display(spark.sql(f"select count(*) from {target_table}"))


# In[22]:


# Display the distinct counts of the source and target table
display(spark.sql(f"select count( DISTINCT {partitionBy_columns}) from {source_table}"))
display(spark.sql(f"select count( DISTINCT {partitionBy_columns}) from {target_table}"))


# In[23]:


# Display the min and max of date column from the source
display(spark.sql(f"select min(MarketDate), max(MarketDate) from {source_table}"))


# In[24]:


# Display a sample 10 records from the target table 
display(spark.sql(f"select * from {target_table} limit 10"))

