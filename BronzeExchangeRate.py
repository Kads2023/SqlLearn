#!/usr/bin/env python
# coding: utf-8

# ## BronzeExchangeRate
# 
# New notebook

# # **Bronze ExchangeRate**

# In[1]:


from pyspark.sql.functions import col, conv, hex
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, BinaryType


# **Input Params**

# In[2]:


# in_env='sales'


# In[3]:


table_and_file_prefix = ""
try:
    if in_env.lower() == "sales":
        table_and_file_prefix = "sales_"
    print(f"in_env --> {in_env}, table_and_file_prefix --> {table_and_file_prefix}")
except:
    print(f"in_env, not passed, table_and_file_prefix --> {table_and_file_prefix}")


# In[4]:


df_ExchangeRateEvents = spark.read.parquet(f"Files/InitialLoad/{table_and_file_prefix.capitalize()}ExchangeRate.parquet")


# In[5]:


df_ExchangeRateEvents.count()


# In[6]:


df_ExchangeRateEvents.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("parquet.vorder.enabled ","true").saveAsTable(f"{table_and_file_prefix}exchangerate",path=f"Tables/{table_and_file_prefix}exchangerate")


# In[7]:


spark.sql(f"REFRESH TABLE BronzeLakeHouse.{table_and_file_prefix}exchangerate")
spark.sql("REFRESH TABLE BronzeLakeHouse.exchangerate")


# In[8]:


display(spark.sql(f"select count(*) from BronzeLakeHouse.{table_and_file_prefix}exchangerate"))
display(spark.sql("select count(*) from BronzeLakeHouse.exchangerate"))


# In[9]:


spark.sql(f"OPTIMIZE BronzeLakeHouse.{table_and_file_prefix}exchangerate VORDER")
spark.sql("OPTIMIZE BronzeLakeHouse.exchangerate VORDER")


# In[10]:


display(spark.sql(f"select * from BronzeLakeHouse.{table_and_file_prefix}exchangerate limit 10"))
display(spark.sql("select * from BronzeLakeHouse.exchangerate limit 10"))

