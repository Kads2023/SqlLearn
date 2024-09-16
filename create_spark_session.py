# start_spark_session.py
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("SharedSparkSession") \
    .getOrCreate()

# Keep the session alive
while True:
    time.sleep(60)  # Sleep for 60 seconds


import subprocess

# Start the Spark session script in the background
process = subprocess.Popen(['python', 'start_spark_session.py'])

# Print the process ID
print(f"Started Spark session with PID: {process.pid}")


from pyspark.sql import SparkSession

# Directory where the session configuration was saved
conf_file = '/mnt/data/spark_session_conf.txt'

# Read the session configuration
conf = {}
with open(conf_file, 'r') as f:
    for line in f:
        key, value = line.strip().split('=')
        conf[key] = value

# Re-create the Spark session using the loaded configuration
spark_builder = SparkSession.builder
for key, value in conf.items():
    spark_builder = spark_builder.config(key, value)

spark = spark_builder.getOrCreate()

