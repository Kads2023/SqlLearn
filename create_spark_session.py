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
