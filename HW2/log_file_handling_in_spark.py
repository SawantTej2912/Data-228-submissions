

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime


spark = SparkSession.builder.appName("HandleLogFiles").getOrCreate()

# Printing Spark version and current time
print("Spark version: ", spark.version)
print("Current time: ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Load all .gz files in the directory into a DataFrame
df = spark.read.text("data_export/*.gz")

# Check the number of partitions
print("Number of partitions: ", df.rdd.getNumPartitions())
df.show(truncate=False)

# Create a parsed dataframe (log_df)
# Extract the necessary information from log data using regular expressions
pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP.*" (\d+) (\d+)'
log_df = df.select(
    F.regexp_extract("value", pattern, 1).alias("ip"),
    F.regexp_extract("value", pattern, 2).alias("timestamp"),
    F.regexp_extract("value", pattern, 3).alias("method"),
    F.regexp_extract("value", pattern, 4).alias("url"),
    F.regexp_extract("value", pattern, 5).alias("status").cast("int"),
    F.regexp_extract("value", pattern, 6).alias("size").cast("int")
)

# Filtering out non-matching lines
log_df = log_df.filter(
    (F.col("status").isNotNull()) & F.col("size").isNotNull()
)

log_df.show(5, truncate=False)


# DataFrame Operations 
# Counting each unique (status, method) pair and sort desc by count 
status_method_counts_df = (
    log_df.groupBy("status", "method")
    .count()
    .orderBy(F.desc("count"))
)

# print the outcome
print("\n=== DataFrame result:(status, method) counts ===")
status_method_counts_df.show(truncate=False)


# Spark SQL
# Repeating the same operation as above using SQL

log_df.createOrReplaceTempView("logs")

status_method_counts_sql_df = spark.sql("""
SELECT 
    status, 
    method, 
    COUNT(1) as count
FROM 
    logs
GROUP BY 
    status, 
    method
ORDER BY 
    count DESC
""")

# print the outcome
print("\n=== Spark SQL result:(status, method) counts ===")
status_method_counts_sql_df.show(truncate=False)

spark.stop()


# # Keep only 404 error logs
# error_404_logs = log_df.filter(log_df.status == 404)

# # Group by URL and then count, and sort by count in descending order
# url_404_count = error_404_logs.groupBy("url").count().orderBy(F.desc("count"))

# # print the outcome
# url_404_count.show()

# # Register the DataFrame as a temporary SQL table
# log_df.createOrReplaceTempView("logs")

# # Use SparkSQL to count URLs with 404 status
# url_404_count = spark.sql("""
# SELECT url, COUNT(1) as count
# FROM logs
# WHERE status = 404
# GROUP BY url
# ORDER BY count DESC
# """)
# url_404_count.show()
