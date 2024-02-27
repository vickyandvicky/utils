from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder \
    .appName("CassandraConnectionExample") \
    .config("spark.cassandra.connection.host", "your_cassandra_host") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "your_username") \
    .config("spark.cassandra.auth.password", "your_password") \
    .config("spark.cassandra.connection.ssl.enabled", "true") \
    .getOrCreate()

# Read data from Cassandra table
cassandra_table = "your_keyspace.your_table"
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table=cassandra_table, keyspace="your_keyspace") \
    .load()

# Show the data
df.show()

# Perform some operations on the DataFrame
result_df = df.withColumn("new_column", F.col("existing_column") + 1)

# Write the result back to Cassandra
result_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table=cassandra_table, keyspace="your_keyspace") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
