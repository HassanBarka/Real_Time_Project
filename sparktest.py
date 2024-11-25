from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()

# Set Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "users_created"


# Read data from Kaf
# ka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Extract and convert the value column to a string
messages_df = kafka_df.selectExpr("CAST(value AS STRING) as message")

# Write the output to the console in real-time
query = messages_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination of the stream
query.awaitTermination()