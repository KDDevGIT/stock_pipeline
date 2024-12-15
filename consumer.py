from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define Kafka and schema
KAFKA_TOPIC = "stock-data"
BROKER = "kafka:9092"

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# Start Spark session
spark = SparkSession.builder \
    .appName("StockDataProcessor") \
    .getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Parse JSON messages
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Compute moving average of the closing price
moving_avg_df = parsed_df.groupBy("timestamp") \
    .agg(avg("close").alias("moving_avg"))

# Write output to console or storage
query = moving_avg_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

