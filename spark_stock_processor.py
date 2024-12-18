from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Spark session with Delta Lake configurations
spark = SparkSession.builder \
    .appName("StockPriceStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema for incoming stock price data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Read data from Kafka topic
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .load()

# Parse Kafka message values and apply schema
processed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .selectExpr(f"from_json(value, '{schema.json()}') AS data") \
    .select("data.*")

# Write the processed data to Delta Lake
query = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "file:///C:/Users/kdabc/stock_pipeline/checkpoints/") \
    .start("file:///C:/Users/kdabc/stock_pipeline/delta/")

# Wait for termination
query.awaitTermination()

processed_df.write.format("delta").mode("overwrite").save("file:///C:/Users/kdabc/stock_pipeline/delta")

