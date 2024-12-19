from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, window, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StockPriceProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Read data stream from Kafka topic
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the Kafka messages (JSON format)
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .selectExpr(
        "get_json_object(json_data, '$.symbol') as symbol",
        "get_json_object(json_data, '$.timestamp') as timestamp",
        "CAST(get_json_object(json_data, '$.close') AS DOUBLE) as close"
    )

# Calculate a 5-minute moving average
moving_avg = parsed_stream.groupBy(
    "symbol",
    window("timestamp", "5 minutes").alias("time_window")
).agg(
    avg("close").alias("moving_avg_close")
)

# Write the processed stream to the console
query = moving_avg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
