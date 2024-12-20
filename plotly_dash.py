from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Real-Time Stock Graph") \
    .getOrCreate()

# Read Kafka stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka JSON data
stock_data = kafka_stream.selectExpr("CAST(value AS STRING) as value")
parsed_data = stock_data.select(
    get_json_object(col("value"), "$.symbol").alias("symbol"),
    get_json_object(col("value"), "$.close").alias("close").cast("double"),
    get_json_object(col("value"), "$.timestamp").alias("timestamp")
)

# Initialize plot
fig, ax = plt.subplots()
timestamps = []
prices = []

# Update function for Matplotlib
def update_plot(i):
    ax.clear()
    ax.plot(timestamps, prices, marker="o", label="Stock Price")
    ax.set_title("Real-Time Stock Prices from Kafka")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Close Price")
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

# Function to process and append Kafka data
def process_batch(batch_df, epoch_id):
    global timestamps, prices
    pandas_df = batch_df.toPandas()
    if not pandas_df.empty:
        pandas_df["timestamp"] = pd.to_datetime(pandas_df["timestamp"])
        timestamps.extend(pandas_df["timestamp"].tolist())  # Append new timestamps
        prices.extend(pandas_df["close"].tolist())  # Append new close prices

# Start streaming query
query = parsed_data.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

# Set up Matplotlib animation
ani = FuncAnimation(fig, update_plot, interval=1000)
plt.show()

query.awaitTermination()
