from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "stock_prices"

# Create SparkSession
spark = SparkSession.builder \
    .appName("RealTimeLineGraph") \
    .getOrCreate()

# Define Schema
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True)
])

# Read Data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .load()

# Parse Kafka Messages
stock_data = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Collect Real-Time Data for Visualization
real_time_data = []

def update_data(batch_df, batch_id):
    global real_time_data
    # Collect the batch data
    batch_df_pandas = batch_df.select("timestamp", "close").toPandas()
    real_time_data.append(batch_df_pandas)

query = stock_data.writeStream \
    .foreachBatch(update_data) \
    .start()

# Real-Time Visualization with Matplotlib
def animate(i):
    global real_time_data
    if real_time_data:
        # Combine all collected batches
        combined_df = pd.concat(real_time_data)
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
        combined_df = combined_df.sort_values(by="timestamp")
        
        plt.cla()
        plt.plot(combined_df['timestamp'], combined_df['close'], label="Close Price")
        plt.xlabel("Timestamp")
        plt.ylabel("Close Price")
        plt.title("Real-Time Stock Prices")
        plt.legend(loc="upper left")

# Configure Matplotlib Animation
fig = plt.figure()
ani = FuncAnimation(fig, animate, interval=1000)

plt.show()

query.awaitTermination()
