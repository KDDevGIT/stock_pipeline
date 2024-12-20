from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, get_json_object
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd
import threading

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Real-Time Stock Plot") \
    .getOrCreate()

# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data
parsed_data = kafka_stream.select(
    get_json_object(col("value").cast("string"), "$.timestamp").alias("timestamp"),
    get_json_object(col("value").cast("string"), "$.close").cast("double").alias("close"),
    get_json_object(col("value").cast("string"), "$.symbol").alias("symbol")
)

# Initialize plot
fig, ax = plt.subplots()
timestamps = []
prices = []
ticker_symbol = None

# Lock for thread-safe operations
data_lock = threading.Lock()

# Function to update plot
def update_plot(frame):
    with data_lock:
        ax.clear()
        if timestamps and prices:
            # Plot data
            ax.plot(timestamps, prices, marker="o", color="b", label=ticker_symbol)
            
            # Add title and labels
            ax.set_title("Real-Time Stock Prices", fontsize=14, fontweight='bold')
            ax.set_xlabel("Timestamp", fontsize=12)
            ax.set_ylabel("Close Price", fontsize=12)
            
            # Add gridlines
            ax.grid(True, linestyle='--', linewidth=0.5)
            
            # Set x-axis ticks rotation
            plt.xticks(rotation=45, fontsize=10)
            plt.yticks(fontsize=10)
            
            # Add legend with the ticker symbol
            ax.legend(loc="upper left", fontsize=10)
            
            # Adjust layout
            plt.tight_layout()

            # Annotate price change on each data point
            for i in range(1, len(prices)):
                price_change = prices[i] - prices[i - 1]
                color = "green" if price_change > 0 else "red"
                ax.text(
                    timestamps[i], prices[i],
                    f"${price_change:+.2f}",  # Format as currency with sign
                    color=color, fontsize=10, fontweight="bold", ha="center"
                )

# Function to process each batch
def process_batch(batch_df, epoch_id):
    global timestamps, prices, ticker_symbol
    pandas_df = batch_df.toPandas()
    if not pandas_df.empty:
        with data_lock:
            pandas_df["timestamp"] = pd.to_datetime(pandas_df["timestamp"])
            timestamps.extend(pandas_df["timestamp"].dt.strftime("%H:%M:%S").tolist())
            prices.extend(pandas_df["close"].tolist())
            
            # Update the ticker symbol
            if ticker_symbol is None and "symbol" in pandas_df.columns:
                ticker_symbol = pandas_df["symbol"].iloc[0]

# Start the Spark streaming query
def start_query():
    query = parsed_data.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()
    query.awaitTermination()

# Run Spark query in a separate thread
spark_thread = threading.Thread(target=start_query)
spark_thread.daemon = True
spark_thread.start()

# Set up Matplotlib animation
ani = FuncAnimation(fig, update_plot, interval=1000)
plt.show()
