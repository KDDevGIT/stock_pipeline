from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd
import threading

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Real-Time Multi-Stock Plot") \
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
    get_json_object(col("value").cast("string"), "$.symbol").alias("symbol"),
    get_json_object(col("value").cast("string"), "$.timestamp").alias("timestamp"),
    get_json_object(col("value").cast("string"), "$.close").cast("double").alias("close")
)

# Initialize plot
fig = plt.figure(figsize=(10, 6))  # Proper figure initialization
data = {}  # To store data for multiple stocks

# Lock for thread-safe operations
data_lock = threading.Lock()

# Function to update plot
def update_plot(frame):
    with data_lock:
        fig.clf()  # Clear the figure before redrawing
        num_stocks = len(data)
        if num_stocks == 0:
            return

        # Dynamically create subplots
        for i, (symbol, stock_data) in enumerate(data.items(), start=1):
            ax = fig.add_subplot(num_stocks, 1, i)
            if stock_data["timestamps"] and stock_data["prices"]:
                # Plot the stock data
                ax.plot(stock_data["timestamps"], stock_data["prices"], marker="o", label=symbol, color="b")
                
                # Add current closing price in the title
                current_price = stock_data["prices"][-1] if stock_data["prices"] else 0.0
                ax.set_title(f"Real-Time Prices: {symbol} (${current_price:.2f})", fontsize=14, fontweight="bold")
                
                # X and Y labels
                ax.set_xlabel("Timestamp", fontsize=12)
                ax.set_ylabel("Close Price", fontsize=12)
                
                # Grid and legend
                ax.grid(True, linestyle="--", linewidth=0.5)
                ax.legend(loc="upper left", fontsize=10)
                
                # Annotate price changes
                for j in range(1, len(stock_data["prices"])):
                    price_change = stock_data["prices"][j] - stock_data["prices"][j - 1]
                    color = "green" if price_change > 0 else "red"
                    ax.text(
                        stock_data["timestamps"][j], stock_data["prices"][j],
                        f"${price_change:+.2f}",
                        color=color, fontsize=9, fontweight="bold", ha="center"
                    )
                # Adjust x-axis tick rotation
                plt.setp(ax.get_xticklabels(), rotation=45, fontsize=8)

        # Automatically adjust layout for better visibility
        fig.tight_layout()

# Function to process each batch
def process_batch(batch_df, epoch_id):
    global data
    pandas_df = batch_df.toPandas()
    if not pandas_df.empty:
        with data_lock:
            pandas_df["timestamp"] = pd.to_datetime(pandas_df["timestamp"]).dt.strftime("%H:%M:%S")
            for symbol in pandas_df["symbol"].unique():
                if symbol not in data:
                    # Initialize data for the new symbol
                    data[symbol] = {"timestamps": [], "prices": []}
                # Update data
                symbol_data = pandas_df[pandas_df["symbol"] == symbol]
                data[symbol]["timestamps"].extend(symbol_data["timestamp"].tolist())
                data[symbol]["prices"].extend(symbol_data["close"].tolist())

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
