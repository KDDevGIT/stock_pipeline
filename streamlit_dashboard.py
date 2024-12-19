import streamlit as st
from kafka import KafkaConsumer
import pandas as pd
import json
import time

# Kafka Configuration
TOPIC = "stock_prices"
BROKER = "localhost:9092"

# Initialize Data Storage
data = {"timestamp": [], "symbol": [], "close": []}
df = pd.DataFrame(data)

# Kafka Consumer Setup
def consume_kafka_data(consumer):
    global df
    for message in consumer:
        record = message.value
        symbol = record.get("symbol")  # Stock symbol
        timestamp = record.get("timestamp")  # Timestamp
        close_price = record.get("close")  # Closing price
        
        if symbol and timestamp and close_price:
            # Append the new data point
            new_row = {"timestamp": timestamp, "symbol": symbol, "close": close_price}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        time.sleep(0.1)  # Slight delay for throttling


# Start Streamlit Dashboard
st.title("Real-Time Stock Prices")
st.subheader("Price Over Time")

# Real-Time Line Chart
chart = st.empty()

# Start Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# Background Task for Data Consumption
while True:
    consume_kafka_data(consumer)
    
    if not df.empty:
        # Convert timestamp to datetime and sort
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df_sorted = df.sort_values(by="timestamp")
        
        # Display real-time line chart
        latest_data = df_sorted.set_index("timestamp")
        chart.line_chart(latest_data["close"])
    
    time.sleep(1)  # Refresh rate for the Streamlit dashboard
