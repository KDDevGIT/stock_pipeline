from kafka import KafkaProducer
import yfinance as yf
import json
import time

# Used as an alternative for AlphaVantage that has API Pull Limit

# Configuration
STOCK_SYMBOL = 'AAPL'  # Replace with the stock symbol of your choice
KAFKA_TOPIC = 'stock_prices'
KAFKA_SERVER = 'localhost:9092'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

def fetch_stock_data(symbol):
    """Fetch the latest stock data for the given symbol."""
    stock = yf.Ticker(symbol)
    data = stock.history(period="1d", interval="1m")
    if not data.empty:
        latest = data.iloc[-1]  # Get the latest row of data
        return {
            "symbol": symbol,
            "timestamp": latest.name.strftime('%Y-%m-%d %H:%M:%S'),
            "open": latest['Open'],
            "high": latest['High'],
            "low": latest['Low'],
            "close": latest['Close'],
            "volume": latest['Volume']
        }
    return None

# Fetch and send stock data
while True:
    try:
        # Fetch the latest stock data
        stock_data = fetch_stock_data(STOCK_SYMBOL)
        if stock_data:
            # Send data to Kafka
            producer.send(KAFKA_TOPIC, value=stock_data)
            print(f"Data sent to Kafka: {stock_data}")
        else:
            print(f"No data available for {STOCK_SYMBOL}.")

        # Wait for 1 minute before fetching again
        time.sleep(60)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)  # Wait before retrying in case of error
