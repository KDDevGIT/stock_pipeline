from kafka import KafkaProducer
import yfinance as yf
import json
import time

# Configuration
STOCK_SYMBOLS = ['AAPL', 'TSLA', 'GOOGL', 'AMZN']  # Add more stock symbols as needed
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

# Fetch and send stock data for multiple symbols
while True:
    for symbol in STOCK_SYMBOLS:
        try:
            # Fetch stock data for the current symbol
            stock_data = fetch_stock_data(symbol)
            if stock_data:
                # Send data to Kafka
                producer.send(KAFKA_TOPIC, value=stock_data)
                print(f"Data sent to Kafka: {stock_data}")
            else:
                print(f"No data available for {symbol}.")

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

        # Avoid hitting API limits; sleep briefly between requests
        time.sleep(60 / len(STOCK_SYMBOLS))  # Distribute requests across a minute
