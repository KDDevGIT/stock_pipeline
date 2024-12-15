#Primary producer
from kafka import KafkaProducer
import requests
import json
import time

# Configuration
API_KEY = "HEBOH95078GF5MAS"  # Replace with your API key
SYMBOL = "AAPL"  # Stock symbol (e.g., Apple)
KAFKA_TOPIC = "stock-data"  # Kafka topic name
BROKER = "kafka:9092"  # Kafka broker address

def get_stock_data():
    """
    Fetches real-time stock price data using Alpha Vantage API.
    """
    try:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval=1min&apikey={API_KEY}"
        response = requests.get(url)
        data = response.json()

        if "Time Series (1min)" not in data:
            print("Error fetching data. Check API key or quota limit.")
            return None

        return data["Time Series (1min)"]
    except Exception as e:
        print(f"Error: {e}")
        return None

def produce_messages():
    """
    Sends stock data to a Kafka topic.
    """
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Starting to produce messages to Kafka...")

    try:
        while True:
            stock_data = get_stock_data()
            if stock_data:
                for timestamp, values in stock_data.items():
                    message = {
                        "timestamp": timestamp,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"]),
                    }
                    producer.send(KAFKA_TOPIC, message)
                    print(f"Produced: {message}")
                    time.sleep(1)  # Delay between messages
            time.sleep(60)  # Fetch new data every minute
    except KeyboardInterrupt:
        print("Stopping producer...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    produce_messages()

