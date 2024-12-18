from kafka import KafkaProducer
import requests
import json
import time

# Alpha Vantage API configurations
API_KEY = 'HEBOH95078GF5MAS'  # Replace with your API key
STOCK_SYMBOL = 'AAPL'  # You can use other stock symbols as needed
KAFKA_TOPIC = 'stock_prices'
KAFKA_SERVER = 'localhost:9092'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fetch stock data and send to Kafka
while True:
    try:
        # Fetch stock data from Alpha Vantage
        response = requests.get(
            'https://www.alphavantage.co/query',
            params={
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': STOCK_SYMBOL,
                'interval': '1min',
                'apikey': API_KEY
            }
        )
        data = response.json()

        # Send to Kafka topic
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Data sent for {STOCK_SYMBOL}")

        # Wait to avoid exceeding API limits
        time.sleep(60 / 25)  # 25 requests per minute
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)  # Wait before retrying
