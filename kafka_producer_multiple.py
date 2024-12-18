from kafka import KafkaProducer
import requests
import json
import time

# Alpha Vantage API configurations
API_KEY = 'HEBOH95078GF5MAS'  # Replace with your API key
STOCK_SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'TSLA']  # Add more stock symbols as needed
KAFKA_TOPIC = 'stock_prices'
KAFKA_SERVER = 'localhost:9092'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fetch stock data for multiple symbols and send to Kafka
while True:
    for symbol in STOCK_SYMBOLS:
        try:
            # Fetch stock data from Alpha Vantage
            response = requests.get(
                'https://www.alphavantage.co/query',
                params={
                    'function': 'TIME_SERIES_INTRADAY',
                    'symbol': symbol,
                    'interval': '1min',
                    'apikey': API_KEY
                }
            )
            data = response.json()

            # Check if the response contains valid data
            if "Time Series (1min)" in data:
                # Prepare a message with the symbol and data
                producer.send(KAFKA_TOPIC, value={'symbol': symbol, 'data': data["Time Series (1min)"]})
                print(f"Data sent for {symbol}")
            else:
                print(f"Invalid response for {symbol}: {data}")

            # Wait to avoid exceeding API limits
            time.sleep(60 / 25)  # 25 requests per minute
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            time.sleep(5)  # Wait before retrying
