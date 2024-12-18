from kafka import KafkaProducer
import requests
import json
import time

API_KEY = 'HEBOH95078GF5MAS'
STOCK_SYMBOLS = ['AAPL', 'TSLA', 'GOOGL', 'AMZN']
KAFKA_TOPIC = 'stock_prices'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_data(symbol):
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
    if "Time Series (1min)" in data:
        return data["Time Series (1min)"]
    elif "Information" in data:
        print(f"API limit reached: {data['Information']}")
        return None
    else:
        print(f"Unexpected response for {symbol}: {data}")
        return None

while True:
    for symbol in STOCK_SYMBOLS:
        try:
            data = fetch_data(symbol)
            if data:
                producer.send(KAFKA_TOPIC, value={'symbol': symbol, 'data': data})
                print(f"Data sent for {symbol}")
            else:
                print(f"Skipping {symbol} due to API limit.")
                time.sleep(3600)  # Pause for 1 hour if limit is hit
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
        time.sleep(60 / len(STOCK_SYMBOLS))  # Adjust sleep to balance requests
