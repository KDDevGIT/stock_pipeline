from confluent_kafka import Producer
import requests
import json
import time

API_KEY = "HEBOH95078GF5MAS"
STOCK_SYMBOL = "TSLA"
URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=1min&apikey={API_KEY}"

# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def fetch_stock_data():
    response = requests.get(URL)
    data = response.json()
    return data.get("Time Series (1min)", {})

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce messages to Kafka
while True:
    stock_data = fetch_stock_data()
    for timestamp, values in stock_data.items():
        message = {
            "timestamp": timestamp,
            "open": values["1. open"],
            "high": values["2. high"],
            "low": values["3. low"],
            "close": values["4. close"],
            "volume": values["5. volume"]
        }
        producer.produce('stock_prices', value=json.dumps(message), callback=delivery_report)
    producer.flush()
    time.sleep(60)
