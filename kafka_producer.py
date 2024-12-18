from kafka import KafkaProducer
import requests
import json
import time

API_KEY = 'your_api_key'
STOCK_SYMBOL = 'TSLA'
KAFKA_TOPIC = 'stock_prices'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    response = requests.get(f'https://www.alphavantage.co/query',
                            params={
                                'function': 'TIME_SERIES_INTRADAY',
                                'symbol': STOCK_SYMBOL,
                                'interval': '1min',
                                'apikey': API_KEY
                            })
    data = response.json()
    producer.send(KAFKA_TOPIC, value=data)
    time.sleep(60)
