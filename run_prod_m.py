import subprocess

#starts Kafka API pull for SINGLE Stock
subprocess.run(["python", "kafka_producer_yfinance_multiple.py",])