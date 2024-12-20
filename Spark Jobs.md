### Note: Please see 'Spark config.md' to configure Apache Spark and Hadoop
### Note: Before submitting Spark Jobs, Make sure docker container is running!
### Please see 'Starting The Containder.md' before submitting spark jobs

### Producers
<ins>stock_pipeline</ins>
kafka_producer_yfinance.py #pulls SINGLE API stock data calls. Can change ticker here. 
kafka_producer_yfinance_m.py #pulls MULTIPLE API stock data calls. Can change # of stocks and tickers here. 

### Apache Spark jobs and queries
<ins>Moving Avg Per Batch</ins>
- uses kafka_producer_yfinance.py or kafka_producer_yfinance_m.py
- Moving Average over single or multiple batches from Kafka
- producer pulls API Data, Spark data processor processing the data as a rolling average PTP. 
- Results in batches, processor can handle single or multiple stocks depending on the Kafka producer. 
- run: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_processor.py
- execute: run_spark_avg.py to easily run this command for both single and mutliple stocks.

<ins>Single and Multiple Real-Time Stock Plot</ins>
- uses kafka_producer_yfinance.py or kafka_producer_yfinance_m.py
- Plots real-time ingested data as a line graph in Plotly-Dash
- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 plotly_dash.py
- execute run_spark_plotly.py to easily run this command for single stocks from producer. 
- execute run_spark_plotly_m.py to easily run this command for multiple stocks from producer. 
