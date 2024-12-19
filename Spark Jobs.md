#Apache Spark jobs and queries

#Moving Average over single or multiple batches from Kafka
#producer pulls API Data, Spark data processor processing the data as a rolling average PTP. 
#Results in batches, processor can handle single or multiple stocks depending on the Kafka producer. 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 C:\Users\kdabc\stock_pipeline\spark_processor.py #this will also work