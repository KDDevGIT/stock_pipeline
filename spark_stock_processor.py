from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, window

spark = SparkSession.builder \
    .appName("StockPriceProcessing") \
    .getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .load()

df.selectExpr("CAST(value AS STRING)").createOrReplaceTempView("stocks")

result = spark.sql("""
    SELECT avg(price) as moving_avg
    FROM stocks
    GROUP BY window(timestamp, '1 minute')
""")

query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
