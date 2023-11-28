from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

conf = SparkConf().setAppName('StockSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

stock_df = spark.read.option("header", "true").csv("GOOGLE.csv")

df = stock_df.withColumn("Year", col("Date").substr(1, 4).cast("int"))

df = df.select("Year", col("Close").cast("double"))

average_price_df = df.groupBy("Year").agg({"Close": "avg"})

average_price_df.write.csv("exercise3.txt")

