from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, year

# Configuración de Spark
conf = SparkConf().setAppName('MonthlyCrimeCount')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'Crimes_-_2001_to_Present.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Añade columnas 'Month' y 'Year' a partir de 'Date'
crime_df = crime_df.withColumn("Month", month("Date")).withColumn("Year", year("Date"))

# Agrupa por 'Year' y 'Month', y cuenta los delitos
monthly_crime_count_df = crime_df.groupBy("Year", "Month").agg(count("ID").alias("MonthlyCrimeCount"))

# Muestra los resultados
monthly_crime_count_df.show()
