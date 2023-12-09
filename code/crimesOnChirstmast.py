from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count ,dayofmonth , month

# Configuración de Spark
conf = SparkConf().setAppName('ChristmasCrimeCount')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'Crimes_-_2001_to_Present.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Añade una columna 'Day' a partir de 'Date'
crime_df = crime_df.withColumn("Day", dayofmonth("Date")).withColumn("Month", month("Date"))


# Filtra los datos para incluir solo los delitos en Navidad (25 de diciembre)
christmas_crime_df = crime_df.filter((col("Month") == 12) & (col("Day") == 25))


# Agrupa por 'Year' y cuenta los delitos en Navidad
christmas_crime_count_df = christmas_crime_df.groupBy("Year").agg(count("ID").alias("ChristmasCrimeCount"))

# Muestra los resultados
christmas_crime_count_df.show()