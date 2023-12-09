from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.functions import col, count
# Configuración de Spark  spark-submit crimesWithArrest.py
conf = SparkConf().setAppName('ArrestedCrimesSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'Crimes_-_2001_to_Present.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Filtra los delitos en los que se produce un arresto
arrested_crimes_df = crime_df.filter(col("Arrest") == "true")

# Selecciona las columnas relevantes, en este caso, 'Primary Type' y 'Year'
df = arrested_crimes_df.select("Primary Type", "Year")

# Agrupa por 'Primary Type', cuenta la frecuencia de arrestos por tipo de delito
#arrested_crime_counts_df = df.groupBy("Primary Type").agg(spark_sum("Year").alias("TotalArrestedCrimes"))
arrested_crime_counts_df = arrested_crimes_df.groupBy("Primary Type").agg(count("*").alias("TotalArrestedCrimes"))

# Escribe los resultados en un archivo CSV  more arrested_crimes_summary/*
arrested_crime_counts_df.write.csv("arrested_crimes_summary")
