from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, dayofmonth, count, max

# Configuración de Spark
conf = SparkConf().setAppName('CrimeSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'Crimes_-_2001_to_Present.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Añade una columna 'Month' y 'Day' a partir de 'Date'
crime_df = crime_df.withColumn("Month", month("Date"))
crime_df = crime_df.withColumn("Day", dayofmonth("Date"))

# Agrupa por 'Year' y 'Month', calcula la suma y cuenta los días
monthly_crime_sum_df = crime_df.groupBy("Year", "Month").agg(count("ID").alias("TotalCrimes"), max("Day").alias("DaysInMonth"))

# Calcula la media (delitos por día)
monthly_crime_avg_df = monthly_crime_sum_df.withColumn("AvgCrimesPerDay", col("TotalCrimes") / col("DaysInMonth"))

# Encuentra el mes con más delitos cada año
most_crime_month_df = monthly_crime_avg_df.groupBy("Year").agg(max("Month").alias("MostCrimeMonth"))

# Muestra los resultados
most_crime_month_df.write.csv("most_problematic_month_per_year")

