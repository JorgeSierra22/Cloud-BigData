from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, max as spark_max

conf = SparkConf().setAppName('CrimeSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'your_crime_data.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Selecciona las columnas relevantes, en este caso, 'Date' y 'District'
df = crime_df.select("Date", "District")

# Extrae la hora del día de la columna 'Date'
df = df.withColumn("HourOfDay", hour(col("Date")))

# Cuenta la frecuencia de crímenes por distrito y hora del día
crime_counts_df = df.groupBy("District", "HourOfDay").count()

# Encuentra el distrito más problemático por hora del día
most_problematic_district_df = crime_counts_df.groupBy("HourOfDay").agg(
    col("HourOfDay"),
    col("District"),
    spark_max("count").alias("MaxCrimesCount")
)

# Ordena el DataFrame por hora del día y distrito
most_problematic_district_df = most_problematic_district_df.orderBy("HourOfDay", "District")

# Escribe los resultados en archivos CSV separados por hora del día
for hour_row in most_problematic_district_df.collect():
    hour_of_day = hour_row["HourOfDay"]
    hour_district_df = most_problematic_district_df.filter(col("HourOfDay") == hour_of_day)
    hour_district_df.write.csv(f"most_problematic_district_hour_{hour_of_day}")
