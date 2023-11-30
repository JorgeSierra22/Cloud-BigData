from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

conf = SparkConf().setAppName('CrimeSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'your_crime_data.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Selecciona las columnas relevantes, en este caso, 'Year' y 'Primary Type'
df = crime_df.select("Year", "Primary Type")

# Agrupa por 'Primary Type' y un nuevo campo 'Year_Group' que representa el rango de 5 años
df = df.withColumn('Year_Group', ((df['Year'] - 1) / 5).cast('int') * 5 + 1)

# Cuenta la frecuencia de crímenes por tipo y rango de 5 años
crime_counts_df = df.groupBy("Primary Type", "Year_Group").count()

# Encuentra el delito más repetido en cada rango de 5 años
most_repeated_crime_df = crime_counts_df.groupBy("Year_Group").agg(
    spark_max("count").alias("Max_Count"),
    col("Primary Type").first().alias("Most_Repeated_Crime")
)

# Escribe los resultados en un archivo CSV
most_repeated_crime_df.write.csv("most_repeated_crime")
