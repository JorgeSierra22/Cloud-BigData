from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, split, concat_ws
import sys

# Configuración de Spark
conf = SparkConf().setAppName('ArrestedCrimesSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Obtén los argumentos de la línea de comandos
crime_type = sys.argv[1]
year = sys.argv[2]

# Reemplaza 'Crimes_-_2001_to_Present.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Filtra el DataFrame para el año y tipo de delito específicos
filtered_df = crime_df.filter((col("Year") == year) & (col("Primary Type") == crime_type))

# Divide el campo 'Block' por espacios y selecciona los elementos 2 y 3
filtered_df = filtered_df.withColumn("BlockArray", split(col("Block"), " "))
filtered_df = filtered_df.withColumn("Street", concat_ws(" ", col("BlockArray")[2], col("BlockArray")[3]))

# Agrupa por calle y cuenta los delitos
result_df = filtered_df.groupBy("Street").agg(count("*").alias("CrimeCount")).orderBy("CrimeCount", ascending=False)

# Escribe los resultados en un archivo CSV 
result_df.write.csv("grep_street")
