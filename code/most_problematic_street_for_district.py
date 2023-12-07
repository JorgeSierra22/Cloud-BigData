from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, concat_ws, collect_set, upper, count, desc, first

# Configuración de Spark
conf = SparkConf().setAppName('MostFrequentStreetByDistrict')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'Crimes_-_2001_to_Present.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Divide el campo 'Block' por espacios y selecciona los elementos 2 y 3
crime_df = crime_df.withColumn("BlockArray", split(col("Block"), " "))
crime_df = crime_df.withColumn("Street", concat_ws(" ", col("BlockArray")[2], col("BlockArray")[3]))

# Convierte todas las letras a mayúsculas en la columna 'Street'
crime_df = crime_df.withColumn("Street", upper(col("Street")))

# Filtra las filas donde la columna 'Street' no es vacía
filtered_df = crime_df.filter(col("Street") != "")

# Agrupa por 'District' y cuenta la frecuencia de cada calle
street_count_by_district_df = filtered_df.groupBy("District", "Street").agg(count("Street").alias("StreetCount"))

# Encuentra la calle más frecuente por distrito
most_frequent_street_by_district_df = street_count_by_district_df.orderBy("StreetCount", ascending=False).groupBy("District").agg(
    first("Street").alias("MostFrequentStreet"),
    first("StreetCount").alias("Count")
)

# Escribe los resultados en un archivo CSV
most_frequent_street_by_district_df.write.csv("most_frequent_street_by_district")

# Detén la sesión de Spark
spark.stop()
