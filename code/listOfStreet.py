from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, concat_ws, collect_set, upper

# Configuración de Spark
conf = SparkConf().setAppName('StreetListByDistrict')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)


crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Divide el campo 'Block' por espacios y selecciona los elementos 2 y 3
crime_df = crime_df.withColumn("BlockArray", split(col("Block"), " "))
crime_df = crime_df.withColumn("Street", concat_ws(" ", col("BlockArray")[2], col("BlockArray")[3]))

# Convierte todas las letras a mayúsculas en la columna 'Street'
crime_df = crime_df.withColumn("Street", upper(col("Street")))

# Filtra las filas donde la columna 'Street' no es vacía
filtered_df = crime_df.filter(col("Street") != "")

# Agrupa por 'District' y recopila las calles únicas en una lista
street_list_by_district_df = filtered_df.groupBy("District").agg(
    concat_ws(", ", collect_set("Street")).alias("StreetList")
)

# Escribe los resultados en un archivo CSV
street_list_by_district_df.write.csv("street_list_by_district")

