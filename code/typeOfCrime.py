from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuración de Spark
conf = SparkConf().setAppName('CrimeSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Selecciona las columnas relevantes, en este caso, 'Year' y 'District'
df = crime_df.select("Year", "Primary Type")

# Cuenta la frecuencia de crímenes por distrito
crime_counts_df = df.groupBy("Primary Type", "Year").count()

# Encuentra el distrito más problemático por año
most_problematic_district_df = crime_counts_df.groupBy("Primary Type","Year").agg({"count": "max"})

# Encuentra el distrito más problemático globalmente
most_problematic_district_global = most_problematic_district_df.groupBy("Primary Type","Year").agg({"max(count)": "max"})

# Escribe los resultados en un archivo CSV
most_problematic_district_global.write.csv("most_repeated_crime")



