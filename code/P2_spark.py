from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

conf = SparkConf().setAppName('CrimeSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'your_crime_data.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Selecciona las columnas relevantes, en este caso, 'Year' y 'District'
df = crime_df.select("Year", "District")

# Cuenta la frecuencia de crímenes por distrito
crime_counts_df = df.groupBy("District", "Year").count()

# Encuentra el distrito más problemático por año
most_problematic_district_df = crime_counts_df.groupBy("Year", "District").agg({"count": "max"})

# Encuentra el distrito más problemático globalmente
most_problematic_district_global = most_problematic_district_df.groupBy("Year").agg({"max(count)": "max"})

# Escribe los resultados en un archivo CSV
most_problematic_district_global.write.csv("most_problematic_year")



