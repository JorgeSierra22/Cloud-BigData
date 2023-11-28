from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

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
most_problematic_district_df = crime_counts_df.groupBy("Year").agg(
    col("Year"),
    col("District"),
    spark_max("count").alias("MaxCrimesCount")
)

# Ordena el DataFrame por año
most_problematic_district_df = most_problematic_district_df.orderBy("Year")

# Escribe los resultados en archivos CSV separados por año
for year_row in most_problematic_district_df.collect():
    year = year_row["Year"]
    year_district_df = most_problematic_district_df.filter(col("Year") == year)
    year_district_df.write.csv(f"most_problematic_district_{year}")
