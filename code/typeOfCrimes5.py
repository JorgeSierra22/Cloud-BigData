from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Configuración de Spark
conf = SparkConf().setAppName('CrimeSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Selecciona las columnas relevantes, en este caso, 'Year' y 'Primary Type'
df = crime_df.select("Year", "Primary Type")

# Define los límites de los intervalos
intervals = [(2001, 2006), (2007, 2012), (2013, 2018), (2019, 2023)]

# Añade una columna 'Interval' que indica a qué intervalo pertenece cada año
df = df.withColumn("Interval", when((col("Year") >= intervals[0][0]) & (col("Year") <= intervals[0][1]), "2001-2006")
                              .when((col("Year") >= intervals[1][0]) & (col("Year") <= intervals[1][1]), "2007-2012")
                              .when((col("Year") >= intervals[2][0]) & (col("Year") <= intervals[2][1]), "2013-2018")
                              .when((col("Year") >= intervals[3][0]) & (col("Year") <= intervals[3][1]), "2019-2023")
                              .otherwise("Unknown"))

# Filtra y cuenta la frecuencia de crímenes por intervalo
crime_counts_df = df.groupBy("Primary Type", "Interval").count()

# Escribe los resultados en archivos CSV separados para cada intervalo
for interval in set(df.select("Interval").rdd.flatMap(lambda x: x).collect()):
    interval_df = crime_counts_df.filter(col("Interval") == interval)
    interval_df.write.csv(f"most_repeated_crime_{interval}")

