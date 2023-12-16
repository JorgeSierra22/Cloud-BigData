#BUCKET=gs://$GOOGLE_CLOUD_PROJECT
#gcloud dataproc jobs submit pyspark --cluster example-cluster --region=europe-west6 $BUCKET/typeOfCrimes5.1.py -- $BUCKET/Crimes_-_2001_to_Present.csv $BUCKET/most_repeated_crime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os
import sys

# Configuración de Spark
conf = SparkConf().setAppName('CrimeSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

crime_df = spark.read.option("header", "true").csv(sys.argv[1])

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

# Directorio de resultados
output_dir = sys.argv[2] #"most_repeated_crime"

# Crea el directorio si no existe
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Escribe los resultados en archivos CSV separados para cada intervalo dentro del directorio 'resultados'
for interval in set(df.select("Interval").rdd.flatMap(lambda x: x).collect()):
    interval_df = crime_counts_df.filter(col("Interval") == interval)
    interval_df.write.csv(os.path.join(output_dir, f"most_repeated_crime_{interval}"))
