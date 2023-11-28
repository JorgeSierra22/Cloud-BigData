from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, to_timestamp

conf = SparkConf().setAppName('CrimeSummary')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)

# Reemplaza 'your_crime_data.csv' con la ruta real de tu archivo de datos de crímenes
crime_df = spark.read.option("header", "true").csv("Crimes_-_2001_to_Present.csv")

# Selecciona la columna 'Date'
df = crime_df.select("Date")

# Convierte la columna 'Date' al formato de fecha y hora
df = df.withColumn("Timestamp", to_timestamp(df["Date"], "MM/dd/yyyy hh:mm:ss a"))

# Extrae la hora del día de la columna 'Timestamp'
df = df.withColumn("HourOfDay", hour(df["Timestamp"]))

# Cuenta la frecuencia de crímenes por hora del día
crime_counts_df = df.groupBy("HourOfDay").count()

# Ordena el DataFrame por hora del día
crime_counts_df = crime_counts_df.orderBy("HourOfDay")

# Escribe los resultados en un archivo CSV
crime_counts_df.write.csv("crime_counts_by_hour")
