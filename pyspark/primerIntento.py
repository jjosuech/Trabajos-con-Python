from pyspark.sql import SparkSession
#    .master("local[*]") \ antes esto estaba debajo del app.name
# Crear sesión de Spark en modo local
spark = SparkSession.builder \
    .appName("PruebaPySpark") \
    .getOrCreate()

# DataFrame de prueba
data = [("Josue", 27), ("Ana", 30)]
df = spark.createDataFrame(data, ["Nombre", "Edad"])

df.show()

spark.stop()