from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear sesión de Spark en modo local
spark = SparkSession.builder \
    .appName("EjemploPySparkLocal") \
    .master("local[*]") \
    .getOrCreate()

# Crear un DataFrame de ejemplo
datos = [
    ("Juan", 25),
    ("Ana", 30),
    ("Luis", 19),
    ("María", 40)
]

columnas = ["Nombre", "Edad"]

df = spark.createDataFrame(datos, columnas)

# Mostrar el DataFrame original
print("=== DataFrame original ===")
df.show()

# Filtrar personas mayores de 25 años
df_filtrado = df.filter(col("Edad") > 25)

print("=== Personas con edad > 25 ===")
df_filtrado.show()

# Agregar una nueva columna con 5 años más
df_con_extra = df.withColumn("EdadFutura", col("Edad") + 5)

print("=== DataFrame con columna EdadFutura ===")
df_con_extra.show()

# Detener la sesión de Spark
spark.stop()
