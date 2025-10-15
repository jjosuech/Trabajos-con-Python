from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark import SparkContext

spSession= SparkSession.builder\
    .appName("Demo Spark")\
    .getOrCreate()

spContext= spSession.sparkContext

#Carga de Datos:
data=spSession.read.csv('cars.csv', header=True, sep=';')

# Filtrar la fila que no es un registro real
data = data.filter(data.Car != "STRING")

#mostrar los 5 primeros datos ya filtrados con lo de arriba
#data.show(5)

#Conociendo los nombres y tipo de valor de los datos
#data.printSchema()


#conociendo las Columnas que se presentan en los datos
#print(data.columns)


#para poder ver la columna y el tipo de datos
#print(data.dtypes)


#SELECCION DE COLUMNAS (puede ser con una o mas columnas)
#--METODO 1 (agregando dentro del show truncate para ver los datos completos ya que con truncate en true o sin añadir al show se ve los datos pero cortados)
#Respeta la sintaxys del nombre de la Columna
#data.select(data.Car, data.Cylinders).show(truncate=False)


#--NO RESPETA LA las mayusculas DE LA COLUMNA
#METODO 2
#data.select(data['Car'], data['CYLINDERS']).show(truncate=False)

#Metodo3 con col 
#data.select(col('car'), col('cylinders')).show(truncate=False)

#AGREGAR NUEVAS COLUMNAS
#primero vemos la insersion de una columna a nivel visual, no afecta los datos originales
#1 sola columna:
#df=data.withColumn('Firs Column',lit(1))
#df.show(5,truncate=False)

#VARIAS COLUMNAS
#df=data.withColumn('Second_Column',lit(1))\
#    .withColumn('third_column',lit('thir_column'))
#df.show(5,truncate=False)


#agrupar por orgigen y contar SEGUN EL ORIGEN:
#EJEMPLO 1
#df=data.groupBy('Origin').count()
#df.show(5)

#EJEMPLO 2
#df=data.groupBy("Horsepower").count()
#df.show(5)

#EJEMPLO CON MULTIPLES COLUMNAS
"""df=data.groupBy('Origin','Horsepower').count()
df.show(5)

df=data.groupBy('Horsepower', 'Origin').count()
df.show(5)"""



#ELIMINACION DE COLUMNAS:
#df=data.drop('Second_Column','third_column')
#df.show


#ORDENAR FILAS
#ASCENDENTE
"""df=data.orderBy("Cylinders")
df.show(5, truncate=False)"""

#DESENDENTE
"""df=data.orderBy("Cylinders", ascending=False)
df.show(truncate=False)"""


#CONVIENDO ORDERBY
"""df=data.groupBy('Origin').count().orderBy('count', ascending=False)
df.show(5)"""

#IMPRIMIR CANTIDAD DE REGISTROS
"""df=data.count()
print("La cantidad de registros que hay en la data es :", df)


#Filtrar cantidad de registros por pais
Europa=data.filter(col('Origin')=="Europe").count()
print("La cantidad de registroos de Europa es:", Europa)

mostrarxPais=data.filter(col("Origin")=="Europe")
mostrarxPais.show(truncate=False)"""






#IMPRIMIR CANTIDAD DE REGISTROS
df=data.count()
print("La cantidad de registros que hay en la data es :", df)


#Filtrar cantidad de registros por pais y caballos de fuerza
paisyFurza=data.filter((col("Origin")=="US")&(col("Horsepower")=="175.0")).count()
print("Total de registros por pais y caballos de fuerza son: ", paisyFurza)

showPaisFuerza=data.filter((col("Origin")=="US")&(col("Horsepower")=="175.0"))
showPaisFuerza.show()
