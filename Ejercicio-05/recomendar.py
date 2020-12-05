from pyspark import SparkConf, SparkContext
import sys 

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio5-Recomendar")
sc = SparkContext(conf = conf)

#Input:
#R = sc.textFile("/user/hduser/TP-3/datos")

#En este caso crearemos un input predefinido, que representa el resultado del script ./recomendacion.py
datos = [(('ID_V1', 'ID_D1'), 10), (('ID_V1', 'ID_D2'), 3), (('ID_V1', 'ID_D3'), 23), (('ID_V2', 'ID_D1'), 12), (('25', 'ID_D2'), 2)]
R = sc.parallelize(datos)
"""
  R:
	((ID_V1, ID_D1), 10)
	((ID_V1, ID_D2), 3)
	((ID_V1, ID_D3), 23)
	((ID_V2, ID_D1), 12)
	((25, ID_D2), 2)
"""

#Input T:
#T = sc.textFile("/user/hduser/TP-3/recomendacion_T")
datosT = [('ID_D1', 5), ('ID_D2', 6), ('ID_D3', 7)]
T = sc.parallelize(datosT)
"""
	(ID_D1, val)
	(ID_D2, val)
	(ID_D3, val)
"""

#U = sc.textFile("/user/hduser/TP-3/recomendacion_U")
datosU = [('ID_V1', 3), ('ID_V2', 2), ('25', 1)]
U = sc.parallelize(datosU)
"""
	(ID_V1, val)
	(ID_V2, val)
	(25, val)
"""

#Vehiculo
id_v = '25'

#Filtro + Mappers
rdd_t = R.filter(lambda linea: linea[0][0]==id_v)
"""
	((25, ID_D2), 2)
"""

rdd_destinos_conocidos = rdd_t.map(lambda linea: (linea[0][1], linea[1]) )
"""
	(ID_D2, 2)
"""

#Destinos desconocidos
rdd_destinos_desconocidos = T.subtractByKey(rdd_destinos_conocidos)
"""
	(ID_D1, val)
	(ID_D3, val)
"""

#Realizar Recomendacion al destino desconocido
#Filtrar por Vehiculo
rdd_u = U.filter(lambda linea: linea[0]==id_v)
"""
	(25, val)
"""

#Recuperacion de info Desconocidos, a traves de ID conocidos
# 				(ID_D1, val)
# (25, val) X 	(ID_D3, val)
rdd_u_cartesiano_desconocidos = rdd_u.cartesian(rdd_destinos_desconocidos)
"""
	( (25, val1), (ID_D1, val2) )
	( (25, val1), (ID_D3, val2) )
"""

#Realizar recomendacion
rdd_destinos_valores = rdd_u_cartesiano_desconocidos.map(lambda linea: (linea[1][0], (linea[0][1] * linea[1][1]) ) )
"""
	(ID_D1, val1*val2)
	(ID_D3, val1*val2)
"""

#Ordenar por valor y obtener maximo
#Imprime: (destinoRecomendado, valMax)
rdd_destinos_valores.takeOrdered(1, key = lambda x:-x[1])