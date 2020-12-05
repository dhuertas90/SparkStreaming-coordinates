from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys
import random

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio5-Recomendacion")
sc = SparkContext(conf = conf)

def crearTupla(linea):	
	x = linea[1][2] ** 2
	y = linea[1][2] * linea[1][1]
	return (linea[0], (x,y))
"""
El Input sería el archivo “./datos” que representa un archivo generado por el Streaming en un momento dado. Dentro se encontraría el contenido de la variable 'history'. Realizaríamos el input del archivo de la siguiente manera: dataset = sc.textFile("/user/hduser/TP-3/datos")
Para la resolución crearemos datos predefinidos que representan el input mencionado anteriormente.
"""

datos = [(('ID_V1', 'ID_D1'), 10), (('ID_V1', 'ID_D2'), 3), (('ID_V1', 'ID_D3'), 23), (('ID_V2', 'ID_D1'), 12), (('25', 'ID_D2'), 2)]
R = sc.parallelize(datos)
"""
	R (datos):
			((ID_V1, ID_D1), 10)
			((ID_V1, ID_D2), 3)
			((ID_V1, ID_D3), 23)
			((ID_V2, ID_D1), 12)
			((ID_V3, ID_D2), 2)
"""

U = R.distinct().map(lambda linea: (linea[0][0], random.randint(1,5) ))
"""
	(ID_V1, randU)
	(ID_V1, randU)
	(ID_V1, randU)
	(ID_V2, randU)
	(ID_V3, randU)
"""

T = R.distinct().map(lambda linea: (linea[0][1], random.randint(1,5)))
"""
	(ID_D1, randT)
	(ID_D2, randT)
	(ID_D3, randT)
	(ID_D1, randT)
	(ID_D2, randT)
"""

for x in xrange(1,10):

	#Operar
	rdd_destinos = R.map(lambda linea: (linea[0][1], (linea[0][0], linea[1])) )
	"""
		(ID_D1, (ID_V1, r))
		(ID_D2, (ID_V1, r))
		(ID_D3, (ID_V1, r))
		(ID_D1, (ID_V2, r))
		(ID_D2, (ID_V3, r))
	"""

	rdd_vehiculos=  R.map(lambda linea: (linea[0][0], (linea[0][1], linea[1])) )
	"""
		(ID_V1, (ID_D1, r))
		(ID_V1, (ID_D1, r))
		(ID_V1, (ID_D3, r))
		(ID_V2, (ID_D1, r))
		(ID_V3, (ID_D2, r))
	"""
	
	destinos_vKey = rdd_destinos.map(lambda l: (l[1][0], (l[0], l[1][1])) )
	"('ID_V1', ('ID_D1', r))"
	v_join_T = destinos_vKey.join(U)
	"(ID_V1, ((ID_D1, r), t) )"
	rdd_v_join_U = v_join_T.map(lambda l: (l[1][0][0], (l[0], l[1][0][1], l[1][1])) )
	"""
		(ID_D1, (ID_V1, r, t))
		 (ID_D1, (ID_V1, r, t))
		(ID_D2, (ID_V2, r, t))
		 (ID_D2, (ID_V2, r, t))	
		(ID_D3, (ID_V3, r, t))
		(ID_D1, (ID_V2, r, t))
		 (ID_D1, (ID_V2, r, t))
		(ID_D2, (ID_V3, r, t))
		 (ID_D2, (ID_V3, r, t))
	"""
	rdd_T_calculos = rdd_v_join_U.map(lambda linea: crearTupla(linea))
	""" 
		(ID_D1, (t^2, r*t))
		(ID_D1, (t^2, r*t))
		(ID_D2, (t^2, r*t))
		(ID_D2, (t^2, r*t))
		(ID_D3, (t^2, r*t))
		(ID_D1, (t^2, r*t))
		(ID_D1, (t^2, r*t))
		(ID_D2, (t^2, r*t))
		(ID_D2, (t^2, r*t))
	"""
	rdd_T_sumatorias = rdd_T_calculos.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]) )
	"""
		(ID_D1, (sum t^2, sum r*t))
		(ID_D2, (sum t^2, sum r*t))
		(ID_D3, (sum t^2, sum r*t))
	"""
	T = rdd_T_sumatorias.map(lambda linea: (linea[0], (1/(linea[1][0] + 3.14) * linea[1][1])) ).distinct()
	"""
		(ID_D1, (1/res1)*res2)
		(ID_D2, (1/res1)*res2))
		(ID_D3, (1/res1)*res2))
	"""


	vehiculos_dKey = rdd_vehiculos.map(lambda l: (l[1][0], (l[0], l[1][1]) ))
	"(ID_D1, (ID_V1, r))"
	d_join_T = vehiculos_dKey.join(T)
	"(ID_D1, ((ID_V1, r), u))"
	rdd_d_join_T = d_join_T.map(lambda l: (l[1][0][0], (l[0], l[1][0][1], l[1][1])) )
	"""
		(ID_V1, (ID_D1, r, u))
		 (ID_V1, (ID_D1, r, u))
		 (ID_V1, (ID_D1, r, u))
		(ID_V1, (ID_D1, r, u))
		 (ID_V1, (ID_D1, r, u))
		 (ID_V1, (ID_D1, r, u))
		(ID_V1, (ID_D3, r, u))
		 (ID_V1, (ID_D3, r, u))
		 (ID_V1, (ID_D3, r, u))
		(ID_V2, (ID_D1, r, u))
		(ID_V3, (ID_D2, r, u))
	"""
	rdd_U_calculos = rdd_d_join_T.map(lambda linea: crearTupla(linea))
	""" 
		(ID_V1, (u^2, r*u))
		(ID_V1, (u^2, r*u))
		(ID_V1, (u^2, r*u))
		(ID_V1, (u^2, r*u))
		(ID_V1, (u^2, r*u))
		(ID_V1, (u^2, r*u))
		(ID_V1, (u^2, r*u))
		(ID_V1, (u^2, r*u))
		(ID_V1, (u^2, r*u))
		(ID_V2, (u^2, r*u))
		(ID_V3, (u^2, r*u))
	"""
	rdd_U_sumatorias = rdd_U_calculos.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]) )
	"""
		(ID_V1, (sum u^2, sum r*u))
		(ID_V2, (sum u^2, sum r*u))
		(ID_V3, (sum u^2, sum r*u))
	"""
	U = rdd_U_sumatorias.map(lambda linea: (linea[0], ( 1/(linea[1][0] + 3.14) * linea[1][1]) ) ).distinct()
	"""
		(ID_V1, (1/res1)*res2)
		(ID_V2, (1/res1)*res2)
		(ID_V3, (1/res1)*res2)
	"""

rdd_U.saveAsTextFile("/user/hduser/TP-3/recomendacion_U")
rdd_T.saveAsTextFile("/user/hduser/TP-3/recomendacion_T")
#Guardamos R para utilizarlo en recomendar.py
R.saveAsTextFile("/user/hduser/TP-3/R")