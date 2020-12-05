from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys 

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio2")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 15)
ssc.checkpoint("/user/hduser/TP-3/buffer-ejercicio-n2")
stream = ssc.socketTextStream("localhost", 7777)
stream2 = ssc.socketTextStream("localhost", 7778)

def fUpdate(newValues, history):
	if(history == None):
		history = 0
	if(newValues == None):
		newValues = 0
	else:
		newValues = sum(newValues)
	return newValues + history

def condicion(l):
	linea = l[4]
	linea = linea.strip("u")
	return (len(linea) > 0)

def calcular_top3(tup):
	rdd_a = tup.reduceByKey(lambda x,y: x+y)
	rdd_top3 = rdd_a.takeOrdered(3, key = lambda x:-x[1])
	print(rdd_top3)

#Operar
rdd_lineas = stream.map(lambda linea: linea.split(";"))
rdd_lineas += stream2.map(lambda linea: linea.split(";")) #2

data_set = rdd_lineas.filter(lambda linea: condicion(linea))
rdd_filtrado = data_set.filter(lambda linea: linea!='Otro')
rdd_destinos = rdd_filtrado.map(lambda linea: (linea[4], 1))
rdd_resumen = rdd_destinos.reduceByKey(lambda x,y: x + y )
"""
(escuela, 10)
(farmacia, 32)
(plaza, 32)
(kiosko, 7)
(planetario, 10)
"""

history = rdd_resumen.updateStateByKey(fUpdate)

#--Imprimir TOP 3--
history.foreachRDD(calcular_top3)

ssc.start()
ssc.awaitTermination()