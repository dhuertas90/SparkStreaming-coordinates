from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys 

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio3")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 15)
ssc.checkpoint("/user/hduser/TP-3/buffer-ej3")
stream = ssc.socketTextStream("localhost", 7777)

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

def calcular_top10(tup):
	rdd_a = tup.reduceByKey(lambda x,y: x+y)
	rdd_top10 = rdd_a.takeOrdered(10, key = lambda x:-x[1])
	print(rdd_top10)

#Operar
rdd_lineas = stream.map(lambda linea: linea.split(";"))
data_set = rdd_lineas.filter(lambda linea: condicion(linea))
rdd_destinos = rdd_filtrado.map(lambda linea: ((linea[1] , linea[2]), 1))
rdd_resumen = rdd_destinos.reduceByKey(lambda x,y: x + y )

history = rdd_resumen.updateStateByKey(fUpdate)

#--Imprimir TOP 10--
history.foreachRDD(calcular_top10)

ssc.start()
ssc.awaitTermination()