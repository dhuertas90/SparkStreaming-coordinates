from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys 

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio4")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 15)
ssc.checkpoint("/user/hduser/TP-3/buffer-ej4")
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

#Operar
rdd_lineas = stream.map(lambda linea: linea.split(";"))

#Calcular e Imprimir listado de Franjas+Cantidades
intervalo=500
rdd_t_c = rdd_lineas.map(lambda linea: ((int(linea[3]) // intervalo), 1) )
rdd_sumatoria = rdd_t_c.reduceByKey(lambda x,y: x+y)

history = rdd_sumatoria.updateStateByKey(fUpdate)

history.pprint()

ssc.start()
ssc.awaitTermination()