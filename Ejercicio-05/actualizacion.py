from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys 

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio5-Actualizacion")
sc = SparkContext(conf = conf)


ssc = StreamingContext(sc, 15)
ssc.checkpoint("/user/hduser/TP-3/buffer-ej5-Actualizacion")
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
rdd_filtrado = rdd_lineas.filter(lambda linea: condicion(linea))
rdd_general = rdd_filtrado.map(lambda linea: ((linea[0], linea[4]), 1))
rdd_resul = rdd_general.reduceByKey(lambda x,y: x + y )

history = rdd_resul.updateStateByKey(fUpdate)

#Necesitamos tener en un momento dado el dataset history guardado, para levantarlo en recomendacion.py.
#De manera figurativa la siguiente linea guardaria esta informacion, en python:
#				->	history.saveAsTextFile("/user/hduser/TP-3/datos")
#En Spark Streaming: 
#				->  history.saveAsTextFiles(prefix, [suffix])
history.pprint()
ssc.start()
ssc.awaitTermination()