from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys 

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio1")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 15)
ssc.checkpoint("/user/hduser/TP-3/buffer-ej1")
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
data_set = rdd_lineas.filter(lambda linea: condicion(linea))
rdd_group = data_set.map(lambda linea: (linea[0], 1))
resumen = rdd_group.reduceByKey(lambda x,y: x + y )

history = resumen.updateStateByKey(fUpdate)
history.pprint()
ssc.start()
ssc.awaitTermination()