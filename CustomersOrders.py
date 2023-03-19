# This is a sample Python script.
from pyspark import SparkContext
from sys import stdin

sc: SparkContext = SparkContext("local[*]", "wordcount")
sc.setLogLevel("ERROR")
lines = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 9/DataSets/moviedata.data")
ratings = lines.map(lambda x: (x.split("\t")[2], 1))
result = ratings.reduceByKey(lambda x, y: x + y).collect()


for re in result:
    print(re)


