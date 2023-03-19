# This is a sample Python script.
from pyspark import SparkContext
from sys import stdin


def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numOfFriends = int(fields[3])
    return age, numOfFriends


sc: SparkContext = SparkContext("local[*]", "wordcount")
sc.setLogLevel("ERROR")
lines = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 9/DataSets/friendsdata.csv")

rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

avgByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])
result = avgByAge.collect()

for re in result:
    print(re)
