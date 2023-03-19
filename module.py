from pyspark import SparkContext
from sys import stdin

sc: SparkContext = SparkContext("local[*]", "wordcount")
sc.setLogLevel("INFO")
inputData = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 9/DataSets/search_data.txt")
words = inputData.flatMap(lambda x: x.split(" "))
word_counts = words.map(lambda x: (x, 1))
final_count = word_counts.reduceByKey(lambda x, y: x + y)
result = final_count.collect()

for re in result:
    print(re)

stdin.readline()
#     println {
#       "harsh"
#    }
