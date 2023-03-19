from pyspark import SparkContext

sc = SparkContext("local[*]", "logLevelCount")
sc.setLogLevel("INFO")

if __name__ != "__main__":
    original_logs_rdd = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 10/DataSets/bigLog.txt")
    print("inside the else part")
else:
    my_list = ["WARN: Tuesday 4 September 0405",
               "ERROR: Tuesday 4 September 0408",
               "ERROR: Tuesday 4 September 0408",
               "ERROR: Tuesday 4 September 0408",
               "ERROR: Tuesday 4 September 0408",
               "ERROR: Tuesday 4 September 0408"]
    original_logs_rdd = sc.parallelize(my_list)

new_pair_rdd = original_logs_rdd.map(lambda x: (x.split(":")[0], 1))
resultant_rdd = new_pair_rdd.reduceByKey(lambda x, y: x + y)
result = resultant_rdd.collect()
for x in result:
    print(x)
