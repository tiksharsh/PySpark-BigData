from pyspark import SparkContext

# Set the log level to only print errors
sc = SparkContext("local[*]", "LogLevelCount")
sc.setLogLevel("WARN")
# Create a SparkContext using every core of the local machine
base_rdd = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 10/DataSets/bigLog.txt")

mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0], x.split(":")[1]))
grouped_rdd = mapped_rdd.groupByKey()
final_rdd = grouped_rdd.map(lambda x: (x[0], len(x[1])))

result = final_rdd.collect()

for x in result:
    print(x)
