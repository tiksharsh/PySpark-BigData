from pyspark import SparkContext

sc = SparkContext("local[*]", "KeywordAmount")
initial_rdd = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 10/DataSets/bigdata-campaign-data.csv")

mapped_input = initial_rdd.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))
words = mapped_input.flatMapValues(lambda x: x.split(" "))

final_mapped = words.map(lambda x: (x[1].lower(), x[0]))
total = final_mapped.reduceByKey(lambda x, y: x + y)

sorted = total.sortBy(lambda x: x[1], False)
result = sorted.take(20)

for x in result:
    print(x)
