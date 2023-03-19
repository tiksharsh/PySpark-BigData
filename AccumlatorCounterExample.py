from pyspark import SparkContext


def blankLineChecker(line):
    if (len(line) == 0):
        myaccum.add(1)


sc = SparkContext("local[*]", "AccumulatorExample")
myrdd = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 10/DataSets/samplefile.txt")
myaccum = sc.accumulator(0.0)

myrdd.foreach(blankLineChecker)
print(myaccum.value)
