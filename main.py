# This is a sample Python script.
from pyspark import SparkContext
from sys import stdin


# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.

    sc: SparkContext = SparkContext("local[*]", "wordcount")
    sc.setLogLevel("ERROR")
    inputData = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 9/DataSets/search_data.txt")
    words = inputData.flatMap(lambda x: x.split(" "))
    word_counts = words.map(lambda x: (x.lower(), 1))
    # word_counts = words.map(lambda x: (x.lower()))
    # final_count = word_counts.countByValue()
    #
    # print(final_count)
    final_count = word_counts.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0]))
    result = final_count.sortByKey(False).map(lambda x: (x[1], x[0])).collect()

    for re in result:
        print(re)

    # stdin.readline()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
