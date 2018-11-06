from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json

master = 'local'
spark = SparkSession \
    .builder \
    .appName("test") \
    .master(master) \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkCotext

# filename = 'hdfs://spark1:9000/test_dat/helloworld.txt'
df = spark.read.json("C://Users/CTWLPC/Desktop/people.json")

words = df.flatMap( lambda line: line.split(" "))

# pairs = words.map( word => (word, 1) )  
pairs = words.map( lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y : x + y)

wordCounts.print()

