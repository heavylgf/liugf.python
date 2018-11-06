from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("PythonSql") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


df = spark.read.json("examples/src/main/resources/people.json")
df = spark.read.json("C:/Users/CTWLPC/Desktop/people.json")

df.show()

df.printScheam()

df.select("name").show()

df.select(df['name'], df['age'] + 1).show()

df.filter(df['age'] > 21).show()

df.groupBy("age").count().show()

df = spark.sql("SELECT * FROM table")

from pyspark.sql import Row

sc = spark.sparkContext



# 生成RDD


