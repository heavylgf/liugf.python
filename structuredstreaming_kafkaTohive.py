from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json

import os
import sys
# Create a variable for our root path
SPARK_HOME = os.environ.get('SPARK_HOME', None)
# Add the PySpark/py4j to the Python Path
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
# sys.path.insert(0, os.environ.get('HADOOP_HOME',None))

os.environ["HADOOP_USER_NAME"] = "hdfs"

if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local") \
        .appName("structuredstreaming") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092") \
        .option("subscribe", "new_test_on_local") \
        .option("group.id", "hahahah") \
        .option("startingOffsets", "latest") \
        .load()

    topicSchema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ]
    )

    df = lines.select(lines.key.cast("string"), from_json(lines.value.cast("string"), topicSchema).alias("people"))

    dfColumn = df.select("people.name", "people.age")

    dfColumn.createOrReplaceTempView("people")
    sq = "select name, age from people "
    df1 = spark.sql(sq)

    query = df1 \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()


