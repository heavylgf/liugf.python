from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json


if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local") \
        .appName("structuredstreaming") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 订阅一个topic,默认从topic最早的offset到最近的offset
    lines = spark \
        .readStream \
        .format("kafka") \
        .option("groupid","ct")\
        .option("kafka.bootstrap.servers", "192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092") \
        .option("subscribe", "TestTopic") \
        .load()

    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    wordCounts = words.groupBy("word").count()

    # topicSchema = StructType(
    #     [
    #         StructField("word", StringType(), True),
    #         StructField("count", IntegerType(), True)
    #     ]
    # )

    # words = lines.select(("key").cast("string"),
    #     from_json(col("value").cast("string"), schema).alias("data_deta1"))

    # df1 = lines.select( \
    #     # col("key").cast("string"),
    #     from_json(col("value").cast("string"), schema2).alias("data_deta1"))
    #
    # # .alias("data_deta1")
    #
    # dfColumn = df1.select(col("data_deta1.data.type_id"), col("data_deta1.data.type_desc"))
    #
    # # wordsDetail = df.select(explode(col("data_deta1")))
    #
    # dfColumn.createOrReplaceTempView("table1")
    # df3 = spark.sql("SELECT type_id,count(*) from tableaaaa")

    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()




    # user_fields = df.flatMap(lambda line: line.split(" "))\
    #     .map(lambda word: (word, 1))\
    #     .reduceByKey(lambda a, b: a+b)

    # user_fields.pprint()

    # df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    # df.show()

    # spark.start()
    # spark.awaitTermination()

    
    # brokers ="192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092"  
    # topic='TestTopic'  
    # start = 70000  
    # partition=0 

    # kafka_stream = KafkaUtils.createDirectStream(spark, [topic], kafkaParams={"metadata.broker.list":brokers})

    # user_fields = kafka_stream.flatMap(lambda line: line.split(" "))\
    #     .map(lambda word: (word, 1))\
    #     .reduceByKey(lambda a, b: a+b)

    # user_fields.pprint()

# running_counts = lines.flatMap(lambda line: line.split(" "))\
#     .map(lambda word: (word, 1))\
#     .updateStateByKey(updateFunc)
# running_counts.pprint()
# ssc.start()
# ssc.awaitTermination()

# kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
# lines = kvs.map(lambda x: x[1])
# counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
# counts.pprint()









