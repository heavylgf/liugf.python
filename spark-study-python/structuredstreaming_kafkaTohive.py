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

    # sc = new org.apache.spark.SparkContext
    # hiveContext = new HiveContext(sc)

    lines = spark \
        .readStream \
        .format("kafka") \
        .option("groupid","ct")\
        .option("kafka.bootstrap.servers", "192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092") \
        .option("subscribe", "TestTopic") \
        .load()


#     case class
#  Person(name:String,col1:Int,col2:String)

# val
#  sc = new org.apache.spark.SparkContext  

# val
#  hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

# import hiveContext.implicits._
# hiveContext.sql("use DataBaseName")

# val
#  data = sc.textFile("path").map(x=>x.split("\\s+")).map(x=>Person(x(0),x(1).toInt,x(2)))

# data.toDF().registerTempTable("table1")

# hiveContext.sql("insert
#  into table2 partition(date='2015-04-02') select name,col1,col2 from table1")

    # words = lines.select(
    #     explode(
    #         split(lines.value, " ")
    #     ).alias("word")
    # )

    topicSchema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ]
    )

    df = lines.select(lines.key.cast("string"),
        from_json(lines.value.cast("string"), topicSchema).alias("people"))

    dfColumn = df.select("people.name", "people.age")

    dfColumn.createOrReplaceTempView("people")
    sq = "select name, age from people "
    df1 = spark.sql(sq)        
    print("insert hive successful !!")
    
    query = df1 \
        .writeStream \
        .format("csv") \
        .option("path", "/user/hive/warehouse/tempdb.db/people/dt=20180927/") \
        .option("checkpointLocation", "/checkpoint_path") \
        .outputMode("append") \
        .start()
        
    query.awaitTermination()

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



    # wordCounts = words.groupBy("word").count()

    # query = wordCounts \
    #     .writeStream \
    #     .saveAsTable("temp.wordcounts") \
    #     .format("parquet") \
    #     .option("path", "user/hive/warehouse/tempdb.db/wordcounts/") \
    #     .outputMode("append") \
    #     .start()

    # query.awaitTermination()


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









