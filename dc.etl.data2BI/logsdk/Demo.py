from perform import SparkInit
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.logging import logging

if __name__ == '__main__':
    # spark session
    sparkSession = SparkInit.sparkInitialize().setAppName("logsdklog_pc_detail")

    # spark
    sparkSessionStart = sparkSession.getOrCreate()

    logger = logging(sparkSessionStart, "info")

    # structured streaming
    spark = SparkInit.onStructuredStreamingWithKafka(sparkSessionStart)

    # banner
    logger.warn(sparkSession.showConf(), 'config')
    logger.warn("liugf", "author")

    spark.printSchema()

    # 创建目标数据结构
    topicSchema = StructType(
        [
            StructField("_source", StructType([
                StructField("uid", IntegerType(), True),
                StructField("date", IntegerType(), True),
                StructField("time", IntegerType(), True),
                StructField("@timestamp", StringType(), True),
                StructField("area-province", StringType(), True),
                StructField("area-city", StringType(), True),
                StructField("area-district", StringType(), True),
                StructField("area-ip", StringType(), True),
                StructField("browser-type", StringType(), True),
                StructField("browser-vers", StringType(), True),

                StructField("cpu_cores", IntegerType(), True),
                StructField("cpu-frequency", IntegerType(), True),
                StructField("cpu-percent", IntegerType(), True),
                StructField("cpu-type", StringType(), True),
                StructField("duration", IntegerType(), True),
                StructField("eqpt-h", IntegerType(), True),
                StructField("eqpt-w", IntegerType(), True),
                StructField("event", StringType(), True),

                StructField("geo", StructType([
                                StructField("lat", StringType(), True),
                                StructField("lon", StringType(), True),
                            ])),

                StructField("group-channel", IntegerType(), True),
                StructField("group-code", StringType(), True),
                StructField("group-id", IntegerType(), True),
                StructField("group-vers", StringType(), True),

                StructField("hallarea-province", StringType(), True),
                StructField("hallarea-city", StringType(), True),
                StructField("hallarea-district", StringType(), True),

                StructField("hard-disk", StringType(), True),
                StructField("hard-mac", StringType(), True),
                StructField("hard-machine", StringType(), True),
                StructField("session", StringType(), True),
                StructField("number", IntegerType(), True),
                StructField("os-arch", StringType(), True),
                StructField("os-factory", StringType(), True),
                StructField("os-type", StringType(), True),
                StructField("mem-percent", IntegerType(), True),
                StructField("mem-total", IntegerType(), True),




                StructField("group-vers", IntegerType(), True),
                StructField("eqpt-w", IntegerType(), True),
                StructField("event", StringType(), True),

                StructField("time", IntegerType(), True),
                StructField("father-vers", StringType(), True),
                StructField("father-vers", StringType(), True),





                StructField("father-vers", StringType(), True),
                StructField("geo", StructType([
                    StructField("lat", StringType(), True),
                    StructField("lon", StringType(), True),
                ])),
            ]))
        ]
    )

    # 按定义的格式转换接收到的json
    jsonConvertToDataframe = spark.select(spark.key.cast("string"),
                                          from_json(spark.value.cast("string"), topicSchema).alias("data"))

    # select column
    jsonConvertToDataframe.printSchema()
    df = jsonConvertToDataframe.select((jsonConvertToDataframe["data"]["_source"]["father-vers"]).alias("father_vers"),
                                       "data._source.geo.lat", "data._source.geo.lon")

    # 输出
    query1 = df \
        .writeStream \
        .format("orc") \
        .option("path", "/user/hive/warehouse/tempdb.db/logsdk_test/") \
        .option("checkpointLocation", "/spark_offset/logsdk_test") \
        .outputMode("append") \
        .start()

    query1.awaitTermination()
