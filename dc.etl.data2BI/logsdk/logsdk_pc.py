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

    topicSchema = StructType(
        [
            StructField("uid", IntegerType(), True),
            StructField("date", IntegerType(), True),
            StructField("time", IntegerType(), True),
            StructField("area-province", StringType(), True),
            StructField("area-city", StringType(), True),
            StructField("area-district", StringType(), True),
            StructField("area-ip", StringType(), True),
            StructField("browser-type", StringType(), True),
            StructField("browser-vers", StringType(), True),
            StructField("cpu-cores", IntegerType(), True),
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
            StructField("mem-total", IntegerType(), True)
        ]
    )

    # topicSchema = StructType(
    #     [
    #         StructField("_source", StructType([
    #             StructField("uid", IntegerType(), True),
    #             StructField("date", IntegerType(), True),
    #             StructField("time", IntegerType(), True),
    #             StructField("@timestamp", StringType(), True),
    #             StructField("area-province", StringType(), True),
    #             StructField("area-city", StringType(), True),
    #             StructField("area-district", StringType(), True),
    #             StructField("area-ip", StringType(), True),
    #             StructField("browser-type", StringType(), True),
    #             StructField("browser-vers", StringType(), True),
    #             StructField("cpu_cores", IntegerType(), True),
    #             StructField("cpu-frequency", IntegerType(), True),
    #             StructField("cpu-percent", IntegerType(), True),
    #             StructField("cpu-type", StringType(), True),
    #             StructField("duration", IntegerType(), True),
    #             StructField("eqpt-h", IntegerType(), True),
    #             StructField("eqpt-w", IntegerType(), True),
    #             StructField("event", StringType(), True),
    #             StructField("geo", StructType([
    #                             StructField("lat", StringType(), True),
    #                             StructField("lon", StringType(), True),
    #                         ])),
    #             StructField("group-channel", IntegerType(), True),
    #             StructField("group-code", StringType(), True),
    #             StructField("group-id", IntegerType(), True),
    #             StructField("group-vers", StringType(), True),
    #             StructField("hallarea-province", StringType(), True),
    #             StructField("hallarea-city", StringType(), True),
    #             StructField("hallarea-district", StringType(), True),
    #             StructField("hard-disk", StringType(), True),
    #             StructField("hard-mac", StringType(), True),
    #             StructField("hard-machine", StringType(), True),
    #             StructField("session", StringType(), True),
    #             StructField("number", IntegerType(), True),
    #             StructField("os-arch", StringType(), True),
    #             StructField("os-factory", StringType(), True),
    #             StructField("os-type", StringType(), True),
    #             StructField("mem-percent", IntegerType(), True),
    #             StructField("mem-total", IntegerType(), True)
    #         ]))
    #     ]
    # )

    # conversion json
    jsonConvertToDataframe = spark.select(spark.key.cast("string"),
                                          from_json(spark.value.cast("string"), topicSchema).alias("data"))

    # select column
    # jsonConvertToDataframe.printSchema()

    pc_df = jsonConvertToDataframe.select("data.uid",
                                          "data.date",
                                          "data.time",
                                          (jsonConvertToDataframe["data"]["area-province"]).alias("area_province"),
                                          (jsonConvertToDataframe["data"]["area-city"]).alias("area_city"),
                                          (jsonConvertToDataframe["data"]["area-district"]).alias("area_district"),
                                          (jsonConvertToDataframe["data"]["area-ip"]).alias("area_ip"),
                                          (jsonConvertToDataframe["data"]["browser-type"]).alias("browser_type"),
                                          (jsonConvertToDataframe["data"]["browser-vers"]).alias("browser_vers"),
                                          (jsonConvertToDataframe["data"]["cpu-cores"]).alias("cpu_cores"),
                                          (jsonConvertToDataframe["data"]["cpu-frequency"]).alias("cpu_frequency"),
                                          (jsonConvertToDataframe["data"]["cpu-percent"]).alias("cpu_percent"),
                                          (jsonConvertToDataframe["data"]["cpu-type"]).alias("cpu_type"),
                                          "data.duration",
                                          (jsonConvertToDataframe["data"]["eqpt-h"]).alias("eqpt_h"),
                                          (jsonConvertToDataframe["data"]["eqpt-w"]).alias("eqpt_w"),
                                          "data.event",
                                          "data.geo.lat",
                                          "data.geo.lon",
                                          (jsonConvertToDataframe["data"]["group-channel"]).alias("group_channel"),
                                          (jsonConvertToDataframe["data"]["group-code"]).alias("group_code"),
                                          (jsonConvertToDataframe["data"]["group-id"]).alias("group_id"),
                                          (jsonConvertToDataframe["data"]["group-vers"]).alias("group_vers"),
                                          (jsonConvertToDataframe["data"]["hallarea-province"]).alias("hallarea_province"),
                                          (jsonConvertToDataframe["data"]["hallarea-city"]).alias("hallarea_city"),
                                          (jsonConvertToDataframe["data"]["hallarea-district"]).alias("hallarea_district"),
                                          (jsonConvertToDataframe["data"]["hard-disk"]).alias("hard_disk"),
                                          (jsonConvertToDataframe["data"]["hard-mac"]).alias("hard_mac"),
                                          (jsonConvertToDataframe["data"]["hard-machine"]).alias(
                                              "hard_machine"),
                                          "data.session",
                                          "data.number",
                                          (jsonConvertToDataframe["data"]["os-arch"]).alias("os_arch"),
                                          (jsonConvertToDataframe["data"]["os-factory"]).alias("os_factory"),
                                          (jsonConvertToDataframe["data"]["os-type"]).alias("os_type"),
                                          (jsonConvertToDataframe["data"]["mem-percent"]).alias("mem_percent"),
                                          (jsonConvertToDataframe["data"]["mem-total"]).alias("mem_total"))

    query1 = pc_df \
        .writeStream \
        .format("orc") \
        .option("path", "/user/hive/warehouse/tempdb.db/logsdk_pc_test1/") \
        .option("checkpointLocation", "/spark_offset/logsdk_pc_test1") \
        .outputMode("append") \
        .start()

    query1.awaitTermination()


    # pc_df.createOrReplaceTempView("logsdklog_pc_detail")
    # sq = "select uid, date, time from logsdklog_pc_detail"
    # df = sparkSessionStart.sql(sq)
    # # df.show()
    #
    # query = df \
    #     .writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start()
    #
    # query.awaitTermination()






