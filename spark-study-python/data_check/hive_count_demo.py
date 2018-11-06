from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json
import time
from datetime import datetime, date, timedelta

if __name__ == '__main__':
    # 今天日期
    # today = datetime.date.today()
    today = date.today()
    # 昨天时间
    yesterday = today - timedelta(days=1)
    yesterday_date = (date.today() + timedelta(days=-1)).strftime("%Y%m%d")

    # 昨天开始时间戳，毫秒级别
    yesterday_start_time = (int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))) * 1000
    yesterday_start_time = str(1536659273000)
    # 昨天结束时间戳，毫秒级别
    yesterday_end_time = (int(time.mktime(time.strptime(str(today), '%Y-%m-%d'))) - 1) * 1000
    yesterday_end_time = str(yesterday_end_time)

    print(yesterday_date)
    print(yesterday_start_time)
    print(yesterday_end_time)

    spark = SparkSession.builder \
        .master("local") \
        .appName("hive_count") \
        .config("spark.some.config.option", "some-value") \
        .enableHiveSupport() \
        .getOrCreate()

    a = "GsLoginDB"
    b = "logindetail"

    mgohost = "192.168.1.199"
    sparkmo = SparkSession.builder \
        .appName("spot") \
        .config("spark.mongodb.input.uri", "mongodb://root:sw3dsw2d@" + mgohost + ":60001/admin") \
        .config("spark.mongodb.input.database", a) \
        .config("spark.mongodb.input.collection", b) \
        .getOrCreate()

    gslogindb_logindetailDF = sparkmo.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("database", a) \
            .option("collection", b) \
            .option("user", "root") \
            .option("password", "sw3dsw2d") \
            .load()
    
    gslogindb_logindetailDF.show()

    # sparkmo = SparkSession.builder \
    #     .appName("spot") \
    #     .config("spark.mongodb.input.uri", "mongodb://root:sw3dsw2d@192.168.1.199:60001/admin") \
    #
    #     .getOrCreate()

    # gslogindb_logindetailDF = sparkmo.read \
    #     .format("com.mongodb.spark.sql.DefaultSource") \
    #     .option("database", "GsloginDB") \
    #     .option("collection", "logindetail") \
    #     .load()
    #
    # gslogindb_logindetailDF.show()
    #
#     # 创建临时视图，主要是为了，可以直接对数据执行sql语句
#     gslogindb_logindetailDF.createOrReplaceTempView("gslogindb_logindetail")
#     sq = 'select count(*) from gslogindb_logindetail where updatets >= ' + yesterday_start_time + ' and updatets <= ' + yesterday_end_time
#     print(sq)
#     df1 = spark.sql(sq)
#     df1.show()
#