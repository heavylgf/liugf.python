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

    # 2018-10-01
    day_begin = str(yesterday)[0:7] + '-01'
    # 1538323200000
    day_begin_time = (int(time.mktime(time.strptime(str(day_begin), '%Y-%m-%d')))) * 1000

    print(yesterday_date)
    print(yesterday_start_time)
    print(yesterday_end_time)

    spark = SparkSession.builder \
        .master("local") \
        .appName("hive_count") \
        .config("spark.some.config.option", "some-value") \
        .enableHiveSupport() \
        .getOrCreate()

    def sparksession(url, username, password, mongo_database, mongo_collection, hive_databasetable, yesterday_start_time, yesterday_end_time):
        sparkmo = SparkSession.builder \
            .appName("spot") \
            .config("spark.mongodb.input.uri", "mongodb://" + username + ":" + password + "@" + url) \
            .getOrCreate()

        mongoDF = sparkmo.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("database", mongo_database) \
            .option("collection", mongo_collection) \
            .load()

        # 创建临时视图，主要是为了，可以直接对数据执行sql语句
        mongoDF.createOrReplaceTempView(hive_databasetable)
        sq = 'select count(*) from ' + hive_databasetable + ' where updatets >= ' + yesterday_start_time + ' and updatets <= ' + yesterday_end_time
        print(sq)
        return sq

    # 读取配置文件
    with open('C:/Users/CTWLPC/Desktop/url.properties') as f:
        for line in f:
            # line = line[:-1]
            url = line.split(" ")[0]
            username = line.split(" ")[1]
            password = line.split(" ")[2]

            print("line:" + line)
            print("url:" + url)
            print("username:" + username)
            print("password:" + password)

            if url == '192.168.1.199:60001/admin':
                databasetables = line.split(" ")[3].split(",")
                for databasetable in databasetables:
                    hive_databasetable = databasetable.lower()
                    print("hive_databasetable:" + hive_databasetable)
                    mongo_database = databasetable.split("_")[0]
                    print("mongo_database:" + mongo_database)
                    mongo_collection = databasetable.split("_")[1]
                    print("mongo_collection:" + mongo_collection)
                    hive_database = mongo_database.lower()
                    hive_table = mongo_collection.lower()

                    sql = sparksession(url, username, password, mongo_database, mongo_collection, hive_databasetable, yesterday_start_time, yesterday_end_time)
                    df1 = spark.sql(sql)
                    df1.show()

                    hivesql = 'select count(*) from ' + hive_databasetable + ' where dt = ' + yesterday
                    hiveDF = spark.sql(hivesql)
                    hiveDF.show()

    print("insert hive successful !!")



    # gslogindb_logindetailDF.show()

    topicSchema = StructType(
        [
            StructField("uid", IntegerType(), True),
            StructField("updatets", IntegerType(), True)
        ]
    )




    # df = gslogindb_logindetailDF.select(gslogindb_logindetailDF.value.cast("string"), topicSchema).alias("gslogindb_logindetail")

    # gslogindb_logindetailDF.agg(count(gslogindb_logindetailDF("updatets"))).show()

    # dfColumn = df.select("gslogindb_logindetail.uid", "gslogindb_logindetail.updatets")
    #
    # dfColumn.createOrReplaceTempView("gslogindb_logindetail")
    # sq = "select uid, updatets from people "
    # df1 = spark.sql(sq)
    # df1.show()
    print("insert hive successful !!")


    # df = spark.sql("select count(*) from ods.gslogindb_logindetail where dt = \'yesterday\' ")
    # df.show()





