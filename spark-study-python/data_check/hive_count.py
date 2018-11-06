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
    # 2018-10-18
    today = date.today()
    # 2018-10-17
    yesterday = today - timedelta(days=1)
    # 20171017
    yesterday_date = (date.today() + timedelta(days=-1)).strftime("%Y%m%d")
    yesterday_str = str(yesterday_date)

    # 2018-10-01
    day_begin = str(yesterday)[0:7] + '-01'

    # 201810
    day_begin_month = str(yesterday_date)[0:6]

    # 20181001
    day_begin_date = str(yesterday_date)[0:6] + '01'
    # 1538323200000
    day_begin_time = (int(time.mktime(time.strptime(str(day_begin), '%Y-%m-%d')))) * 1000
    day_begin_time = str(day_begin_time)

    yesterday_start_time = (int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))) * 1000
    # yesterday_start_time = str(1536659273000)
    yesterday_start_time = str(yesterday_start_time)

    yesterday_end_time = (int(time.mktime(time.strptime(str(today), '%Y-%m-%d'))) - 1) * 1000
    yesterday_end_time = str(yesterday_end_time)

    # print(yesterday_date)
    # print(yesterday_start_time)
    # print(yesterday_end_time)

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

        mongoDF.createOrReplaceTempView(hive_databasetable)
        sql = 'select * from ' + hive_databasetable + ' where updatets >= ' + yesterday_start_time + ' and updatets <= ' + yesterday_end_time
        print("sql: " + sql)
        return sql

    def sparksession_date(url, username, password, mongo_database, mongo_collection, hive_databasetable, yesterday_start_time, yesterday_end_time):
        sparkmo = SparkSession.builder \
            .appName("spot") \
            .config("spark.mongodb.input.uri", "mongodb://" + username + ":" + password + "@" + url) \
            .getOrCreate()

        mongoDF = sparkmo.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("database", mongo_database) \
            .option("collection", mongo_collection) \
            .load()

        mongoDF.createOrReplaceTempView(hive_databasetable)
        sql = 'select * from ' + hive_databasetable + ' where date >= ' + yesterday_start_time + ' and date <= ' + yesterday_end_time
        # print("sql: " + sql)
        return sql

    def tools(databasetablelabel):
        databasetable = databasetablelabel.split("|")[0]
        hive_databasetable = databasetable.lower()
        print("hive_databasetable:" + hive_databasetable)
        mongo_database = databasetable.split("_")[0]
        print("mongo_database:" + mongo_database)
        mongo_collection = databasetable.split("_")[1]
        print("mongo_collection:" + mongo_collection)
        hive_database = mongo_database.lower()
        hive_table = mongo_collection.lower()
        return hive_databasetable, mongo_database, mongo_collection, hive_database, hive_table

    # with open('/project/url.properties') as f:
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
                # GsLoginDB_logindetail|day
                databasetablelabels = line.split(" ")[3].split(",")
                for databasetablelabel in databasetablelabels:
                    # day  month
                    label = databasetablelabel.split("|")[1]

                    hive_databasetable, mongo_database, mongo_collection, hive_database, hive_table = tools(
                        databasetablelabel)

                    print("label:" + label)

                    if label == 'day':

                        if mongo_database == "SilverLogDB":
                            # hivesql = 'select count(*) from ods.' + hive_databasetable + ' where dt = ' + yesterday_str
                            # hivesql = 'select count(*) from ods.' + hive_databasetable + ' where dt = 20180918'
                            # # print("hivesql:" + hivesql)
                            # hiveDF = spark.sql(hivesql)
                            # hiveDF.show()
                            continue
                        if mongo_database == "ScoreDB":
                            sql = sparksession_date(url, username, password, mongo_database, mongo_collection,
                                                    hive_databasetable, yesterday_str, yesterday_str)
                            # hivesql = 'select count(*) from ods.' + hive_databasetable + ' where dt = ' + yesterday_str
                            # hivesql = 'select count(*) from ods.' + hive_databasetable + ' where dt = 20180918'
                            # # print("hivesql:" + hivesql)
                            # hiveDF = spark.sql(hivesql)
                            # hiveDF.show()
                        else:
                            # hivesql = 'select * from ods.' + hive_databasetable + ' where dt = ' + yesterday_str
                            # hivesql = 'select * from ods.' + hive_databasetable + ' where dt = 20180918'
                            # hiveDF = spark.sql(hivesql).count()
                            # print("hiveDF:" + str(hiveDF))

                            sql = sparksession(url, username, password, mongo_database, mongo_collection,
                                               hive_databasetable,
                                               yesterday_start_time, yesterday_end_time)

                            mongoDF = spark.sql(sql).count()
                            print("mongoDF:" + str(mongoDF))
                            # result = hiveDF - mongoDF
                            # print(hiveDF - mongoDF)

                    if label == 'month':
                        hivesql = 'select * from ods.' + hive_databasetable + ' where dt = ' + day_begin_month
                        # hivesql = 'select * from ods.' + hive_databasetable + ' where dt = 20180918'
                        print("hivesql:" + hivesql)
                        hiveDF = spark.sql(hivesql).count()

                        print("hiveDF:" + str(hiveDF))

                        sql = sparksession(url, username, password, mongo_database, mongo_collection,
                                           hive_databasetable,
                                           day_begin_time, yesterday_end_time)

                        mongoDF = spark.sql(sql).count()

                        print("mongoDF:" + str(mongoDF))
                        result = hiveDF - mongoDF
                        print(hiveDF - mongoDF)
                        print(result)


    print("insert hive successful !!")
