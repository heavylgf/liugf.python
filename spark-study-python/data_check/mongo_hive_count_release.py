#!/usr/bin/python
# coding:utf-8
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json
import time
from datetime import datetime, date, timedelta
from ctdc.log.dclog import DcLogger
import math

SOURCE_FILE_PATH = "/data/projects/server/ROOT/validation_data/url.properties"
# SOURCE_FILE_PATH = "C:/Users/CTWLPC/Desktop/url.properties"

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

    spark = SparkSession.builder \
        .master("local") \
        .appName("hive_count") \
        .config("spark.some.config.option", "some-value") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    Logger = spark._jvm.org.apache.log4j.Logger
    mylogger = Logger.getLogger(__name__)


    def sparksession(url, username, password, mongo_database, mongo_collection, hive_databasetable, start_date,
                     end_date):
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
        sql = 'select * from ' + hive_databasetable + ' where _id >= ' + start_date + ' and _id <= ' + end_date
        return sql


    def sparksession_sum(url, username, password, mongo_database, mongo_collection, hive_databasetable, start_date,
                         end_date):
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

        sql = 'select * from ' + hive_databasetable + ' where _id >= ' + start_date + ' and _id <= ' + end_date
        return sql


    def tools(databasetablelabel):
        databasetable = databasetablelabel.split("|")[0]
        hive_databasetable = databasetable.lower()
        print("hive_databasetable:" + hive_databasetable)
        mongo_collection = databasetable
        print("mongo_collection:" + mongo_collection)
        return hive_databasetable, mongo_collection


    with open(SOURCE_FILE_PATH, 'r') as f:
        logger = DcLogger()
        for line in f:
            line = line[:-1]
            url = line.split(" ")[0]
            username = line.split(" ")[1]
            password = line.split(" ")[2]

            # line:192.168.1.199:60001/admin root sw3dsw2d GsLoginDB_logindetail|day,SilverLogDB_login|day,GsPlayTogetherDB_endroom|month
            # print("line:" + line)
            # url: 192.168.1.199:60001/admin
            # print("url:" + url)
            # root
            # print("username:" + username)
            # sw3dsw2d
            # print("password:" + password)

            MONGO_ADDR = "mongodb://" + url
            # print("MONGO_ADDR:" + MONGO_ADDR)
            databasetablelabels = line.split(" ")[3].split(",")
            for databasetablelabel in databasetablelabels:
                # day  month
                label = databasetablelabel.split("|")[1]
                print("label:" + label)
                hive_databasetable, mongo_collection = tools(databasetablelabel)

                if label == 'day':
                    hivesql = 'select * from ods.' + hive_databasetable + ' where dt = ' + yesterday_str
                    # hivesql = 'select * from ods.' + hive_databasetable + ' where dt = 20180918'
                    # print("hivesql:" + hivesql)
                    hiveDF = spark.sql(hivesql).count()

                    sql = sparksession(url, username, password, "MongoCountDB", mongo_collection,
                                       hive_databasetable, yesterday_str, yesterday_str)

                    mongoDF = spark.sql(sql)
                    collection_sum_value = mongoDF.agg({"count": "sum"}).collect()[0][0]

                    # print("mongoDF_type:" + type(mongoDF))
                    # print("hiveDF_type:" + type(hiveDF))

                    # result = abs(float(str((hiveDF - collection_sum_value) / collection_sum_value)))
                    result = math.fabs((hiveDF - collection_sum_value) / collection_sum_value)
                    mylogger.warn("hiveDF：" + str(hiveDF))
                    mylogger.warn("collection_sum_value：" + str(collection_sum_value))
                    print("result : " + str(result))

                    if result > 0.01:
                        logger.error(mongo_collection + "：result " + str(result))

                if label == 'month':
                    hivesql = 'select * from ods.' + hive_databasetable + ' where dt = ' + day_begin_month
                    # print("hivesql:" + hivesql)
                    hiveDF = spark.sql(hivesql).count()
                    # print("hiveDF:" + str(hiveDF))

                    sql = sparksession_sum(url, username, password, "MongoCountDB", mongo_collection,
                                           hive_databasetable, day_begin_date, yesterday_str)

                    mongoDF = spark.sql(sql)
                    collection_sum_value = mongoDF.agg({"count": "sum"}).collect()[0][0]

                    result = math.fabs((hiveDF - collection_sum_value) / collection_sum_value)

                    mylogger.warn("hiveDF：" + str(hiveDF))
                    mylogger.warn("collection_sum_value：" + str(collection_sum_value))
                    print("result : " + str(result))

                    if result > 0.01:
                        logger.error(mongo_collection + "：result " + str(result))

    print("mongo hive count successful !!")


