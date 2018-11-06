from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json
import time
from datetime import datetime, date, timedelta
from pyspark.sql.types import DoubleType

# SOURCE_FILE_PATH = "/projects/server/ROOT/mongodb_count/url.properties"
SOURCE_FILE_PATH = "C:/Users/CTWLPC/Desktop/url.properties"

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

    def sparksession(url, username, password, mongo_database, mongo_collection, hive_databasetable, start_date, end_date):
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
        sql = 'select _id, count from ' + hive_databasetable + ' where _id >= ' + start_date + ' and _id <= ' + end_date
        print("sql: " + sql)
        return sql

    def sparksession_sum(url, username, password, mongo_database, mongo_collection, hive_databasetable, start_date, end_date):
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
        sql = 'select _id, count from ' + hive_databasetable + ' where _id >= ' + start_date + ' and _id <= ' + end_date
        print("sql: " + sql)
        return sql

    def tools(databasetablelabel):
        databasetable = databasetablelabel.split("|")[0]
        hive_databasetable = databasetable.lower()
        print("hive_databasetable:" + hive_databasetable)

        mongo_collection = databasetable
        print("mongo_collection:" + mongo_collection)
        # hive_database = mongo_database.lower()
        # hive_table = mongo_collection.lower()
        return hive_databasetable, mongo_collection

    with open(SOURCE_FILE_PATH, 'r') as f:
        for line in f:
            line = line[:-1]
            url = line.split(" ")[0]
            username = line.split(" ")[1]
            password = line.split(" ")[2]

            # line:192.168.1.199:60001/admin root sw3dsw2d GsLoginDB_logindetail|day,SilverLogDB_login|day,GsPlayTogetherDB_endroom|month
            print("line:" + line)
            # url: 192.168.1.199:60001/admin
            print("url:" + url)
            # root
            print("username:" + username)
            # sw3dsw2d
            print("password:" + password)

            MONGO_ADDR = "mongodb://" + url
            print("MONGO_ADDR:" + MONGO_ADDR)
            databasetablelabels = line.split(" ")[3].split(",")
            for databasetablelabel in databasetablelabels:
                # day  month
                label = databasetablelabel.split("|")[1]
                print("label:" + label)
                hive_databasetable, mongo_collection = tools(databasetablelabel)

                if label == 'day':
                    hivesql = 'select * from ods.' + hive_databasetable + ' where dt = ' + yesterday_str
                    # hivesql = 'select * from ods.' + hive_databasetable + ' where dt = 20180918'
                    print("hivesql:" + hivesql)
                    # hiveDF = spark.sql(hivesql).count()
                    # print("hiveDF:" + str(hiveDF))

                    sql = sparksession(url, username, password, "MongoCountDB", mongo_collection,
                                       hive_databasetable, yesterday_str, yesterday_str)

                    mongoDF = spark.sql(sql)
                    # mongoDF.show()

                    # collection_sum_value = mongoDF.agg({"count": "sum"}).collect()
                    # print("your_collection : " + str(collection_sum_value))

                    collection_sum_value = mongoDF.agg({"count": "sum"}).collect()[0][0]
                    print(type(collection_sum_value))
                    print("collection_sum_value : " + str(collection_sum_value))

                    # mongoDF = spark.sql(sql)

                    print("mongoDF:" + str(mongoDF))


                    # result = mongoDF.rdd.map(lambda p: str(p.count)).collect()[0][0]
                    # for n in result:
                    #     print(n)

                    # result = employee_result.rdd.map(
                    #     lambda p: "name: " + p.name + "  salary: " + str(p.salary)).collect()

                    # your_sum_value = mongoDF.agg({"count": "sum"}).collect()[0][0]
                    # print("your_sum_value:" + str(your_sum_value))
                    # your_sum_value:None

                    # changedTypedf = mongoDF.withColumn("sum(count)", mongoDF["sum(count)"].cast("double"))
                    # print("changedTypedf:" + str(changedTypedf))
                    # changedTypedf: DataFrame[sum(count): double]

                    # print("mongoDF:" + str(mongoDF))
                    # result = hiveDF - mongoDF
                    # print(result)

                if label == 'month':
                    hivesql = 'select * from ods.' + hive_databasetable + ' where dt = ' + day_begin_month
                    print("hivesql:" + hivesql)
                    # hiveDF = spark.sql(hivesql).count()
                    # print("hiveDF:" + str(hiveDF))

                    sql = sparksession_sum(url, username, password, "MongoCountDB", mongo_collection,
                                       hive_databasetable, day_begin_date, yesterday_str)

                    mongoDF = spark.sql(sql)
                    # mongoDF.show()

                    # your_collection = mongoDF.agg({"count": "sum"}).collect()
                    # print("your_collection : " + str(your_collection))

                    collection_sum_value = mongoDF.agg({"count": "max"}).collect()[0][0]
                    print(type(collection_sum_value))
                    print("collection_sum_value : " + str(collection_sum_value))

                    print("mongoDF:" + str(mongoDF))
                    # result = hiveDF - mongoDF
                    # print(hiveDF - mongoDF)
                    # print(result)

    print("mongo hive count successful !!")