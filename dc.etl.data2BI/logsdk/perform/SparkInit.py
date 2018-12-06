#coding=utf-8

import os
from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession as sparkGetOrCreate
from pyspark import SparkConf
from ctdc.config import ini
from resource import const
from perform import SparkInit

# read config file
config = ini.Config(const.CONFIG_PATH)

SparkInit.configs = config.allconfigs()

SECTION_HIVE = "hive"
SECTION_SPARK = "spark"
SECTION_KAFKA = "kafka"

os.environ["HADOOP_USER_NAME"] = config.getconfig(SECTION_HIVE, "user")

class sparkInitialize:
    '''
    spark 初始化

    '''
    def __init__(self):

        self.sparkSession = SparkSession\
            .builder\
            .master(config.getconfig(SECTION_SPARK, "master", "local[*]"))\
            .config("spark.jars", config.getconfig(SECTION_SPARK, "jarlog4j",
                                                   "%s/jars/log4j-1.2.17.jar" % os.environ.get("SPARK_HOME"))) \
        # .master(config.getconfig(SECTION_SPARK,"master","local[*]")) \
        # .config("spark.jars",config.getconfig(SECTION_SPARK, "jarlog4j", "%s/jars/log4j-1.2.17.jar" % os.environ.get("SPARK_HOME")))

    def setAppName(self, appname):
        self.sparkSession.appName(appname)
        return sparkInitialize()

    def onHive(self):
        '''
        设置spark用hive的方式连接
        :return: 返回连接
        '''
        # warehouse_location points to the default location for managed databases and tables
        self.sparkSession\
            .config("spark.sql.warehouse.dir", os.path.abspath(SparkInit.configs[SECTION_HIVE]["warehousepath"])) \
            .enableHiveSupport() \

        return sparkInitialize()

    def showConf(self):
        '''
        展示所有的config信息
        '''
        return SparkConf().getAll()

    def getOrCreate(self):
        '''
        开始
        '''
        return self.sparkSession.getOrCreate()


    def initLogging(self):

        return sparkInitialize()


def onStructuredStreamingWithKafka(sparkGetOrCreate):
    '''
    连接kafka
    :return: 返回sparksession
    '''

    return sparkGetOrCreate \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", SparkInit.configs[SECTION_KAFKA]["bootstrapservers"]) \
        .option("subscribe", SparkInit.configs[SECTION_KAFKA]["topic"]) \
        .option("group.id", SparkInit.configs[SECTION_KAFKA]["groupid"]) \
        .option("startingOffsets", SparkInit.configs[SECTION_KAFKA]["startoffset"]) \
        .load()


# lines = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092") \
#     .option("subscribe", "new_test_on_local") \
#     .option("group.id", "hahahah") \
#     .option("startingOffsets", "latest") \
#     .load()


