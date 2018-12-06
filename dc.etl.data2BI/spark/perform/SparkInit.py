#coding=utf-8

import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from ctdc.config import ini
from resource import const
from ctdc.const import encrypt
from ctdc.encrypt import aes

# read config file
config = ini.Config(const.CONFIG_PATH)

SECTION_HIVE = "hive"
SECTION_SPARK = "spark"
SECTION_MONGO = "mongo"

# warehouse_location points to the default location for managed databases and tables
WAREHOUSE_LOCATION = os.path.abspath(config.getconfig(SECTION_HIVE,"warehousePath","/user/hive/warehouse"))
os.environ["HADOOP_USER_NAME"] = config.getconfig(SECTION_HIVE,"user","hive")


class sparkInitialize:
    '''
    spark 初始化

    '''
    def __init__(self):
        self.sparkSession = SparkSession \
                                .builder \
                                .master(config.getconfig(SECTION_SPARK, "master", "local[*]")) \
                                .config("spark.jars", config.getconfig(SECTION_SPARK, "jarlog4j", "%s/jars/log4j-1.2.17.jar" % os.environ.get("SPARK_HOME")))

    def setAppName(self, appname):
        self.sparkSession.appName(appname)
        return sparkInitialize()

    def onHive(self):
        '''
        设置spark用hive的方式连接
        :return: 返回连接
        '''
        self.sparkSession \
                .config("spark.sql.warehouse.dir", WAREHOUSE_LOCATION) \
                .enableHiveSupport() \

        return sparkInitialize()

    def onMongo(self):
        '''
        设置spark与mongo的连接
        :return:返回类sparkInitialize
        '''
        mongo_login_user = config.getconfig(SECTION_MONGO,"user","root")
        mongo_login_pwd = aes.decrypt(config.getconfig(SECTION_MONGO, "password", "MP8R9DwsyCvvVd3TdvAU7w=="), encrypt.MONGODB).strip()
        mongo_url = config.getconfig(SECTION_MONGO, "url", "192.168.1.199:60001")
        mongo_auth_db = config.getconfig(SECTION_MONGO,"authdb","admin")

        mongo_jdbc = "mongodb://%s:%s@%s/%s" %(mongo_login_user,mongo_login_pwd,mongo_url,mongo_auth_db)

        self.sparkSession \
            .config("spark.mongodb.output.uri", mongo_jdbc) \

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


