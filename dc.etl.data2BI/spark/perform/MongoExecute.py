#coding=utf-8

from utils.Date import dateIntervalIterator
from ctdc.config import ini
from resource import const
from utils import mongodb
import time


SECTION_MONGO = "mongo"


def getClient(db):
    '''
    获取数据库实例
    '''

    config = ini.Config(const.CONFIG_PATH)

    host = "mongodb://%s" %(config.getconfig(SECTION_MONGO, "url", "192.168.1.199:60001"))
    user = config.getconfig(SECTION_MONGO, "user", "root")
    pwd = config.getconfig(SECTION_MONGO, "password", "MP8R9DwsyCvvVd3TdvAU7w==")
    # database = config.getconfig(SECTION_MONGO, "authdb", "admin")
    return mongodb.MongoDB(host, user, pwd).usedb(db)


def remove(db,collection,colume,date):
    '''
    删除现有数据
    :param colume: 筛选列
    :param date: 删选日期
    :return: None
    '''
    getClient(db)[collection].remove({colume: int(date)})
    getClient(db)[collection].remove({colume: date})



class mongoExecute:

    def __init__(self):
        config = ini.Config(const.CONFIG_PATH)

        host = "mongodb://%s" % (config.getconfig(SECTION_MONGO, "url", "192.168.1.199:60001"))
        user = config.getconfig(SECTION_MONGO, "user", "root")
        pwd = config.getconfig(SECTION_MONGO, "password", "MP8R9DwsyCvvVd3TdvAU7w==")

        self.mongondb_instance = mongodb.MongoDB(host, user, pwd)

    def collectionOverwrite(self,dataframe,db,collection):
        '''
        删除collection在重建表
        :param db:表所在库
        :param collection:需删除数据的表
        :return:
        '''
        dataframe\
            .write\
            .format("com.mongodb.spark.sql.DefaultSource")\
            .mode('overwrite')\
            .option("database",db)\
            .option("collection", collection)\
            .save()

    def collectionAppend(self,dataframe,db,collection,start_date=None,end_date=None):
        '''
        删除collection中时间区间数据
        :param db:表所在库
        :param collection:需删除数据的表
        :param start_date:
        :param end_date:
        :return:
        '''

        for exectue_date in dateIntervalIterator(start_date, end_date, 1):
                print("Mongo delete date: " + exectue_date)
                remove(db,collection,"dt",exectue_date)

        dataframe\
            .write\
            .format("com.mongodb.spark.sql.DefaultSource")\
            .mode('append')\
            .option("database",db)\
            .option("collection", collection)\
            .save()


    def collectionOverwrite(self,dataframe,db,collection):

        '''
        :param db:表所在库
        :param collection:需删除数据的表
        :return:
        '''

        dataframe\
            .write\
            .format("com.mongodb.spark.sql.DefaultSource")\
            .mode('overwrite')\
            .option("database",db)\
            .option("collection", collection)\
            .save()