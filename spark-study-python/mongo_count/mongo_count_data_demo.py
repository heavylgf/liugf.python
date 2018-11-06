import json
import os
import shutil
import time
from datetime import datetime, date, timedelta
from ctdc.db import mongodb

SOURCE_FILE_PATH = "/project/mongodb/table.properties"

if __name__ == '__main__':
    # 2018-10-18
    today = date.today()
    # 2018-10-17
    yesterday = today - timedelta(days=1)
    # 20171017
    yesterday_date = (date.today() + timedelta(days=-1)).strftime("%Y%m%d")
    yesterday_str = str(yesterday_date)

    # # 2018-10-01
    # day_begin = str(yesterday)[0:7] + '-01'
    #
    # # 201810
    # day_begin_month = str(yesterday_date)[0:6]
    #
    # # 20181001
    # day_begin_date = str(yesterday_date)[0:6] + '01'
    # # 1538323200000
    # day_begin_time = (int(time.mktime(time.strptime(str(day_begin), '%Y-%m-%d')))) * 1000
    # day_begin_time = str(day_begin_time)

    yesterday_start_time = (int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))) * 1000
    # yesterday_start_time = str(1536659273000)
    yesterday_start_time = str(yesterday_start_time)
    print("yesterday_start_time:" + yesterday_start_time)

    yesterday_end_time = (int(time.mktime(time.strptime(str(today), '%Y-%m-%d'))) - 1) * 1000
    yesterday_end_time = str(yesterday_end_time)
    print("yesterday_end_time:" + yesterday_end_time)


    def tools(databasetable):
        # databasetable = GsLoginDB_logindetail
        mongo_database = databasetable.split("_")[0]
        print("mongo_database:" + mongo_database)
        mongo_collection = databasetable.split("_")[1]
        print("mongo_collection:" + mongo_collection)
        # hive_database = mongo_database.lower()
        # hive_table = mongo_collection.lower()
        return mongo_database, mongo_collection


    with open(SOURCE_FILE_PATH, 'r') as file:
        for line in file:
            line = line[:-1]
            url = line.split(" ")[0]
            username = line.split(" ")[1]
            password = line.split(" ")[2]

            print("line:" + line)
            print("url:" + url)
            print("username:" + username)
            print("password:" + password)

            if url == '192.168.1.199:60001/admin':
                # GsLoginDB_logindetail|day
                databasetables = line.split(" ")[3].split(",")
                for databasetable in databasetables:
                    print(databasetable)
                    mongo_database, mongo_collection = tools(databasetable)

                    MONGO_ADDR = "mongodb://" + url
                    print("MONGO_ADDR:", MONGO_ADDR)
                    client = mongodb.MongoDB(MONGO_ADDR, username, password, adminauth=True)

                    if mongo_database == "SilverLogDB":
                        continue
                    else:
                        database = client.usedb(mongo_database)
                        collection = database[mongo_collection]

                        # mongo_collection_count1 = collection.count({"updatets":{"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                        mongo_collection_count1 = collection.count(
                            {"updatets": {"$gte": 1536659273000, "$lte": 1540207123000}})
                        print("mongo_collection_count1:" + str(mongo_collection_count1))
                        mongo_collection_count = collection.find(
                            {"updatets": {"$gte": 1536659273000, "$lte": 1540207123000}}).count()
                        print("mongo_collection_count:" + str(mongo_collection_count))