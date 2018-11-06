import json
import os
import shutil
import time
from datetime import datetime, date, timedelta
from ctdc.db import mongodb

SOURCE_FILE_PATH = "C:/Users/CTWLPC/Desktop/table.properties"

if __name__ == '__main__':

    MONGO_ADDR = "mongodb://10.111.182.82:50005/admin"
    client = mongodb.MongoDB(MONGO_ADDR, "talend", "HOYooAqtWmIcmFHiz/y3fQ==", adminauth=True)

    database = client.usedb("ScoreDB")
    collection = database["scorelog"]

    # collection.find({"date": 20181022})
    collection_count = collection.count({"date": 20181022})
    print("collection_count:" + str(collection_count))



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

    # yesterday_start_time = (int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))) * 1000
    # yesterday_start_time = str(1536659273000)
    # yesterday_start_time = str(yesterday_start_time)
    yesterday_start_time = 1536659273000

    yesterday_end_time = (int(time.mktime(time.strptime(str(today), '%Y-%m-%d'))) - 1) * 1000
    # yesterday_end_time = str(yesterday_end_time)

    CLIENT_to_MONGO_ADDR = "mongodb://192.168.1.199:60001/admin"
    client_to_mongo = mongodb.MongoDB(CLIENT_to_MONGO_ADDR, "root", "MP8R9DwsyCvvVd3TdvAU7w==", adminauth=True)
    count_database = client_to_mongo.usedb("MongoCountDB")

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
                # GsLoginDB_logindetail
                databasetables = line.split(" ")[3].split(",")
                for databasetable in databasetables:
                    print(databasetable)
                    mongo_database, mongo_collection = tools(databasetable)

                    MONGO_ADDR = "mongodb://" + url
                    print("MONGO_ADDR:" + MONGO_ADDR)
                    client = mongodb.MongoDB(MONGO_ADDR, username, password, adminauth=True)



                    # if mongo_database == "SilverLogDB":
                    #     database = client.usedb(mongo_database)
                    #     # collection = database[mongo_collection]
                    #
                    #     collection_count_backboxlog = database["backboxlog"].count(
                    #         {"updatets":{"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                    #
                    #     collection_count_gamelog = database["gamelog"].count(
                    #         {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                    #
                    #     collection_count_safeboxlog = database["safeboxlog"].count(
                    #         {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                    #
                    #     collection_count = collection_count_backboxlog + collection_count_gamelog + collection_count_safeboxlog
                    #
                    #     print("SilverLogDB_collection_count:" + str(collection_count))
                    #
                    #     count_database[databasetable].insert({"date": yesterday_str, "count": collection_count})
                    #
                    # else:
                    database = client.usedb(mongo_database)
                    collection = database[mongo_collection]

                    collection.find({"date": yesterday_str})

                    mongo_collection_count = collection.count(
                        {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                    print("mongo_collection_count:" + str(mongo_collection_count))

                    # count_database[databasetable].update().insert({"date":yesterday_str, "count": mongo_collection_count})
                    count_database[databasetable].update({"_id":yesterday_str}, {"count": mongo_collection_count }, upsert = True )

            # SilverLogDB
            elif url == '10.111.182.82:50015/admin':
                databasetables = line.split(" ")[3].split(",")
                for databasetable in databasetables:
                    print(databasetable)
                    mongo_database, mongo_collection = tools(databasetable)

                    MONGO_ADDR = "mongodb://" + url
                    print("MONGO_ADDR:", MONGO_ADDR)
                    # client = mongodb.MongoDB(MONGO_ADDR, username, password, adminauth=True)
                    #
                    # if mongo_database == "SilverLogDB":
                    #     database = client.usedb(mongo_database)
                    #
                    #     collection_count_backboxlog = database["backboxlog"].count(
                    #         {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                    #
                    #     collection_count_gamelog = database["gamelog"].count(
                    #         {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                    #
                    #     collection_count_safeboxlog = database["safeboxlog"].count(
                    #         {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                    #
                    #     collection_count = collection_count_backboxlog + collection_count_gamelog + collection_count_safeboxlog
                    #
                    #     print("SilverLogDB_collection_count:" + str(collection_count))
                    #
                    #     count_database[databasetable].insert({"date": yesterday_str, "count": collection_count})

            # ScoreDB_scorelog
            else:
                databasetables = line.split(" ")[3].split(",")
                for databasetable in databasetables:
                    print(databasetable)
                    mongo_database, mongo_collection = tools(databasetable)

                    MONGO_ADDR = "mongodb://" + url
                    print("MONGO_ADDR:", MONGO_ADDR)
                    # client = mongodb.MongoDB(MONGO_ADDR, username, password, adminauth=True)
                    #
                    # database = client.usedb(mongo_database)
                    # collection = database[mongo_collection]
                    #
                    # mongo_collection_count = collection.count(
                    #     {"date": {"$eq": yesterday_str}})
                    # print("mongo_collection_count:" + str(mongo_collection_count))
                    # count_database[databasetable].insert({"date": yesterday_str, "count": mongo_collection_count})

