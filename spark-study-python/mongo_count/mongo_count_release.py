import json
import os
import shutil
import time
from datetime import datetime, date, timedelta
from ctdc.db import mongodb

SOURCE_FILE_PATH = "/projects/server/ROOT/mongodb_count/table.properties"

if __name__ == '__main__':
    # 2018-10-18
    today = date.today()
    # 2018-10-17
    yesterday = today - timedelta(days=1)
    # 20171017
    yesterday_str = (date.today() + timedelta(days=-1)).strftime("%Y%m%d")
    # yesterday_str = str(yesterday_date)

    yesterday_start_time = (int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))) * 1000
    # yesterday_start_time = 1536659273000
    # yesterday_start_time = str(yesterday_start_time)
    print("yesterday_start_time:" + str(yesterday_start_time))

    yesterday_end_time = (int(time.mktime(time.strptime(str(today), '%Y-%m-%d'))) - 1) * 1000
    # yesterday_end_time = str(yesterday_end_time)
    print("yesterday_end_time:" + str(yesterday_end_time))

    CLIENT_to_MONGO_ADDR = "mongodb://10.50.0.171:60001/admin"
    client_to_mongo = mongodb.MongoDB(CLIENT_to_MONGO_ADDR, "dbadmin", "bZQqcb4pX5WTMj/N+K7OYw==", adminauth=True)
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

            if url == '10.51.183.9:50001/admin':
                # GsLoginDB_logindetail
                databasetables = line.split(" ")[3].split(",")
                for databasetable in databasetables:
                    print("databasetable:" + databasetable)
                    mongo_database, mongo_collection = tools(databasetable)

                    MONGO_ADDR = "mongodb://" + url
                    print("MONGO_ADDR:" + MONGO_ADDR)
                    client = mongodb.MongoDB(MONGO_ADDR, username, password, adminauth=True)
                    database = client.usedb(mongo_database)
                    collection = database[mongo_collection]

                    if mongo_database == "GsUserDB":
                        collection_count = collection.count({"date": int(yesterday_str)})
                        print("collection_count:" + str(collection_count))

                        count_database[databasetable].update({"_id": yesterday_str}, {"count": collection_count},
                                                             upsert=True)
                        print(str(databasetable) + " : " + str(collection_count))
                    else:
                        collection_count = collection.count(
                            {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})
                        print("mongo_collection_count:" + str(collection_count))

                        count_database[databasetable].update({"_id": yesterday_str}, {"count": collection_count},
                                                             upsert=True)

                        print(str(databasetable) + " : " + str(collection_count))
                        ##count_database[databasetable].insert({"_id":yesterday_str, "count": mongo_collection_count})

            # SilverLogDB
            elif url == '10.51.183.9:50015/admin':
                databasetables = line.split(" ")[3].split(",")
                for databasetable in databasetables:
                    print("databasetable:" + databasetable)
                    mongo_database, mongo_collection = tools(databasetable)

                    MONGO_ADDR = "mongodb://" + url
                    print("MONGO_ADDR:" + MONGO_ADDR)
                    client = mongodb.MongoDB(MONGO_ADDR, username, password, adminauth=True)

                    if mongo_database == "SilverLogDB":
                        database = client.usedb(mongo_database)

                        collection_count_backboxlog = database["backboxlog"].count(
                            {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})

                        collection_count_gamelog = database["gamelog"].count(
                            {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})

                        collection_count_safeboxlog = database["safeboxlog"].count(
                            {"updatets": {"$gte": yesterday_start_time, "$lte": yesterday_end_time}})

                        collection_count = collection_count_backboxlog + collection_count_gamelog + collection_count_safeboxlog

                        print("SilverLogDB_collection_count:" + str(collection_count))

                        count_database[databasetable].update({"_id": yesterday_str}, {"count": collection_count},
                                                             upsert=True)
                        print(str(databasetable) + " : " + str(collection_count))
                        # count_database[databasetable].insert({"_id": yesterday_str, "count": collection_count})

            # ScoreDB_scorelog
            else:
                databasetables = line.split(" ")[3].split(",")
                for databasetable in databasetables:
                    print("databasetable:" + databasetable)
                    mongo_database, mongo_collection = tools(databasetable)

                    MONGO_ADDR = "mongodb://" + url
                    print("MONGO_ADDR:" + MONGO_ADDR)
                    client = mongodb.MongoDB(MONGO_ADDR, username, password, adminauth=True)

                    database = client.usedb(mongo_database)
                    collection = database[mongo_collection]

                    collection.find({"date": yesterday_str})
                    collection_count = collection.count({"date": int(yesterday_str)})
                    print("collection_count:" + str(collection_count))

                    count_database[databasetable].update({"_id": yesterday_str}, {"count": collection_count},
                                                         upsert=True)
                    print(str(databasetable) + " : " + str(collection_count))
                    # count_database[databasetable].insert({"_id": yesterday_str, "count": mongo_collection_count})
    print("end  data .......")

