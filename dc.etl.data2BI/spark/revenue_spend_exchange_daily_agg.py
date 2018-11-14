import datetime
import sys
import time

from perform.MongoExecute import mongoExecute
from perform.SparkInit import sparkInitialize
from utils.HiveAlter import partition
from utils.SubmitArguments import arguments
from utils.logging import logging

DEFAULT_START_DATE = str(datetime.datetime.strptime(time.strftime('%Y%m%d', time.localtime()), '%Y%m%d')
                         + datetime.timedelta(days=-1))[0:10].replace("-", "")

DEFAULT_END_DATE = time.strftime('%Y%m%d', time.localtime())

# spark session
sparkSession = sparkInitialize().setAppName("GsGiftCouponAcquiregcTest").onHive().onMongo()

spark = sparkSession.getOrCreate()

logger = logging(spark, "WARN")

# banner
logger.warn(sparkSession.showConf(), 'config')
logger.warn("liugf", "author")


def logic(start_date=DEFAULT_START_DATE, end_date=DEFAULT_END_DATE):
    gsgiftcoupon_acquiregc_sql = "select t1.game as game_id, " \
                                 "t1.gamecode as game_code, " \
                                 "t1.date as date, " \
                                 "t1.pkgtype as package_type, " \
                                 "t2.enum_value as package_type_name, " \
                                 "t1.fromapp as from_app_id, " \
                                 "t1.fromappcode as from_app_code, " \
                                 "t1.ostype as os_type, " \
                                 "t1.recomgame as recom_game_id, " \
                                 "t1.recomgamecode as recom_game_code, " \
                                 "case " \
                                 "when t1.recomgame is null then 1 " \
                                 "when t1.recomgame is not null then 3 " \
                                 "end " \
                                 "as recom_game_relation, " \
                                 "sum(t1.num) as giftcoupon_amount, " \
                                 "max(t1.dt) as dt " \
                                 "from " \
                                 "(select game, " \
                                 "gamecode, " \
                                 "date, " \
                                 "pkgtype, " \
                                 "fromapp, " \
                                 "fromappcode, " \
                                 "ostype, " \
                                 "recomgame, " \
                                 "recomgamecode, " \
                                 "num, " \
                                 "dt  " \
                                 "from ods.gsgiftcoupondb_acquiregc " \
                                 "where dt >= '%s' and dt < '%s' " \
                                 ") t1 " \
                                 "left join " \
                                 "(select enum_key, " \
                                 "enum_value, " \
                                 "enum_type " \
                                 "from dwd.dim_common_enum_dict " \
                                 "where enum_type = 'pkgtype' " \
                                 ") t2 " \
                                 "on t1.pkgtype = t2.enum_key " \
                                 "group by t1.game, " \
                                 "t1.gamecode, " \
                                 "t1.date, " \
                                 "t1.pkgtype, " \
                                 "t2.enum_value, " \
                                 "t1.fromapp, " \
                                 "t1.fromappcode, " \
                                 "t1.ostype, " \
                                 "t1.recomgame, " \
                                 "t1.recomgamecode " \
                                 % (start_date, end_date) \
                                 # % (20181107, 20181108)

    logger.warn(gsgiftcoupon_acquiregc_sql, 'gsgiftcoupon_acquiregc_sql ')
    gsgiftcoupon_acquiregc_df = spark.sql(gsgiftcoupon_acquiregc_sql)

    # gsgiftcoupon_acquiregc_df.show()

    hive_partition = partition(spark, logger)
    hive_partition.dropPartition("bi.revenue_spend_exchange_daily_agg_level_2", "dt", start_date, end_date)

    gsgiftcoupon_acquiregc_df \
        .write.partitionBy("dt") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("bi.revenue_spend_exchange_daily_agg_level_2")

    # gsgiftcoupon_acquiregc_df \
    #     .write \
    #     .format("orc") \
    #     .insertInto("bi.revenue_spend_exchange_daily_agg_level_2")

    main_recommendation_sql = "select recom_game_id as game_id," \
                              "recom_game_code as game_code," \
                              "date," \
                              "package_type," \
                              "package_type_name," \
                              "from_app_id," \
                              "from_app_code," \
                              "os_type," \
                              "game_id as recom_game_id," \
                              "game_code as recom_game_code," \
                              "2 as recom_game_relation," \
                              "giftcoupon_amount, " \
                              "dt " \
                              "from bi.revenue_spend_exchange_daily_agg_level_2 " \
                              "where dt >= '%s' and dt < '%s' " \
                              " and recom_game_relation = 3 " \
                              % (start_date, end_date) \
        # % (20181107, 20181108)

    logger.warn(main_recommendation_sql, 'main_recommendation_sql ')
    main_recommendation_df = spark.sql(main_recommendation_sql)

    main_recommendation_df \
        .write \
        .partitionBy("dt") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("bi.revenue_spend_exchange_daily_agg_level_2")

    # insert to agg_level_1
    agg_level_1_sql = "select game_id," \
                      "game_code," \
                      "date," \
                      "package_type," \
                      "package_type_name," \
                      "from_app_id," \
                      "from_app_code," \
                      "os_type," \
                      "recom_game_relation," \
                      "sum(giftcoupon_amount) as giftcoupon_amount, " \
                      "dt " \
                      "from bi.revenue_spend_exchange_daily_agg_level_2 " \
                      "where dt >= '%s' and dt < '%s' " \
                      "group by " \
                      "game_id, " \
                      "game_code, " \
                      "date, " \
                      "package_type, " \
                      "package_type_name," \
                      "from_app_id, " \
                      "from_app_code, " \
                      "os_type, " \
                      "recom_game_relation, " \
                      "dt " \
                      % (start_date, end_date)
                      # % (20181107, 20181108)

    agg_level_1_partition = partition(spark, logger)
    agg_level_1_partition.dropPartition("bi.revenue_spend_exchange_daily_agg_level_1", "dt", start_date, end_date)

    logger.warn(agg_level_1_sql, 'agg_level_1_sql ')
    agg_level_1_df = spark.sql(agg_level_1_sql)
    agg_level_1_df \
        .write \
        .partitionBy("dt") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("bi.revenue_spend_exchange_daily_agg_level_1")

    # agg_level_1  into mongoDB
    insert_mongo_agg_level_1_sql = "SELECT game_id as gameId, " \
                                   "game_code as gameCode, " \
                                   "date as date, " \
                                   "package_type as packageTypeId, " \
                                   "package_type_name as packageTypeName, " \
                                   "from_app_id as fromAppId, " \
                                   "case " \
                                   "when from_app_code is null then ' ' " \
                                   "when from_app_code = '' then ' ' " \
                                   "else from_app_code " \
                                   "end " \
                                   "as fromAppCode, " \
                                   "os_type as osType, " \
                                   "recom_game_relation as recommendRelation, " \
                                   "giftcoupon_amount as count, " \
                                   "dt " \
                                   "FROM bi.revenue_spend_exchange_daily_agg_level_1 " \
                                   "where dt >= '%s' and dt < '%s'" \
                                   % (start_date, end_date) \
        # % (20181107, 20181108)

    logger.warn(insert_mongo_agg_level_1_sql, 'insert_mongo_agg_level_1_sql ')
    insert_mongo_agg_level_1_df = spark.sql(insert_mongo_agg_level_1_sql)

    mongo = mongoExecute()
    mongo.collectionAppend(insert_mongo_agg_level_1_df, "GameProfitDB",
                           "voucher.brief", start_date, end_date)

    # insert_mongo_agg_level_1_df\
    #     .write\
    #     .format("com.mongodb.spark.sql.DefaultSource")\
    #     .mode('overwrite')\
    #     .option("database", "bi")\
    #     .option("collection", "revenue_spend_exchange_daily_agg_level_1")\
    #     .save()

    # agg_level_2  into mongoDB
    insert_mongo_agg_level_2_sql = "SELECT game_id as gameId, " \
                                   "game_code as gameCode, " \
                                   "date as date, " \
                                   "package_type as packageTypeId, " \
                                   "package_type_name as packageTypeName, " \
                                   "from_app_id as fromAppId, " \
                                   "case " \
                                   "when from_app_code is null then ' ' " \
                                   "when from_app_code = '' then ' ' " \
                                   "else from_app_code " \
                                   "end " \
                                   "as fromAppCode, " \
                                   "os_type as osType, " \
                                   "recom_game_id as relateGameId, " \
                                   "recom_game_code as relateGameCode, " \
                                   "recom_game_relation as recommendRelation, " \
                                   "giftcoupon_amount as count, " \
                                   "dt " \
                                   "FROM bi.revenue_spend_exchange_daily_agg_level_2 " \
                                   "where dt >= '%s' and dt < '%s'" \
                                   % (start_date, end_date) \
                                   # % (20181107, 20181108)

    logger.warn(insert_mongo_agg_level_2_sql, 'insert_mongo_agg_level_2_sql ')
    insert_mongo_agg_level_2_df = spark.sql(insert_mongo_agg_level_2_sql)

    mongo.collectionAppend(insert_mongo_agg_level_2_df, "GameProfitDB",
                           "voucher.detail", start_date, end_date)

    # insert_mongo_agg_level_2_df \
    #     .write \
    #     .format("com.mongodb.spark.sql.DefaultSource") \
    #     .mode('overwrite') \
    #     .option("database", "bi") \
    #     .option("collection", "revenue_spend_exchange_daily_agg_level_2") \
    #     .save()

if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None:
        logic()
    else:
        logic(argv["start_date"], argv["end_date"])


