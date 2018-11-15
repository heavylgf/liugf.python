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
sparkSession = sparkInitialize().setAppName("PropSmallMobilePropsTest").onHive().onMongo()

spark = sparkSession.getOrCreate()

logger = logging(spark, "WARN")

# banner
logger.warn(sparkSession.showConf(), 'config')
logger.warn("liugf", "author")


def logic(start_date=DEFAULT_START_DATE, end_date=DEFAULT_END_DATE):
    # Queries are expressed in HiveQL  currencytype(3) & currencytype(100)
    gspropsmalldb_mobileprops_sql = "select t1.game as game_id, " \
                                    "t1.gamecode as game_code, " \
                                    "t1.date as date, " \
                                    "t1.goodsid as goods_id, " \
                                    "t2.goods_name as goods_name, " \
                                    "t1.pkgtype as package_type, " \
                                    "t3.enum_value as package_type_name, " \
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
                                    "sum(cash_amount) as cash_amount, " \
                                    "sum(silver_amount) as silver_amount, " \
                                    "max(t1.dt) as dt " \
                                    "from " \
                                    "(select game, " \
                                    "gamecode, " \
                                    "date, " \
                                    "goodsid, " \
                                    "case when currencytype = 100 then currencynum else 0 end as cash_amount, " \
                                    "case when currencytype = 3 then currencynum else 0 end as silver_amount, " \
                                    "pkgtype, " \
                                    "fromapp, " \
                                    "fromappcode, " \
                                    "ostype, " \
                                    "recomgame, " \
                                    "recomgamecode, " \
                                    "currencynum, " \
                                    "dt " \
                                    "FROM ods.gspropsmalldb_mobileprops " \
                                    "where dt >= '%s' and dt < '%s' and currencytype in(100, 3) " \
                                    ") t1 " \
                                    "inner join " \
                                    "(select goods_id, " \
                                    "goods_name, " \
                                    "goods_type, " \
                                    "goods_class " \
                                    "from dwd.dim_goods_dict where goods_class = 2 " \
                                    ") t2 " \
                                    "on t1.goodsid = t2.goods_id " \
                                    "left join " \
                                    "(select enum_key, " \
                                    "enum_value, " \
                                    "enum_type " \
                                    "from dwd.dim_common_enum_dict " \
                                    "where enum_type = 'pkgtype' " \
                                    ") t3 " \
                                    "on t1.pkgtype = t3.enum_key " \
                                    "group by t1.game, " \
                                    "t1.gamecode, " \
                                    "t1.date, " \
                                    "t1.goodsid, " \
                                    "t2.goods_name, " \
                                    "t1.pkgtype, " \
                                    "t3.enum_value, " \
                                    "t1.fromapp, " \
                                    "t1.fromappcode, " \
                                    "t1.ostype, " \
                                    "t1.recomgame, " \
                                    "t1.recomgamecode " \
                                    % (start_date, end_date)

    logger.warn(gspropsmalldb_mobileprops_sql, 'gspropsmalldb_mobileprops_sql ')
    gspropsmalldb_mobileprops_df = spark.sql(gspropsmalldb_mobileprops_sql)

    drop_partition = partition(spark, logger)
    drop_partition.dropPartition("bi.revenue_income_propsmall_daily_agg_level_2", "dt", start_date, end_date)
    gspropsmalldb_mobileprops_df \
        .write.partitionBy("dt") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("bi.revenue_income_propsmall_daily_agg_level_2")

    gspaydb_basic_sql = "select t1.game as game_id, " \
                        "t1.gamecode as game_code, " \
                        "t1.paydate as date, " \
                        "t1.gamegoodsid as goods_id, " \
                        "t2.goods_name as goods_name, " \
                        "t1.pkgtype as package_type, " \
                        "t3.enum_value as package_type_name, " \
                        "t1.fromapp as from_app_id, " \
                        "t1.fromappcode as from_app_code, " \
                        "t1.ostype as os_type, " \
                        "t1.recomgame as recom_game_id, " \
                        "t1.recomgamecode as recom_game_code, " \
                        "case " \
                        "when t1.recomgame is null then 1 " \
                        "when t1.recomgame is not null then 3 " \
                        "end " \
                        "as recom_game_relation," \
                        "sum(t1.price) as cash_amount, " \
                        "0 as silver_amount, " \
                        "t1.dt " \
                        "from " \
                        "(select game, " \
                        "gamecode, " \
                        "paydate, " \
                        "gamegoodsid ," \
                        "pkgtype, " \
                        "fromapp, " \
                        "fromappcode, " \
                        "ostype, " \
                        "recomgame, " \
                        "recomgamecode, " \
                        "prodver, " \
                        "price, " \
                        "paydate as dt " \
                        "FROM ods.gspaydb_basic " \
                        "where dt='%s' and paydate >= '%s' and paydate < '%s' " \
                        "and (prodver is null or prodver = '') and  product = 6001 " \
                        ") t1 " \
                        "inner join " \
                        "(select goods_id, " \
                        "goods_name, " \
                        "goods_type, " \
                        "goods_class " \
                        "from dwd.dim_goods_dict where goods_class = 2 " \
                        ") t2 " \
                        "on t1.gamegoodsid = t2.goods_id " \
                        "left join " \
                        "(select enum_key, " \
                        "enum_value, " \
                        "enum_type " \
                        "from dwd.dim_common_enum_dict " \
                        "where enum_type = 'pkgtype' " \
                        ") t3 " \
                        "on t1.pkgtype = t3.enum_key " \
                        "group by t1.game, " \
                        "t1.gamecode, " \
                        "t1.paydate, " \
                        "t1.gamegoodsid, " \
                        "t2.goods_name, " \
                        "t1.pkgtype, " \
                        "t3.enum_value, " \
                        "t1.fromapp, " \
                        "t1.fromappcode, " \
                        "t1.ostype, " \
                        "t1.recomgame, " \
                        "t1.recomgamecode, " \
                        "t1.dt " \
                        % (start_date[0:6], start_date, end_date)

    logger.warn(gspaydb_basic_sql, 'gspaydb_basic_sql ')
    gspaydb_basic_df = spark.sql(gspaydb_basic_sql)

    gspaydb_basic_df \
        .write.partitionBy("dt") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("bi.revenue_income_propsmall_daily_agg_level_2")

    main_recommendation_sql = "select recom_game_id as game_id, " \
                              "recom_game_code as game_code, " \
                              "date, " \
                              "goods_id, " \
                              "goods_name, " \
                              "package_type, " \
                              "package_type_name, " \
                              "from_app_id, " \
                              "from_app_code, " \
                              "os_type, " \
                              "game_id as recom_game_id, " \
                              "game_code as recom_game_code, " \
                              "2 as recom_game_relation, " \
                              "cash_amount, " \
                              "silver_amount, " \
                              "dt " \
                              "from bi.revenue_income_propsmall_daily_agg_level_2 " \
                              "where dt >= '%s' and dt < '%s'" \
                              " and recom_game_relation = 3 " \
                              % (start_date, end_date)

    logger.warn(main_recommendation_sql, 'main_recommendation_sql ')
    main_recommendation_df = spark.sql(main_recommendation_sql)

    main_recommendation_df \
        .write \
        .partitionBy("dt") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("bi.revenue_income_propsmall_daily_agg_level_2")

    # insert to agg_level_1
    agg_level_1_sql = "select game_id, " \
                      "game_code, " \
                      "date, " \
                      "goods_id, " \
                      "goods_name, " \
                      "package_type, " \
                      "package_type_name, " \
                      "from_app_id, " \
                      "from_app_code, " \
                      "os_type, " \
                      "recom_game_relation, " \
                      "sum(cash_amount) as cash_amount, " \
                      "sum(silver_amount) as silver_amount, " \
                      "dt " \
                      "from bi.revenue_income_propsmall_daily_agg_level_2 " \
                      "where dt >= '%s' and dt < '%s' " \
                      "group by game_id, " \
                      "game_code, " \
                      "date, " \
                      "goods_id, " \
                      "goods_name, " \
                      "package_type, " \
                      "package_type_name, " \
                      "from_app_id, " \
                      "from_app_code, " \
                      "os_type, " \
                      "recom_game_relation, " \
                      "dt " \
                      % (start_date, end_date)

    drop_partition.dropPartition("bi.revenue_income_propsmall_daily_agg_level_1", "dt", start_date, end_date)
    # drop_partition.dropPartition("bi.revenue_spend_exchange_daily_agg_level_1", "dt", '20181107', '20181108')

    logger.warn(agg_level_1_sql, 'agg_level_1_sql ')
    agg_level_1_df = spark.sql(agg_level_1_sql)

    agg_level_1_df \
        .write \
        .partitionBy("dt") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("bi.revenue_income_propsmall_daily_agg_level_1")

    # insert into  agg_level_1 to mongoDB
    insert_mongo_agg_level_1_sql = "select game_id as gameId, " \
                                   "if(game_code is null or game_code = '', '', game_code) as gameCode, " \
                                   "date as date, " \
                                   "goods_id as propId, " \
                                   "goods_name as propName, " \
                                   "package_type as packageTypeId, " \
                                   "if(package_type_name is null or package_type_name = '', '', package_type_name) " \
                                   "as packageTypeName, " \
                                   "from_app_id as fromAppId, " \
                                   "case " \
                                   "when from_app_code is null then ' ' " \
                                   "when from_app_code = '' then ' ' " \
                                   "else from_app_code " \
                                   "end " \
                                   "as fromAppCode, " \
                                   "os_type as osType, " \
                                   "recom_game_relation as recommendRelation, " \
                                   "if(cash_amount is null or cash_amount = '', 0, cash_amount) as money, " \
                                   "if(silver_amount is null or silver_amount = '', 0, silver_amount) as silvers, " \
                                   "dt " \
                                   "from bi.revenue_income_propsmall_daily_agg_level_1 " \
                                   "where dt >= '%s' and dt < '%s' " \
                                   % (start_date, end_date) \
        # % (20181108, 20181109)

    logger.warn(insert_mongo_agg_level_1_sql, 'insert_mongo_agg_level_1_sql ')
    insert_mongo_agg_level_1_df = spark.sql(insert_mongo_agg_level_1_sql)

    mongo = mongoExecute()
    mongo.collectionAppend(insert_mongo_agg_level_1_df, "GameProfitDB",
                           "unique_prop_income.brief", start_date, end_date)

    # insert into agg_level_2 to mongoDB
    insert_mongo_agg_level_2_sql = "select game_id as gameId, " \
                                   "if(game_code is null or game_code = '', '', game_code) as gameCode, " \
                                   "date as date, " \
                                   "goods_id as propId, " \
                                   "goods_name as propName, " \
                                   "package_type as packageTypeId, " \
                                   "if(package_type_name is null or package_type_name = '', '', package_type_name) " \
                                   "as packageTypeName, " \
                                   "from_app_id as fromAppId, " \
                                   "case " \
                                   "when from_app_code is null then ' ' " \
                                   "when from_app_code = '' then ' ' " \
                                   "else from_app_code " \
                                   "end " \
                                   "as fromAppCode, " \
                                   "os_type as osType, " \
                                   "recom_game_id as relateGameId, " \
                                   "if(recom_game_code is null or recom_game_code = '', '', recom_game_code) " \
                                   "as relateGameCode, " \
                                   "recom_game_relation as  recommendRelation, " \
                                   "if(cash_amount is null or cash_amount = '', 0, cash_amount) as money, " \
                                   "if(silver_amount is null or silver_amount = '', 0, silver_amount) as silvers, " \
                                   "dt " \
                                   "from bi.revenue_income_propsmall_daily_agg_level_2 " \
                                   "where dt >= '%s' and dt < '%s' " \
                                   % (start_date, end_date)
    # % (20181108, 20181109)

    logger.warn(insert_mongo_agg_level_2_sql, 'insert_mongo_agg_level_2_sql ')
    insert_mongo_agg_level_2_df = spark.sql(insert_mongo_agg_level_2_sql)

    mongo.collectionAppend(insert_mongo_agg_level_2_df, "GameProfitDB",
                           "unique_prop_income.detail", start_date, end_date)


if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None:
        logic()
    else:
        logic(argv["start_date"], argv["end_date"])
