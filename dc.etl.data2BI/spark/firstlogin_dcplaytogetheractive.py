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
    drop_partition.dropPartition("bi.revenue_income_propsmall_daily_agg_level_2_1", "dt", start_date, end_date)
    gspropsmalldb_mobileprops_df \
        .write.partitionBy("dt") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("bi.revenue_income_propsmall_daily_agg_level_2_1")



if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None:
        logic()
    else:
        logic(argv["start_date"], argv["end_date"])


