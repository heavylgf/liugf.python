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
sparkSession = sparkInitialize().setAppName("merge_logsdklog_mobile_detail").onHive()

spark = sparkSession.getOrCreate()

logger = logging(spark, "WARN")

# banner
logger.warn(sparkSession.showConf(), 'config')
logger.warn("liugf", "author")

def logic(start_date=DEFAULT_START_DATE, end_date=DEFAULT_END_DATE):
    # logsdklog_mobile_detail_sql_1 = "select uid, " \
    #                               "date, " \
    #                               "time, " \
    #                               "app_id, " \
    #                               "app_code, " \
    #                               "app_vers, " \
    #                               "app_promchann, " \
    #                               "area_address, " \
    #                               "area_province, " \
    #                               "area_city, " \
    #                               "area_district, " \
    #                               "area_ip, " \
    #                               "duration, " \
    #                               "session, " \
    #                               "sys, " \
    #                               "tcy_hardid, " \
    #                               "tcy_imei, " \
    #                               "tcy_mac, " \
    #                               "geo, " \
    #                               "geo_lat, " \
    #                               "geo_lon, " \
    #                               "remark_channelID, " \
    #                               "father_code, " \
    #                               "father_id, " \
    #                               "father_promchann, " \
    #                               "father_vers, " \
    #                               "event, " \
    #                               "eqpt_carrier, " \
    #                               "eqpt_facturer, " \
    #                               "eqpt_idfv, " \
    #                               "eqpt_mobile, " \
    #                               "eqpt_net, " \
    #                               "eqpt_os, " \
    #                               "eqpt_resolution-h, " \
    #                               "eqpt_resolution-w, " \
    #                               "eqpt_vers, " \
    #                               "dt " \
    #                               "from ods.logsdklog_mobile_detail " \
    #                               "where dt >= '%s' and dt < '%s' " \
    #                               % (start_date, end_date)
    #
    # logger.warn(logsdklog_mobile_detail_sql_1, 'logsdklog_mobile_detail_sql_1 ')
    # gspropsmalldb_mobileprops_df = spark.sql(logsdklog_mobile_detail_sql_1)
    #
    # gspropsmalldb_mobileprops_df.coalesce(1)
    #
    # drop_partition = partition(spark, logger)
    # drop_partition.dropPartition("ods.merge_logsdklog_mobile_detail_1", "dt", start_date, end_date)
    # gspropsmalldb_mobileprops_df \
    #     .coalesce(1) \
    #     .write \
    #     .partitionBy("dt") \
    #     .format("orc") \
    #     .mode("append") \
    #     .saveAsTable("ods.merge_logsdklog_mobile_detail_1")
    #
    # logsdklog_mobile_detail_sql = "select uid, " \
    #                               "date, " \
    #                               "time, " \
    #                               "app_id, " \
    #                               "app_code, " \
    #                               "app_vers, " \
    #                               "app_promchann, " \
    #                               "area_address, " \
    #                               "area_province, " \
    #                               "area_city, " \
    #                               "area_district, " \
    #                               "area_ip, " \
    #                               "duration, " \
    #                               "session, " \
    #                               "sys, " \
    #                               "tcy_hardid, " \
    #                               "tcy_imei, " \
    #                               "tcy_mac, " \
    #                               "geo, " \
    #                               "geo_lat, " \
    #                               "geo_lon, " \
    #                               "remark_channelID, " \
    #                               "father_code, " \
    #                               "father_id, " \
    #                               "father_promchann, " \
    #                               "father_vers, " \
    #                               "event, " \
    #                               "eqpt_carrier, " \
    #                               "eqpt_facturer, " \
    #                               "eqpt_idfv, " \
    #                               "eqpt_mobile, " \
    #                               "eqpt_net, " \
    #                               "eqpt_os, " \
    #                               "eqpt_resolution-h, " \
    #                               "eqpt_resolution-w, " \
    #                               "eqpt_vers, " \
    #                               "dt " \
    #                               "from ods.merge_logsdklog_mobile_detail_1 " \
    #                               "where dt >= '%s' and dt < '%s' " \
    #                               % (start_date, end_date)
    #
    # logger.warn(logsdklog_mobile_detail_sql, 'logsdklog_mobile_detail_sql ')
    # gspropsmalldb_mobileprops_df = spark.sql(logsdklog_mobile_detail_sql)
    #
    # gspropsmalldb_mobileprops_df.coalesce(1)
    #
    # drop_partition = partition(spark, logger)
    # drop_partition.dropPartition("ods.logsdklog_mobile_detail", "dt", start_date, end_date)
    # gspropsmalldb_mobileprops_df \
    #     .coalesce(1) \
    #     .write \
    #     .partitionBy("dt") \
    #     .format("orc") \
    #     .mode("append") \
    #     .saveAsTable("ods.logsdklog_mobile_detail")

    test_store_in_orc_sql_1 = "select name, " \
                              "age, " \
                              "datetime, " \
                              "date as date " \
                              "from tempdb.test_store_in_orc " \
                              "where date='2018-12-05' and sex = 'female' "

    logger.warn(test_store_in_orc_sql_1, 'test_store_in_orc_sql_1 ')
    test_store_in_orc_df_1 = spark.sql(test_store_in_orc_sql_1)

    drop_partition = partition(spark, logger)
    drop_partition.dropPartition("tempdb.test_store_in_orc_1", "date", start_date, end_date)
    test_store_in_orc_df_1 \
        .coalesce(1) \
        .write \
        .partitionBy("date") \
        .format("orc") \
        .mode("append") \
        .saveAsTable("tempdb.test_store_in_orc_1")

    test_store_in_orc_sql = "select name, " \
                            "age, " \
                            "datetime, " \
                            "date as date, 'female' as sex " \
                            "from tempdb.test_store_in_orc_1 " \
                            "where date='2018-12-05' "

    logger.warn(test_store_in_orc_sql, 'test_store_in_orc_sql ')
    test_store_in_orc_df = spark.sql(test_store_in_orc_sql)

    # drop_partition = partition(spark, logger)
    # drop_partition.dropPartition("tempdb.test_store_in_orc_df", "dt", start_date, end_date)
    test_store_in_orc_df \
        .coalesce(1) \
        .write \
        .partitionBy("date", "sex") \
        .format("orc") \
        .mode("overwrite") \
        .saveAsTable("tempdb.test_store_in_orc")


    # firstlogin_mobile_lianyun_user_sql = "select uid, " \
    #                                      "date, " \
    #                                      "time, " \
    #                                      "channel, " \
    #                                      "group, " \
    #                                      "ostype, " \
    #                                      "jointdatafrom, " \
    #                                      "dt " \
    #                                      "from bi.firstlogin_mobile_lianyun_user " \
    #                                      "where dt = 20180920 "
    #
    # logger.warn(firstlogin_mobile_lianyun_user_sql, 'firstlogin_mobile_lianyun_user_sql ')
    # firstlogin_mobile_lianyun_user_df = spark.sql(firstlogin_mobile_lianyun_user_sql)
    #
    # firstlogin_mobile_lianyun_user_df.show()
    #
    # drop_partition = partition(spark, logger)
    # drop_partition.dropPartition("bi.firstlogin_mobile_lianyun_user_1", "dt", start_date, end_date)
    # firstlogin_mobile_lianyun_user_df \
    #     .coalesce(1) \
    #     .write \
    #     .partitionBy("dt") \
    #     .format("orc") \
    #     .mode("append") \
    #     .saveAsTable("bi.firstlogin_mobile_lianyun_user_1")


    # drop_partition = partition(spark, logger)
    # drop_partition.dropPartition("bi.revenue_income_propsmall_daily_agg_level_2_1", "dt", start_date, end_date)
    # gspropsmalldb_mobileprops_df \
    #     .write.partitionBy("dt") \
    #     .format("orc") \
    #     .mode("append") \
    #     .saveAsTable("bi.revenue_income_propsmall_daily_agg_level_2_1")



    # logsdklog_mobile_detail_sql = "select uid, " \
    #                               "date, " \
    #                               "time, " \
    #                               "app_id, " \
    #                               "app_code, " \
    #                               "app_vers, " \
    #                               "app_promchann, " \
    #                               "area_address, " \
    #                               "area_province, " \
    #                               "area_city, " \
    #                               "area_district, " \
    #                               "area_ip, " \
    #                               "duration, " \
    #                               "session, " \
    #                               "sys, " \
    #                               "tcy_hardid, " \
    #                               "tcy_imei, " \
    #                               "tcy_mac, " \
    #                               "geo, " \
    #                               "geo_lat, " \
    #                               "geo_lon, " \
    #                               "remark_channelID, " \
    #                               "father_code, " \
    #                               "father_id, " \
    #                               "father_promchann, " \
    #                               "father_vers, " \
    #                               "event, " \
    #                               "eqpt_carrier, " \
    #                               "eqpt_facturer, " \
    #                               "eqpt_idfv, " \
    #                               "eqpt_mobile, " \
    #                               "eqpt_net, " \
    #                               "eqpt_os, " \
    #                               "eqpt_resolution-h, " \
    #                               "eqpt_resolution-w, " \
    #                               "eqpt_vers, " \
    #                               "dt " \
    #                               "from ods.merge_logsdklog_mobile_detail_1 " \
    #                               "where dt >= '%s' and dt < '%s' " \
    #                               % (start_date, end_date)
    #
    # logger.warn(logsdklog_mobile_detail_sql, 'logsdklog_mobile_detail_sql ')
    # gspropsmalldb_mobileprops_df = spark.sql(logsdklog_mobile_detail_sql)
    #
    # gspropsmalldb_mobileprops_df.coalesce(1)
    #
    # drop_partition = partition(spark, logger)
    # drop_partition.dropPartition("ods.logsdklog_mobile_detail", "dt", start_date, end_date)
    # gspropsmalldb_mobileprops_df \
    #     .coalesce(1) \
    #     .write \
    #     .partitionBy("dt") \
    #     .format("orc") \
    #     .mode("append") \
    #     .saveAsTable("ods.logsdklog_mobile_detail")

if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None or argv["start_date"] == "" or argv["end_date"] == "":
        logic()
    else:
        logic(argv["start_date"], argv["end_date"])




