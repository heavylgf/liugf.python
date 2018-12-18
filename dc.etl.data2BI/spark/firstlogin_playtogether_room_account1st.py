#coding=utf-8

from perform.SparkInit import sparkInitialize
from utils.logging import logging
from perform.MongoExecute import mongoExecute
import time, datetime, sys
from utils.SubmitArguments import arguments
from utils.HiveAlter import partition

# 默认开始时间
DEFAULT_START_DATE = str(datetime.datetime.strptime(time.strftime('%Y%m%d', time.localtime()), '%Y%m%d') +
                         datetime.timedelta(days=-1))[0:10].replace("-", "")

DEFAULT_END_DATE = time.strftime('%Y%m%d', time.localtime())

# 声明spark session
sparkSession = sparkInitialize().setAppName("firstlogin_playtogether_room_account1st").onHive().onMongo()

# 开始
spark = sparkSession.getOrCreate()

# 声明日志
logger = logging(spark, "warn")

# banner
logger.warn(sparkSession.showConf(), 'config')
logger.warn("Author:liugf", "banner")


def logic(start_date=DEFAULT_START_DATE, end_date=DEFAULT_END_DATE):

    '''
    处理逻辑
    大致步骤  删除分区 -》 中间表 -》插入最终表
    '''
    # 删除分区
    hive_partition = partition(spark, logger)

    hive_partition.dropPartition("bi.firstlogin_playtogether_room_account1st", "dt", start_date, end_date)

    execute_sql_increase_table = '''
            select t.uid, 
                t.app, 
                cast(t.date as int) date,
                cast(t.time as int) time,
                t.group, 
                t.province, t.city, t.district, t.hardid, t.roomno, t.roomtype
             from	
            (select uid,
                app, date, time, group, province, city, district, hardid, roomno, roomtype,
                cast(concat(cast(date as  string), 
                            case when length(time)=1 then concat('00000',cast(time as string)) 
                                 when length(time)=2 then concat('0000',cast(time as string))
                                 when length(time)=3 then concat('000',cast(time as string))
                                 when length(time)=4 then concat('00',cast(time as string)) 
                                 when length(time)=5 then concat('0',cast(time as string)) 
                                 else cast(time as string) 
                            end) as bigint) as date_time, 
                row_number() over(partition by uid, app order by cast(concat(cast(date as  string),
                case when length(time)=1 then concat('00000',cast(time as string)) 
                     when length(time)=2 then concat('0000',cast(time as string))
                     when length(time)=3 then concat('000',cast(time as string))
                     when length(time)=4 then concat('00',cast(time as string)) 
                     when length(time)=5 then concat('0',cast(time as string)) 
                     else cast(time as string) 
                end) as bigint) ) 
                as rn
                from ods.gsplaytogetherdb_gameactive
                where app is not null and group is not null and group in (6, 66, 8, 88)
            ) t
            where rn = 1
         '''

    logger.warn(execute_sql_increase_table, 'sql')

    increase_table = spark.sql(execute_sql_increase_table)

    increase_table.write.mode('overwrite').format("orc").saveAsTable("stag.firstlogin_playtogether_room_account1st")

    # 中间数据插入最终表
    execute_sql_into_the_table = '''
           select
               t2.uid,
               t2.app,
               t2.date,
               t2.time,
               t2.group,
               t2.province,
               t2.city,
               t2.district,
               t2.hardid,
               t2.roomno,
               t2.roomtype,
               t2.date as dt
           from stag.firstlogin_playtogether_room_account1st t2
       '''

    logger.warn(execute_sql_into_the_table, 'sql')

    the_end_table = spark.sql(execute_sql_into_the_table)

    the_end_table.write.partitionBy("dt").mode('append').format("orc") \
        .saveAsTable("bi.firstlogin_playtogether_room_account1st")

    '''
    将生成的数据增量插入至mongo中
    '''
    mongo = mongoExecute()

    mongo.collectionAppend(the_end_table, "bi", "firstlogin_playtogether_room_account1st", start_date, end_date)

    logger.warn("Job over", "banner")

    # execute_sql_increase_table = '''
    #     select t2.uid,
    #         t2.app,
    #         t2.date,
    #         t2.time,
    #         t2.group,
    #         t2.province,
    #         t2.city,
    #         t2.district,
    #         t2.hardid,
    #         t2.roomno,
    #         t2.roomtype
    #         from
    #     (select uid,
    #         app,
    #         date,
    #         time,
    #         group,
    #         province,
    #         city,
    #         district,
    #         hardid,
    #         roomno,
    #         roomtype
    #         from bi.firstlogin_playtogether_room_account1st
    #     ) t1
    #     right join
    #     (select t.uid uid,
    #             t.app app,
    #             cast(t.date as int) date,
    #             cast(t.time as int) time,
    #             t.group group,
    #             t.province province,
    #             t.city city,
    #             t.district district,
    #             t.hardid hardid,
    #             t.roomno roomno,
    #             t.roomtype roomtype
    #      from
    #     (select uid,
    #         app, date, time, group, province, city, district, hardid, roomno, roomtype,
    #         cast(concat(cast(date as  string),
    #                     case when length(time)=1 then concat('00000',cast(time as string))
    #                          when length(time)=2 then concat('0000',cast(time as string))
    #                          when length(time)=3 then concat('000',cast(time as string))
    #                          when length(time)=4 then concat('00',cast(time as string))
    #                          when length(time)=5 then concat('0',cast(time as string))
    #                          else cast(time as string)
    #                     end) as bigint) as date_time,
    #         row_number() over(partition by uid, app order by cast(concat(cast(date as  string),
    #         case when length(time)=1 then concat('00000',cast(time as string))
    #              when length(time)=2 then concat('0000',cast(time as string))
    #              when length(time)=3 then concat('000',cast(time as string))
    #              when length(time)=4 then concat('00',cast(time as string))
    #              when length(time)=5 then concat('0',cast(time as string))
    #              else cast(time as string)
    #         end) as bigint))
    #         as rn
    #         from ods.gsplaytogetherdb_gameactive
    #         where app is not null and group is not null and group in (6, 66, 8, 88)
    #     ) t
    #     where rn = 1
    #     ) t2
    #     on t1.uid = t2.uid and t1.app = t2.app
    #     where t1.uid is null
    #  '''

    # logger.warn(execute_sql_increase_table, 'sql')
    #
    # increase_table = spark.sql(execute_sql_increase_table)
    #
    # increase_table.write.mode('overwrite').format("orc").saveAsTable("stag.firstlogin_playtogether_room_account1st")
    #
    # # 中间数据插入最终表
    # execute_sql_into_the_table = '''
    #     select
    #         t2.uid,
    #         t2.app,
    #         t2.date,
    #         t2.time,
    #         t2.group,
    #         t2.province,
    #         t2.city,
    #         t2.district,
    #         t2.hardid,
    #         t2.roomno,
    #         t2.roomtype,
    #         '20181217' as dt
    #     from stag.firstlogin_playtogether_room_account1st t2
    # '''
    #
    # logger.warn(execute_sql_into_the_table, 'sql')
    #
    # the_end_table = spark.sql(execute_sql_into_the_table)
    #
    # the_end_table.write.partitionBy("dt").mode('append').format("orc") \
    #     .saveAsTable("bi.firstlogin_playtogether_room_account1st")
    #
    # '''
    # 将生成的数据增量插入至mongo中
    # '''
    # mongo = mongoExecute()
    #
    # mongo.collectionAppend(the_end_table, "bi", "firstlogin_playtogether_room_account1st", start_date, end_date)
    #
    # logger.warn("Job over", "banner")

if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None or argv["start_date"] == "" or argv["end_date"] == "":
        logic()
    else:
        logic(argv["start_date"], argv["end_date"])
