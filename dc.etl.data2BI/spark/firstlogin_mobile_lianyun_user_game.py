#coding=utf-8

from perform.SparkInit import sparkInitialize
from utils.logging import logging
from perform.MongoExecute import mongoExecute
import time,datetime,sys
from utils.SubmitArguments import arguments
from utils.HiveAlter import partition


# 默认开始时间
DEFAULT_START_DATE = str(datetime.datetime.strptime(time.strftime('%Y%m%d',time.localtime()), '%Y%m%d') + datetime.timedelta(days=-1))[0:10].replace("-","")

DEFAULT_END_DATE = time.strftime('%Y%m%d',time.localtime())

# 声明spark session
sparkSession = sparkInitialize().setAppName("firstlogin_mobile_lianyun_user_game").onHive().onMongo()

# 开始
spark = sparkSession.getOrCreate()

# 声明日志
logger = logging(spark,"warn")

# banner
logger.warn(sparkSession.showConf(),'config')
logger.warn("Author:liugf","banner")


def logic(start_date=DEFAULT_START_DATE,end_date=DEFAULT_END_DATE):

    '''
    处理逻辑
    大致步骤  删除分区 -》 中间表 -》插入最终表
    '''
    # 删除分区
    hive_partition = partition(spark, logger)

    hive_partition.dropPartition("bi.firstlogin_mobile_lianyun_user_game", "dt", start_date, end_date)

    execute_sql_increase_table = '''
           select t6.uid,
               t6.date,
               t6.time,
               t6.channel,
               t6.group, 
               t6.app,
               t6.ostype, 
               t6.jointdatafrom
            from
        (select uid, app
            from bi.firstlogin_mobile_lianyun_user_game
        ) t5
        right join
        (select t3.uid uid, 
               cast(substr(t3.datetimes,1,8) as int) date,
               cast(substr(t3.datetimes,9) as int) time,
               t4.channel channel,
               t4.group group, 
               t3.app app, 
               t4.ostype ostype, 
               t4.jointdatafrom jointdatafrom
            from
        (select t1.app_id app,
               t2.uid uid,
               t2.datetimes datetimes
            from	
        (select app_id 
            from dwd.dim_game_dict 
            where run_type = '联运_手游联运'
        ) t1
        left join
        (select app,
               uid,
               min(datetimes) datetimes
            from bi.firstlogin_whole
            where dt>=%(sdate)s and dt < %(edate)s
            and uid is not null
            group by app, uid
        ) t2
        on t1.app_id = t2.app
        where t2.uid is not null
        group by t1.app_id, t2.uid, t2.datetimes  
        ) t3 
        left join
        (select t.uid uid,
               t.app app,
               t.datetimes datetimes,
               t.channel channel,
               t.group group,
               t.ostype ostype,
               t.jointdatafrom jointdatafrom
            from
        (select uid,
               app,
               min(datetimes) as datetimes,
               channel,
               group,
               ostype,
               jointdatafrom,
               row_number() over(partition by uid, app order by datetimes) as rn
            from bi.firstlogin_whole
            where dt>=%(sdate)s and dt < %(edate)s 
            and uid is not null
            group by uid,app,channel,group,ostype,jointdatafrom,datetimes
        ) t
        where rn =1
        ) t4
        on t3.datetimes = t4.datetimes and t3.uid = t4.uid and t3.app = t4.app
        ) t6
        on t5.uid = t6.uid and t5.app = t6.app
        where t5.uid is null
           ''' %{"sdate":start_date,"edate":end_date}

    logger.warn(execute_sql_increase_table, 'sql')

    increase_table = spark.sql(execute_sql_increase_table)

    increase_table.write.mode('overwrite').format("orc").saveAsTable("stag.firstlogin_mobile_lianyun_user_game")


# 中间数据插入最终表
    execute_sql_into_the_table = '''select
                                    t6.uid,
           t6.date,
           t6.time,
           t6.channel,
           t6.group, 
           t6.app,
           t6.ostype, 
           t6.jointdatafrom,
           t6.date as dt
        from stag.firstlogin_mobile_lianyun_user_game t6
        '''

    logger.warn(execute_sql_into_the_table, 'sql')

    the_end_table = spark.sql(execute_sql_into_the_table)

    the_end_table.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.firstlogin_mobile_lianyun_user_game")


    '''
    将生成的数据增量插入至mongo中
    '''

    mongo = mongoExecute()

    mongo.collectionAppend(the_end_table,"bi","firstlogin_mobile_lianyun_user_game", start_date, end_date)

    logger.warn("Job over", "banner")


if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None or argv["start_date"] == "" or argv["end_date"] == "":
        logic()
    else:
        logic(argv["start_date"],argv["end_date"])