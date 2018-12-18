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
sparkSession = sparkInitialize().setAppName("firstlogin_yaodou_user_game").onHive().onMongo()

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

    hive_partition.dropPartition("bi.firstlogin_yaodou_user_game", "dt", start_date, end_date)

    execute_sql_increase_table = '''
  select t6.uid, 
       t6.date,
	   t6.group, 
	   t6.game
    from
(select uid, game
    from bi.firstlogin_yaodou_user_game
) t5
right join

(select t3.uid uid, 
       cast(substr(t3.datetimes,1,8) as int) date,
	   t4.group group, 
	   t3.game game
	from
(select t1.game_id game,
       t2.uid uid,
	   t2.datetimes datetimes
    from
(select game_id 
    from dwd.dim_game_dict 
    where run_type = '联运_妖豆'
) t1
left join
(select game,
	   uid,
	   min(datetimes) datetimes
	from bi.firstlogin_whole
	where dt>=%(sdate)s and dt < %(edate)s
	and uid is not null and game is not null 
	and group > 0 and group not in (6,66,8,88,68,69,55)
	group by game, uid
) t2
on t1.game_id = t2.game
where t2.uid is not null 
group by t1.game_id, t2.uid, t2.datetimes
) t3 

left join
(select t.uid uid,
       t.game game,
	   t.datetimes datetimes,
	   t.group group
	from
(select uid,
       game,
	   min(datetimes) as datetimes,
	   group,
	   row_number() over(partition by uid, game order by datetimes) as rn
	from bi.firstlogin_whole
	where dt>=%(sdate)s and dt < %(edate)s
	and uid is not null
	group by uid,game,group,datetimes
) t
where rn =1
) t4
on t3.datetimes = t4.datetimes and t3.uid = t4.uid and t3.game = t4.game
) t6

on t5.uid = t6.uid and t5.game = t6.game
where t5.uid is null

           ''' %{"sdate":start_date,"edate":end_date}

    logger.warn(execute_sql_increase_table,'sql')

    increase_table = spark.sql(execute_sql_increase_table)

    increase_table.write.mode('overwrite').format("orc").saveAsTable("stag.firstlogin_yaodou_user_game")


# 中间数据插入最终表
    execute_sql_into_the_table = '''select
                                    t6.uid, 
       t6.date,
	   t6.group, 
	   t6.game,
	   t6.date as dt
    from stag.firstlogin_yaodou_user_game t6
    '''

    logger.warn(execute_sql_into_the_table, 'sql')

    the_end_table = spark.sql(execute_sql_into_the_table)

    the_end_table.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.firstlogin_yaodou_user_game")


    '''
    将生成的数据增量插入至mongo中
    '''

    mongo = mongoExecute()

    mongo.collectionAppend(the_end_table,"bi","firstlogin_yaodou_user_game", start_date, end_date)

    logger.warn("Job over", "banner")


if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None or argv["start_date"] == "" or argv["end_date"] == "":
        logic()
    else:
        logic(argv["start_date"],argv["end_date"])