#coding=utf-8

from perform.SparkInit import sparkInitialize
from utils.logging import logging
from perform.MongoExecute import mongoExecute
import time


# 声明spark session
sparkSession = sparkInitialize().setAppName("firstlogin_yaodou_user").onHive().onMongo()

# 开始
spark = sparkSession.getOrCreate()

# 声明日志
logger = logging(spark,"warn")

# banner
logger.warn(sparkSession.showConf(),'config')
logger.warn("Author:huangsai","banner")


def logic():

    '''
    处理逻辑
    大致步骤  全表扫描
    '''

    execute_sql_full_table = '''
  select t4.uid,t4.date,t7.group,t7.area_province,t7.area_city,t7.area_district
from
(
select 
t3.uid,
cast(substr(min(t3.datetimes),1,8) as int) `date`,
cast(substr(min(t3.datetimes),9,6) as int) time,
min(t3.datetimes) datetimes
from
(
    select 
    t1.uid, t1.group, t1.datetimes, t1.area_province, t1.area_city, t1.area_district
    from 
    (
    select uid, group, datetimes, area_province, area_city, area_district,game from 
    bi.firstlogin_whole 
    where group not in (6,66,8,88,68,69,55,56) and group >0
    ) t1
    join
    (
    select distinct game_id from
    dwd.dim_game_dict 
    where run_type='联运_妖豆' 
    ) t2
    on t1.game=t2.game_id
) t3
group by t3.uid
) t4
left join 
(
    select 
    t5.uid, t5.group, t5.datetimes, t5.area_province, t5.area_city, t5.area_district,row_number() over ( partition by t5.uid,t5.datetimes order by datetimes asc ) num 
    from 
    (
    select uid, group, datetimes, area_province, area_city, area_district,game from 
    bi.firstlogin_whole 
    where group not in (6,66,8,88,68,69,55,56) and group >0
    ) t5
    join
    (
    select distinct game_id from
    dwd.dim_game_dict 
    where run_type='联运_妖豆' 
    ) t6
    on t5.game=t6.game_id
) t7
on t4.uid=t7.uid and t4.datetimes=t7.datetimes
where t7.num=1
                                 '''

    logger.warn(execute_sql_full_table,'sql')

    df_full_table = spark.sql(execute_sql_full_table)

    df_full_table.write.mode('overwrite').format("orc").saveAsTable("bi.firstlogin_yaodou_user")


    '''
    将生成的数据增量插入至mongo中
    '''

    mongo = mongoExecute()

    mongo.collectionOverwrite(df_full_table,"bi","firstlogin_yaodou_user")

    logger.warn("Job over", "banner")


if __name__ == "__main__":
    logic()