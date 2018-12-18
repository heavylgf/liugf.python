#coding=utf-8

from perform.SparkInit import sparkInitialize
from utils.logging import logging
from perform.MongoExecute import mongoExecute
import time


# 声明spark session
sparkSession = sparkInitialize().setAppName("hardid_firstlogin_collection_package").onHive().onMongo()

# 开始
spark = sparkSession.getOrCreate()

# 声明日志
logger = logging(spark,"warn")

# banner
logger.warn(sparkSession.showConf(),'config')
logger.warn("Author:liugf","banner")


def logic():

    '''
    处理逻辑
    大致步骤  全表扫描
    '''

    execute_sql_full_table = '''
  select t2.fromappcode, t1.date, t1.time, t1.hardid, 
                                       t2.channel,
                                       t2.group,
                                       t2.app
                                    from 
                                    (select hardid,
                                       cast(substr(min(datetimes),1,8) as int) date,
                                       cast(substr(min(datetimes),9) as int) time,
                                       min(datetimes) datetimes
                                    from bi.firstlogin_whole
                                    where app = 3003 or pkgtype = 1000
                                    group by hardid
                                ) t1
                                left join
                                (select t.fromappcode fromappcode,
                                       t.datetimes datetimes,
                                       t.hardid hardid,
                                       t.channel channel,
                                       t.group group,
                                       t.app app
                                    from
                                (select fromappcode,
                                       min(datetimes) as datetimes,
                                       hardid,
                                       channel,
                                       group,
                                       app,
                                       row_number() over(partition by hardid order by datetimes) as rn
                                    from bi.firstlogin_whole
                                    where app = 3003 or pkgtype = 1000	
                                    group by fromappcode,datetimes,hardid,channel,group,app
                                ) t 
                                where rn = 1
                                ) t2
                                on t1.datetimes = t2.datetimes and t1.hardid = t2.hardid
                                 '''

    logger.warn(execute_sql_full_table,'sql')

    df_full_table = spark.sql(execute_sql_full_table)

    df_full_table.write.mode('overwrite').format("orc").saveAsTable("bi.hardid_firstlogin_collection_package")


    '''
    将生成的数据增量插入至mongo中
    '''

    mongo = mongoExecute()


    # out_put_sql = '''
    #                           select
    #                             fromappcode,
    #                             date,
    #                             time,
    #                             hardid,
    #                             channel,
    #                             group,
    #                             app
    #                             from bi.hardid_firstlogin_collection_package
    #                         '''
    #
    # logger.warn(out_put_sql, 'sql')
    #
    # df_out_put = spark.sql(out_put_sql)

    mongo.collectionOverwrite(df_full_table,"bi","hardid_firstlogin_collection_package")


    # # 作业完成标识
    # update_status_sql = "select 'hardid_firstlogin_collection_package' as collection, %s as date" %(time.strftime('%Y%m%d',time.localtime()))
    #
    # logger.warn(update_status_sql, 'sql')
    #
    # df_out_put = spark.sql(update_status_sql)
    #
    # mongo.collectionAppend(df_out_put,"bi","update_job_status")

    logger.warn("Job over", "banner")


if __name__ == "__main__":
    logic()