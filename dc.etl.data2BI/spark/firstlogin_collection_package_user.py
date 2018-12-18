#coding=utf-8

from perform.SparkInit import sparkInitialize
from utils.logging import logging
from perform.MongoExecute import mongoExecute
import time


# 声明spark session
sparkSession = sparkInitialize().setAppName("uid_firstlogin_collection_package").onHive().onMongo()

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
                  select a.uid,a.`date`,a.time,b.fromappCode,b.channel,b.group,b.app
                from
                (select 
                uid	,
                cast(substr(min(datetimes),1,8) as int) `date`,
                cast(substr(min(datetimes),9,6) as int) time,
                min(datetimes) datetimes
                from bi.firstlogin_whole
                where app = 3003 or pkgtype=1000
                group by uid
                ) a 
                left join 
                (
                select uid,datetimes,fromappCode,channel,group,app,row_number() over ( partition by uid,datetimes order by datetimes asc ) num 
                from
                bi.firstlogin_whole 
                where app = 3003 or pkgtype=1000
                )b
                on a.uid=b.uid and a.datetimes=b.datetimes
                where b.num=1
                                 '''

    logger.warn(execute_sql_full_table,'sql')

    df_full_table = spark.sql(execute_sql_full_table)

    df_full_table.write.mode('overwrite').format("orc").saveAsTable("bi.uid_firstlogin_collection_package")


    '''
    将生成的数据增量插入至mongo中
    '''

    mongo = mongoExecute()


    # out_put_sql = '''
    #                           select
    #                             uid,
    #                             fromappcode,
    #                             date,
    #                             time,
    #                             channel,
    #                             group,
    #                             app
    #                             from bi.uid_firstlogin_collection_package
    #                         '''
    #
    # logger.warn(out_put_sql, 'sql')
    #
    # df_out_put = spark.sql(out_put_sql)

    mongo.collectionOverwrite(df_full_table,"bi","uid_firstlogin_collection_package")


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