#coding=utf-8

from perform.SparkInit import sparkInitialize
from utils.logging import logging
from pyspark import SparkConf
import time,datetime,sys
from utils.SubmitArguments import arguments

# 默认开始时间
DEFAULT_START_DATE = str(datetime.datetime.strptime(time.strftime('%Y%m%d',time.localtime()), '%Y%m%d') + datetime.timedelta(days=-1))[0:10].replace("-","")
# 默认结束时间
DEFAULT_END_DATE = time.strftime('%Y%m%d',time.localtime())

# 声明spark session
sparkSession = sparkInitialize().setAppName("demo").onHive().onMongo()

# 开始
spark = sparkSession.getOrCreate()

# 声明日志
logger = logging(spark,"warn")

# banner
logger.warn(sparkSession.showConf(),'config')
logger.warn("zhangwei","author")


def logic(start_date=DEFAULT_START_DATE,end_date=DEFAULT_END_DATE):
    '''
    处理逻辑
    大致步骤  读取转换数据 -》 写入hive  -》 写入mongo
    '''
    execute_sql = "select name,age,dt from tempdb.people where dt >= '%s' and dt < '%s'" %(start_date,end_date)

    logger.warn(execute_sql,'sql')

    df = spark.sql(execute_sql)

    df.write.partitionBy("dt").mode('overwrite').format("orc").saveAsTable("tempdb.fact_silver_detail_2")

    df.write.format("com.mongodb.spark.sql.DefaultSource").mode('overwrite').option("database","TestDB").option("collection", "silvertest").save()



if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None:
        logic()
    else:
        logic(argv["start_date"],argv["start_date"])
