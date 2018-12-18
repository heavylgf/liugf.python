#coding=utf-8

from perform.SparkInit import sparkInitialize
from utils.logging import logging
from pyspark import SparkConf
import time,datetime,sys
from utils.SubmitArguments import arguments
from utils.HiveAlter import partition
from perform.MongoExecute import mongoExecute

# 默认开始时间
DEFAULT_START_DATE = str(datetime.datetime.strptime(time.strftime('%Y%m%d',time.localtime()), '%Y%m%d') + datetime.timedelta(days=-1))[0:10].replace("-","")
# 默认结束时间
DEFAULT_END_DATE = time.strftime('%Y%m%d',time.localtime())

# 声明spark session
sparkSession = sparkInitialize().setAppName("firstlogin_whole").onHive()

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
    大致步骤  生成中间表 -》 删除最终表分区 -》插入最终表

    创建表
    create table if not exists bi.firstlogin_whole (
        uid	int,
        group	int,
        app	int,
        fromapp	int,
        game	int,
        channel	int,
        promcode	int,
        hardid	string,
        sys	int,
        gamecode	string,
        datatype	int,
        pkgtype	int,
        fromappcode	string,
        ostype	int,
        jointdatafrom	int,
        datetimes   string,
        area_province string,
        area_city  string,
        area_district string)
        PARTITIONED BY (
        dt     string)
        STORED AS ORC

    创建中间表
    create table if not exists stag.firstlogin_whole (
        uid	int,
        group	int,
        app	int,
        fromapp	int,
        game	int,
        channel	int,
        promcode	int,
        hardid	string,
        sys	int,
        gamecode	string,
        datatype	int,
        pkgtype	int,
        fromappcode	string,
        ostype	int,
        jointdatafrom	int,
        datetimes   string,
        area_province string,
        area_city  string,
        area_district string)
        STORED AS ORC

    '''

    '''
    按时间筛选获得时间段内的最小粒度
    '''

    execute_sql_to_stag = '''select 
                                    uid,
                                    group,
                                    app,
                                    fromapp,
                                    game,
                                    channel,
                                    promcode,
                                    hardid,
                                    sys,
                                    gamecode,
                                    datatype,
                                    pkgtype,
                                    fromappcode,
                                    ostype,
                                    jointdatafrom,
                                    min(
                                    concat(
                                    cast(date as string),
                                    case when length(time)=1 then concat('00000',cast(time as string)) 
                                         when length(time)=2 then concat('0000',cast(time as string))
                                         when length(time)=3 then concat('000',cast(time as string))
                                         when length(time)=4 then concat('00',cast(time as string))
                                         when length(time)=5 then concat('0',cast(time as string))
                                         when length(time)=6 then cast(time as string)
                                         end ))  datetimes,
                                    area_province, 
                                    area_city, 
                                    area_district
                                from ods.gslogindb_logindetail
                                where uid is not null and uid <> 0 and dt >=%(sdate)s and dt <%(edate)s
                                group by 
                                    uid,
                                    group,
                                    app,
                                    fromapp,
                                    game,
                                    channel,
                                    promcode,
                                    hardid,
                                    sys,
                                    gamecode,
                                    datatype,
                                    pkgtype,
                                    fromappcode,
                                    ostype,
                                    jointdatafrom,
                                    area_province, 
                                    area_city, 
                                    area_district
                                    ''' % {"sdate":start_date,"edate":end_date}

    logger.warn(execute_sql_to_stag,'sql')

    df_stag = spark.sql(execute_sql_to_stag)

    df_stag.write.mode('overwrite').format("orc").saveAsTable("stag.firstlogin_whole")


    '''
    删除最终表的分区
    '''
    hive_partition = partition(spark,logger)

    hive_partition.dropPartition("bi.firstlogin_whole","dt",start_date,end_date)

    '''
    插入最终表
    '''
    execute_sql_to_final = '''
                            select
                            uid,          
                            group,        
                            app,          
                            fromapp,      
                            game,         
                            channel,      
                            promcode,     
                            hardid,       
                            sys,          
                            gamecode,     
                            datatype,     
                            pkgtype,      
                            fromappcode,  
                            ostype,       
                            jointdatafrom, 
                            datetimes,
                            area_province,  
                            area_city,    
                            area_district,
                            substr(datetimes,0,8) as dt
                            from (
                            select 
                            /*+ STREAMTABLE(a) */
                            b.uid,          
                            b.group,        
                            b.app,          
                            b.fromapp,      
                            b.game,         
                            b.channel,      
                            b.promcode,     
                            b.hardid,       
                            b.sys,          
                            b.gamecode,     
                            b.datatype,     
                            b.pkgtype,      
                            b.fromappcode,  
                            b.ostype,       
                            b.jointdatafrom, 
                            b.area_province,  
                            b.area_city,    
                            b.area_district,
                            b.datetimes,
                            case when a.datetimes > b.datetimes then 1 when a.uid is null then 1 else 2 end judge 
                            from bi.firstlogin_whole  a
                            full outer join stag.firstlogin_whole b on 
                            NVL(a.uid,'')=NVL(b.uid,'') and 
                            NVL(a.group,'')=NVL(b.group,'') and 
                            NVL(a.app,'')=NVL(b.app,'') and 
                            NVL(a.fromapp,'')=NVL(b.fromapp,'') and 
                            NVL(a.game,'')=NVL(b.game,'') and 
                            NVL(a.channel,'')=NVL(b.channel,'') and 
                            NVL(a.promcode,'')=NVL(b.promcode,'') and 
                            NVL(a.hardid,'')=NVL(b.hardid,'') and 
                            NVL(a.sys,'')=NVL(b.sys,'') and 
                            NVL(a.gamecode,'')=NVL(b.gamecode,'') and 
                            NVL(a.datatype,'')=NVL(b.datatype,'') and 
                            NVL(a.pkgtype,'')=NVL(b.pkgtype,'') and 
                            NVL(a.fromappcode,'')=NVL(b.fromappcode,'') and 
                            NVL(a.ostype,'')=NVL(b.ostype,'') and 
                            NVL(a.jointdatafrom,'')=NVL(b.jointdatafrom,'') and 
                            NVL(a.area_province,'')=NVL(b.area_province,'') and 
                            NVL(a.area_city,'')=NVL(b.area_city,'') and 
                            NVL(a.area_district,'')=NVL(b.area_district,'')
                            ) t1
                            where 
                            judge = 1

    '''

    logger.warn(execute_sql_to_final,'sql')

    df_final = spark.sql(execute_sql_to_final)

    df_final.write.partitionBy("dt").mode("append").format("orc").saveAsTable("bi.firstlogin_whole")




if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None or argv["start_date"] == "" or argv["end_date"] == "":
        logic()
    else:
        logic(argv["start_date"],argv["end_date"])
