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
sparkSession = sparkInitialize().setAppName("revenue_silver_daily_agg").onHive().onMongo()

# 开始
spark = sparkSession.getOrCreate()

# 声明日志
logger = logging(spark,"warn")

# banner
logger.warn(sparkSession.showConf(),'config')
logger.warn("huangsai","author")


def logic(start_date=DEFAULT_START_DATE,end_date=DEFAULT_END_DATE):
    '''
    处理逻辑
    大致步骤  读取转换数据 -》 写入hive  -》 写入mongo
    '''

    hive_partition = partition(spark,logger)
    hive_partition.dropPartition("bi.revenue_silver_daily_agg_level_2","dt",start_date,end_date)
    hive_partition.dropPartition("bi.revenue_silver_daily_agg_level_1","dt",start_date,end_date)

    '''
    生成银子level2表
    '''

    execute_sql_level_2 = '''select 
                    NVL(t2.game_id, -1) game_id,
                    NVL(t2.game_code, '') game_code,
                    NVL(t2.date,-1) date,
                    NVL(t2.op_id,-1) op_id,
                    NVL(t2.op_name,'') op_name,
                    NVL(t2.package_type,-1) package_type,
                    NVL(t3.enum_value,'') as package_type_name,
                    NVL(t2.from_app_id,-1) from_app_id,
                    NVL(t2.from_app_code, '') from_app_code,
                    NVL(t2.os_type,-1) os_type,
                    NVL(t2.op_type_classified_name,'') op_type_classified_name,
                    t2.recom_game_id,
                    t2.recom_game_code,
                    t2.recom_game_relation,
                    t2.silver_amount,
                    t2.dt
                    from
                    (
                        select 
                        t1.game_id,
                        t1.game_code,
                        t1.date,
                        t1.op_id,
                        t1.op_name,
                        case when t1.os_type_id = 3 then 400 else t1.package_type_id end as package_type,
                        t1.from_app_id,
                        t1.from_app_code,
                        t1.os_type_id as os_type,
                        t1.op_type_classified_name,
                        t1.recom_game_id,
                        t1.recom_game_code,
                        t1.recom_game_relation,
                        sum(abs(t1.silver_deposit)) as silver_amount,
                        max(t1.dt) as dt
                    from 
                    (
                            select 
                            recom_game_id as game_id,
                            recom_game_code as game_code,
                            date,
                            op_id,
                            op_name,
                            package_type_id,
                            from_app_id,
                            from_app_code,
                            os_type_id,
                            game_id as recom_game_id,
                            game_code as recom_game_code,
                            2 as recom_game_relation,
                            silver_deposit,
                            case 
                            when op_type_id in(4,5) then 0
                            when op_type_id in(1,3) then 1
                            end as op_type_classified_name,
                            dt
                            from dwd.fact_silver_detail
                            where dt >= '%s' and dt < '%s' and log_source='gamelog' and op_type_id in(1,3,4,5) and (recom_game_id is not null or (recom_game_code is not null and recom_game_code!=''))
                        union all
                            select 
                            game_id,
                            game_code,
                            date,
                            op_id,
                            op_name,
                            package_type_id,
                            from_app_id,
                            from_app_code,
                            os_type_id,
                            recom_game_id,
                            recom_game_code,
                            case 
                            when recom_game_id is not null or (recom_game_code is not null and recom_game_code!='') then  3 
                            else 1
                            end as recom_game_relation,
                            silver_deposit,
                            case 
                            when op_type_id in(4,5) then 0
                            when op_type_id in(1,3) then 1
                            end as op_type_classified_name,
                            dt
                            from dwd.fact_silver_detail
                            where dt >= '%s' and dt < '%s' and log_source='gamelog' and op_type_id in(1,3,4,5)
                    ) t1
                    group by t1.game_id,t1.game_code,t1.date,t1.op_id,t1.op_name,t1.package_type_id,t1.from_app_id,t1.from_app_code,t1.os_type_id,t1.op_type_classified_name,t1.recom_game_relation,t1.recom_game_id,t1.recom_game_code
                    ) t2
                    left join 
                    (
                        select * from dwd.dim_common_enum_dict 
                        where enum_type = 'pkgtype'  
                    ) t3
                    on t2.package_type=t3.enum_key'''%(start_date,end_date,start_date,end_date)

    logger.warn(execute_sql_level_2,'sql')

    df_level_2 = spark.sql(execute_sql_level_2)

    df_level_2.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.revenue_silver_daily_agg_level_2")

    '''
    生成银子level1表
    '''

    execute_sql_level_1 = '''select 
                game_id,
                game_code,
                date,
                op_id,
                op_name,
                package_type,
                package_type_name,
                from_app_id,
                from_app_code,
                os_type,
                op_type_classified_name,
                recom_game_relation,
                sum(silver_amount) as silver_amount,
                dt
                from bi.revenue_silver_daily_agg_level_2
                where dt >= '%s' and dt < '%s'
                group by 
                game_id,
                game_code,
                date,
                op_id,
                op_name,
                package_type,
                package_type_name,
                from_app_id,
                from_app_code,
                os_type,
                op_type_classified_name,
                recom_game_relation,
                dt'''%(start_date,end_date)

    logger.warn(execute_sql_level_1,'sql')

    df_level_1 = spark.sql(execute_sql_level_1)

    df_level_1.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.revenue_silver_daily_agg_level_1")

    '''
    插入到MongoDB
    '''

    mongo = mongoExecute()
    mongo.collectionAppend(spark.sql('''select 
                                        game_id as gameId, 
                                        game_code as gameCode, 
                                        date , 
                                        op_id as operateId, 
                                        op_name as operateName, 
                                        package_type as packageTypeId, 
                                        from_app_id as fromAppId, 
                                        from_app_code as fromAppCode, 
                                        os_type as osType, 
                                        recom_game_relation as recommendRelation, 
                                        silver_amount as silvers, 
                                        op_type_classified_name as type,
                                        dt
                                        from
                                        bi.revenue_silver_daily_agg_level_1
                                        where dt >= '%s' and dt < '%s' '''%(start_date,end_date)) , "GameProfitDB", "silver.brief",start_date,end_date)
    mongo.collectionAppend(spark.sql('''select 
                                        game_id as gameId, 
                                        game_code as gameCode, 
                                        date , 
                                        op_id as operateId, 
                                        package_type as packageTypeId, 
                                        from_app_id as fromAppId, 
                                        from_app_code as fromAppCode, 
                                        os_type as osType, 
                                        recom_game_id as relateGameId, 
                                        recom_game_code as relateGameCode, 
                                        recom_game_relation as recommendRelation, 
                                        silver_amount as silvers, 
                                        op_type_classified_name as type,
                                        dt
                                        from
                                        bi.revenue_silver_daily_agg_level_2
                                        where dt >= '%s' and dt < '%s' '''%(start_date,end_date)) , "GameProfitDB", "silver.detail",start_date,end_date)



if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None or argv["start_date"] == "" or argv["end_date"] == "":
        logic()
    else:
        logic(argv["start_date"],argv["end_date"])
