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
sparkSession = sparkInitialize().setAppName("revenue_spend_coupon_cash_daily_agg").onHive().onMongo()

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
    hive_partition.dropPartition("bi.revenue_spend_coupon_cash_daily_agg_level_2","dt",start_date,end_date)
    hive_partition.dropPartition("bi.revenue_spend_coupon_cash_daily_agg_level_1","dt",start_date,end_date)

    '''
    生成优惠券level2表
    '''

    execute_sql_level_2 = '''select
                    NVL(a.game_id,-1) game_id,
                    NVL(a.game_code,'') game_code,
                    NVL(a.date,-1) date,
                    NVL(a.package_type,-1) package_type_id,
                    NVL(b.enum_value,'') package_type_name,
                    NVL(a.from_app_id,-1) from_app_id,
                    NVL(a.from_app_code, '') from_app_code,
                    NVL(a.os_type,-1) os_type,
                    a.recom_game_id,
                    a.recom_game_code,
                    a.recom_game_relation,
                    a.dt,
                    a.amount
                    from 
                    (
                    select 
                    game_id,
                    game_code,
                    date,
                    package_type,
                    from_app_id,
                    from_app_code,
                    os_type,
                    recom_game_id,
                    recom_game_code,
                    recom_game_relation,
                    cast(date as string) as dt,
                    sum(value) as amount
                    from 
                    (
                    select
                    game as game_id,
                    gamecode as game_code, 
                    date, 
                    case when ostype = 3 then 400 else pkgtype end as package_type,
                    fromapp as from_app_id, 
                    fromappcode as from_app_code, 
                    ostype as os_type, 
                    recomgame as recom_game_id, 
                    recomgamecode as recom_game_code,
                    case 
                    when recomgame is not null or (recomgamecode is not null and recomgamecode!='') then  3
                    else 1
                    end as recom_game_relation,
                    dt,
                    value
                    from 
                    ods.gscoupondb_consumecash
                    where dt='%s' and date >= '%s' and date < '%s' and optypeid=12 
                    union all
                    select
                    recomgame as game_id,
                    recomgamecode as game_code, 
                    date, 
                    case when ostype = 3 then 400 else pkgtype end as package_type,
                    fromapp as from_app_id, 
                    fromappcode as from_app_code, 
                    ostype as os_type, 
                    game as recom_game_id, 
                    gamecode as recom_game_code,
                    2 as recom_game_relation,
                    dt,
                    value
                    from 
                    ods.gscoupondb_consumecash
                    where dt='%s' and date >= '%s' and date < '%s' and optypeid=12 and (recomgame is not null or (recomgamecode is not null and recomgamecode!=''))
                    ) t1
                    group by t1.game_id,t1.game_code,t1.date,t1.package_type,t1.from_app_id,t1.from_app_code,t1.os_type,t1.recom_game_id,t1.recom_game_code,t1.recom_game_relation
                    ) a
                    left join 
                    (
                    select * from dwd.dim_common_enum_dict 
                    where enum_type = 'pkgtype'  
                    ) b
                    on a.package_type=b.enum_key''' %(start_date[0:6],start_date,end_date,start_date[0:6],start_date,end_date)

    logger.warn(execute_sql_level_2,'sql')

    df_level_2 = spark.sql(execute_sql_level_2)

    df_level_2.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.revenue_spend_coupon_cash_daily_agg_level_2")

    '''
    生成优惠券level1表
    '''

    execute_sql_level_1 = '''select 
                game_id,
                game_code,
                date,
                package_type_id,
                package_type_name,
                from_app_id,
                from_app_code,
                os_type,
                recom_game_relation,
                sum(amount) as amount,
                dt
                from bi.revenue_spend_coupon_cash_daily_agg_level_2
                where dt >= '%s' and dt < '%s'
                group by game_id,game_code,date,package_type_id,package_type_name,from_app_id,from_app_code,os_type,recom_game_relation,dt'''%(start_date,end_date)

    logger.warn(execute_sql_level_1,'sql')

    df_level_1 = spark.sql(execute_sql_level_1)

    df_level_1.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.revenue_spend_coupon_cash_daily_agg_level_1")

    '''
    插入到MongoDB
    '''

    mongo = mongoExecute()
    mongo.collectionAppend(spark.sql('''select 
                                        game_id as gameId, 
                                        game_code as gameCode, 
                                        date , 
                                        package_type_id as packageTypeId, 
                                        from_app_id as fromAppId, 
                                        from_app_code as fromAppCode, 
                                        os_type as osType, 
                                        recom_game_relation as recommendRelation, 
                                        amount as value ,
                                        dt
                                        from
                                        bi.revenue_spend_coupon_cash_daily_agg_level_1
                                        where dt >= '%s' and dt < '%s' '''%(start_date,end_date)) ,"GameProfitDB","coupon.brief", start_date, end_date)
    mongo.collectionAppend(spark.sql('''select 
                                        date , 
                                        game_id as gameId, 
                                        game_code as gameCode, 
                                        recom_game_id as relateGameId,
                                       recom_game_code as relateGameCode,
                                        package_type_id as packageTypeId, 
                                        from_app_id as fromAppId, 
                                        from_app_code as fromAppCode, 
                                        os_type as osType, 
                                        recom_game_relation as recommendRelation, 
                                        amount as value ,
                                        dt
                                        from
                                        bi. revenue_spend_coupon_cash_daily_agg_level_2
                                        where dt >= '%s' and dt < '%s' '''%(start_date,end_date)),"GameProfitDB","coupon.detail", start_date, end_date)


if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None or argv["start_date"] == "" or argv["end_date"] == "":
        logic()
    else:
        logic(argv["start_date"],argv["end_date"])
