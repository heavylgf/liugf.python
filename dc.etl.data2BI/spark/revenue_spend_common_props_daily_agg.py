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
sparkSession = sparkInitialize().setAppName("revenue_spend_common_props_daily_agg").onHive().onMongo()

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
    大致步骤  删除分区 -》读取转换数据 -》 写入hive  -》 写入mongo
    '''

    '''
    添加无推荐关系与被推荐关系数据
    '''

    hive_partition = partition(spark,logger)

    hive_partition.dropPartition("bi.revenue_spend_common_props_daily_agg_level_2","dt",start_date,end_date)

    execute_sql_level_2_step_1 = '''
                                    select
                                    /*+ MAPJOIN(smalltable)*/
                                    case when game_id is null then -1 else game_id end as game_id,
                                    case when game_code is null then '' else game_code end as game_code,
                                    `date` ,
                                    case when item_id is null then -1 else item_id end as item_id,
                                    case when item_name is null then '' else item_name end as item_name,
                                    case when package_type is null then -1 else package_type end as package_type,
                                    case when enum_pkg.enum_value is null then '' else enum_pkg.enum_value end as package_type_name,
                                    case when from_app_id is null then -1 else from_app_id end as from_app_id,
                                    case when from_app_code is null then '' else from_app_code end as from_app_code,
                                    case when os_type is null then -1 else os_type end as os_type,
                                    recom_game_id,
                                    recom_game_code,
                                    case when recom_game_id is null or recom_game_id = '' then 1 else 3 end as recom_game_relation,
                                    item_amount,
                                    dt
                                    from (
                                        select
                                        game_id,
                                        game_code,
                                        `date`,
                                        item_id,
                                        item_name,
                                        package_type,
                                        from_app_id,
                                        from_app_code,
                                        os_type,
                                        recom_game_id,
                                        recom_game_code,
                                        sum(item_amount) as item_amount,
                                        max(dt) as dt
                                        from (
                                            select  
                                            reward.game as game_id,
                                            reward.gamecode as game_code,
                                            reward.`date`,
                                            reward.item as item_id,
                                            dict.item_name,
                                            case when reward.ostype = 3 then 400 else reward.pkgtype end as package_type,
                                            reward.fromapp as from_app_id,
                                            reward.fromappcode as from_app_code,
                                            reward.ostype as os_type,
                                            reward.recomgame as recom_game_id,
                                            reward.recomgamecode as recom_game_code,
                                            cast(reward.itemnum * dict.item_vale * 10000 as bigint) as item_amount,
                                            reward.dt
                                            from ods.gsrewardsystemdb_reward reward,dwd.dim_items_dict dict 
                                            where reward.item = dict.item_id 
                                            and dict.is_common = 1
                                            and reward.dt >= %(sdate)s and reward.dt < %(edate)s 
                                        ) T1
                                        group by 
                                        game_id,
                                        game_code,
                                        `date`,
                                        item_id,
                                        item_name,
                                        package_type,
                                        from_app_id,
                                        from_app_code,
                                        os_type,
                                        recom_game_id,
                                        recom_game_code
                                    ) T2
                                    left join dwd.dim_common_enum_dict enum_pkg on T2.package_type = enum_pkg.enum_key and enum_pkg.enum_type = 'pkgtype'
    ''' % {"sdate":start_date,"edate":end_date}

    logger.warn(execute_sql_level_2_step_1,'sql')

    df_level_2_step_1 = spark.sql(execute_sql_level_2_step_1)

    df_level_2_step_1.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.revenue_spend_common_props_daily_agg_level_2")

    '''
    从被推荐关系生成主推荐关系
    '''


    execute_sql_level_2_step_2 = '''
                                    select
                                    recom_game_id as game_id,
                                    recom_game_code as game_code,
                                    `date`,
                                    item_id,
                                    item_name,
                                    package_type,
                                    package_type_name,
                                    from_app_id,
                                    from_app_code,
                                    os_type,
                                    game_id as recom_game_id,
                                    game_code as recom_game_code,
                                    2 as recom_game_relation,
                                    item_amount,
                                    dt
                                    from bi.revenue_spend_common_props_daily_agg_level_2
                                    where recom_game_relation = 3
                                    and dt >= ''' + start_date + ''' and dt < ''' + end_date

    logger.warn(execute_sql_level_2_step_2, 'sql')

    df_level_2_step_2 = spark.sql(execute_sql_level_2_step_2)

    df_level_2_step_2.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.revenue_spend_common_props_daily_agg_level_2")

    '''
    从二级表去除recommand生成一级表
    '''

    hive_partition.dropPartition("bi.revenue_spend_common_props_daily_agg_level_1", "dt", start_date, end_date)

    execute_sql_level_1_step_1 = '''
                                        select
                                        game_id,
                                        game_code,
                                        `date`,
                                        item_id,
                                        item_name,
                                        package_type,
                                        package_type_name,
                                        from_app_id,
                                        from_app_code,
                                        os_type,
                                        recom_game_relation,
                                        sum(item_amount) as item_amount,
                                        dt
                                        from bi.revenue_spend_common_props_daily_agg_level_2
                                        where dt >= ''' + start_date + ''' and dt < ''' + end_date + '''
                                        group by
                                        game_id,
                                        game_code,
                                        `date`,
                                        item_id,
                                        item_name,
                                        package_type,
                                        package_type_name,
                                        from_app_id,
                                        from_app_code,
                                        os_type,
                                        recom_game_relation,
                                        dt
        '''

    logger.warn(execute_sql_level_1_step_1, 'sql')

    df_level_1_step_1 = spark.sql(execute_sql_level_1_step_1)

    df_level_1_step_1.write.partitionBy("dt").mode('append').format("orc").saveAsTable("bi.revenue_spend_common_props_daily_agg_level_1")

    '''
    将生成的数据增量插入至mongo中
    '''

    mongo = mongoExecute()

    out_put_level_2_sql = '''
                                                select
                                                game_id as gameId,
                                                game_code as gameCode,
                                                `date`,
                                                item_id as itemId,
                                                item_name as itemName,
                                                package_type as packageTypeId,
                                                package_type_name as packageTypeName,
                                                from_app_id as fromAppId,
                                                case when from_app_code is null then '' else from_app_code end as fromAppCode,
                                                os_type as osType,
                                                recom_game_id as relateGameId,
                                                recom_game_code as relateGameCode,
                                                recom_game_relation as recommendRelation, 
                                                item_amount as silvers,
                                                dt
                                                from 
                                                bi.revenue_spend_common_props_daily_agg_level_2
                                                where dt >= ''' + start_date + ''' and dt < ''' + end_date

    level_2 = spark.sql(out_put_level_2_sql)

    mongo.collectionAppend(level_2, "GameProfitDB", "common_prop_cost.detail", start_date, end_date)

    out_put_level_1_sql = '''
                                                select
                                                game_id as gameId,
                                                game_code as gameCode,
                                                `date`,
                                                item_id as itemId,
                                                item_name as itemName,
                                                package_type as packageTypeId,
                                                package_type_name as packageTypeName,
                                                from_app_id as fromAppId,
                                                case when from_app_code is null then '' else from_app_code end as fromAppCode,
                                                os_type as osType,
                                                recom_game_relation as recommendRelation, 
                                                item_amount as silvers,
                                                dt
                                                from 
                                                bi.revenue_spend_common_props_daily_agg_level_1
                                                where dt >= ''' + start_date + ''' and dt < ''' + end_date

    level_1 = spark.sql(out_put_level_1_sql)

    mongo.collectionAppend(level_1, "GameProfitDB", "common_prop_cost.brief", start_date, end_date)


if __name__ == "__main__":
    argv = arguments(sys.argv)
    if argv["start_date"] is None or argv["end_date"] is None or argv["start_date"] == "" or argv["end_date"] == "":
        logic()
    else:
        logic(argv["start_date"],argv["end_date"])
