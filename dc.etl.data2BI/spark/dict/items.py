#coding=utf-8

import requests,json
from pandas.io.json import json_normalize
import pandas as pd


ITEMS_PATH = 'data/items.csv'
ITEMS_TYPE_PATH = 'data/items_type.csv'
PAGE_SIZE = 50
ITEMS_URL = "http://itemsysapi.uc108.org:1505/api/Item/GetItemPageList?pageSize=%d" %(PAGE_SIZE)
ITEMS_TYPE_URL ="http://itemsysapi.uc108.org:1505/api/Item/GetItemTypeList"

items_f_init = open(ITEMS_PATH,'w')
items_f_init.close()

items_type_f_init = open(ITEMS_TYPE_PATH,'w')
items_type_f_init.close()

s = requests.Session()


def getItems():
    total_count = json.loads(s.get(ITEMS_URL+"&PageIndex=%d" % (1)).text)["Data"]["PageInfo"]["TotalCount"]
    i = 1
    return_data = pd.DataFrame()


    while(total_count > 0):
        data = json.loads(s.get(ITEMS_URL+"&PageIndex=%d" % (i)).text)["Data"]["ItemPageList"]

        df = json_normalize(data)

        final_data = df[["ItemID","ItemName","ItemCode","ItemTypeID","ExpiryDays","ExpiryTimeStamp","ItemValue","PlatformID", "IsStop","AppID", "IsCommon"]].replace([True, False], [1, 0])\

        final_data.to_csv(ITEMS_PATH, sep=',',mode="a",encoding='utf-8',index=False,header=False)

        print(i)
        total_count -= PAGE_SIZE
        i += 1

        return_data = pd.concat([return_data,final_data])
    return return_data

def getItemsType():
    data = json.loads(s.get(ITEMS_TYPE_URL).text)["Data"]
    return json_normalize(data)[["ItemTypeID","TypeName","TypeLevel","ParentTypeID"]].to_csv(ITEMS_TYPE_PATH, sep=',',mode="a",encoding='utf-8',index=False,header=False)




getItems()
getItemsType()

#
# items_type_list_level_three_rename = items_type_list.rename(columns={"TypeName":"TypeNameLevelThree","TypeName":"TypeNameLevelThree","ParentTypeID":"ParentTypeIDLevelTwo"})
# items_type_list_level_three = items_type_list_level_three_rename.loc[items_type_list_level_three_rename["TypeLevel"] == 3]
#
# items_type_list_level_two_rename = items_type_list.rename(columns={"TypeName":"TypeNameLevelTwo","TypeName":"TypeNameLevelTwo","ParentTypeID":"ParentTypeIDLevelOne"})
# items_type_list_level_two = items_type_list_level_two_rename.loc[items_type_list_level_two_rename["TypeLevel"] == 2]
#
# items_type_list_level_one_rename = items_type_list.rename(columns={"TypeName":"TypeNameLevelOne","TypeName":"TypeNameLevelOne","ParentTypeID":"ParentTypeIDLevelNone"})
# items_type_list_level_one = items_type_list_level_one_rename.loc[items_type_list_level_one_rename["TypeLevel"] == 1]
#
#
# items_list_level_three =  pd.merge(items_list, items_type_list_level_three, how='left', on=['ItemTypeID'])
# items_list_level_two_1 =  pd.merge(items_list_level_three, items_type_list_level_two, how='left', on=['ItemTypeID'])
# items_list_level_two_2 =  pd.merge(items_list_level_two_1, items_type_list_level_two, how='left', on=['ItemTypeID','ParentTypeIDLevelOne'])
#
# print(items_list_level_two_1)
#
# items_list_level_two_1.to_csv(FILE_PATH, sep=',',mode="w",encoding='utf-8',index=False)
# #
# # print(items_level_three_list.count())
# # print(items_level_three_list)




'''
sql
'''

# create external table stag.dim_items_type_dict(
# item_type_id int,
# item_type_name string,
# item_type_level int,
# parent_type_id int)
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY ','
# STORED AS TEXTFILE
# location '/hive/data/dictionary/itemstype';
#
#
# select * from stag.dim_items_type_dict
#
#
# create table dwd.dim_items_dict(
# item_id int,
# item_name string,
# item_code string,
# item_type_id int,
# expiry_days int,
# expiry_timestamp int,
# item_vale decimal(18,2),
# platform_id int,
# is_stop int,
# app_id int,
# is_common int,
# item_type_id_level_3 int,
# item_type_name_level_3 string,
# item_type_id_level_2 int,
# item_type_name_level_2 string,
# item_type_id_level_1 int,
# item_type_name_level_1 string)
# ROW FORMAT DELIMITED
# STORED AS orc
#
#
#
# insert overwrite table dwd.dim_items_dict
# select
# a.item_id,
# a.item_name,
# a.item_code,
# a.item_type_id,
# a.expiry_days,
# a.expiry_timestamp,
# a.item_vale,
# a.platform_id,
# a.is_stop,
# a.app_id,
# a.is_common,
# level_3.item_type_id as item_type_id_level_3,
# level_3.item_type_name as item_type_name_level_3,
# case when level_2.item_type_id is null then level_2_p.item_type_id else level_2.item_type_id end as item_type_id_level_2,
# case when level_2.item_type_name is null then level_2_p.item_type_name else level_2.item_type_name end as item_type_name_level_2,
# case when level_1.item_type_id is null then level_1_p.item_type_id else level_1.item_type_id end as item_type_id_level_1,
# case when level_1.item_type_name is null then level_1_p.item_type_name else level_1.item_type_name end as item_type_name_level_1
# from
# stag.dim_items_dict a
# left join stag.dim_items_type_dict level_3 on a.item_type_id = level_3.item_type_id and level_3.item_type_level = 3
# left join stag.dim_items_type_dict level_2 on a.item_type_id = level_2.item_type_id and level_2.item_type_level = 2
# left join stag.dim_items_type_dict level_2_p on level_3.parent_type_id = level_2_p.item_type_id and level_2_p.item_type_level = 2
# left join stag.dim_items_type_dict level_1 on a.item_type_id = level_1.item_type_id and level_1.item_type_level = 1
# left join stag.dim_items_type_dict level_1_p on case when level_2.parent_type_id is null then level_2_p.parent_type_id else level_2.parent_type_id end = level_1_p.item_type_id and level_1_p.item_type_level = 1
#
#
# select * from dwd.dim_items_dict
