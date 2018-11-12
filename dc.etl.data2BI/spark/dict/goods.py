#coding=utf-8

import requests,json
from pandas.io.json import json_normalize

FILE_PATH = 'data/goods.csv'

f_init = open(FILE_PATH,'w')
f_init.close()

s = requests.Session()

url = "http://gameshopsvc.tcy365.org:1505/api/Goods/GetPageGoodsList?PageSize=100"

def getGoods(page):
    data = s.get(url+"&PageIndex=%d" % (page))
    return json.loads(data.text)

total_page_num = getGoods(1)["Data"]["TotalPages"]

f = open(FILE_PATH,'a',encoding='utf8')

for i in range(1,total_page_num+1):
    print(i)

    data = getGoods(i)["Data"]["PageData"]

    for line in data:
        f.write("%s,%s,%s,%s,%s\n" % (line["GoodsId"], line["GoodsDisplayName"], line["GoodsType"], line["GoodsClass"], line["GoodsStatus"]))

f.close()

'''
SQL create table
'''
#
# create external table dwd.dim_goods_dict(
# goods_id int,
# goods_name string,
# goods_type int,
# goods_class int,
# goods_status int)
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY ','
# STORED AS TEXTFILE
# location '/hive/data/dictionary/goods';