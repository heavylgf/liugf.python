'''
元组
'''
#定义元组
db_info = ("192.168.10.1", 3306, "root", "root123")
# ip = db_info[0]
# port = db_info[1]
# print("ip={}, port={}".format(ip,port))
# db_info[1] = 3308  #不支持修改
# del db_info[1] #不支持删除

#定义只有一个元素的元组
# one_tuple = ("zhangsan",)
# print(one_tuple)
# none_tuple = () #空元组
# print(none_tuple)

#循环遍历元组
# for item in db_info:
#     print(item)

for item in db_info:
    print(item)


