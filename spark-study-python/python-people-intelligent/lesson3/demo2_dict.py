'''
字典
'''
#列表
user_info_list = ["悟空", 100, "male", "取经"]
# user_info_list = ["", "", ""]

#字典
# user_info_dict = {"":""}
user_info_dict = {"name":"悟空","age":100,"gender":"male","job":"取经"}
# 通过键值来修改值
# user_info_dict["job"] = "取经|偷桃"
# print(user_info_dict)
# print("%s的年龄是：%d，性别：%s，工作内容：%s"%(user_info_dict["name"], user_info_dict["age"], user_info_dict["gender"], user_info_dict["job"]))

#key不能相同
user_info_dict = {"name":"悟空","age":100,"gender":"male","job":"取经","job":"偷桃"}  # 后定义的会覆盖前面定义的
print(user_info_dict)

#添加键值对
user_info_dict["tel"] = "13812345567"
# print(user_info_dict)
# print(len(user_info_dict))

#修改
# user_info_dict["tel"]  = "138888888888"
# print(user_info_dict)

#删除
# s
# print(user_info_dict)
# del user_info_dict["tel"]
# print(user_info_dict["tel"])

#查找一个不存在的key
# if "tel" in user_info_dict:
#     print(user_info_dict["tel"])
# else:
#     print("tel不存在")

#字典 get使用，可以有默认值
# print(user_info_dict.get("name"))
# print(user_info_dict.get("tel","10010"))

#keys获取字典中所有的key
# for key in user_info_dict.keys():
#     print("{}:{}".format(key, user_info_dict[key]))

#values获取字典中所有的values
# for value in user_info_dict.values():
#     print(value)

# items方法使用
# for item in user_info_dict.items():
    # print(type(item))
    # print(item)
#     print(item[0])
#     print(item[1])
for key, value in user_info_dict.items():
    # print("{}:".format(key))
    print(key)
    print(value)

# clear()清空字典
# user_info_dict.clear()
# print(user_info_dict)





