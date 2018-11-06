'''
if条件判断
知识点：
    1.if...else..

'''
'''
如果 条件成立:
    做某事
否则：
    做其他事
if sth == True:
    do sth
else:
    do other
'''

age_in = input("请输入年龄：")
print(age_in)

# age_in = input("请输入年龄:")
# age_int = int(age_in)
# if age_int < 18:
#     print("温馨提示:")
#     print("不向未成年人销售烟酒，请买块糖把")
# # else:
# #     print("请付款")
#
# print("欢迎下次光临")

#多条件判断
# age = 12
#
# if age < 18:
#     print("未成年人")
#
# elif age == 18:
#     print("成年人")
# else:
#     print("--------")
# print("********")

#if嵌套
fee = 50
gender = "b"
if fee == 50:
    print("缴费成功")
    if gender == "b":
        print("请排在男生队伍")
    elif gender == "a":
        print("请排在女生队伍")
    else:
        print("请出去")
else:
    print("谢谢")



