'''
input
print
知识点：
    1.通过input获取用户输入内容
    2.print直接输出字符串内容
'''

# card_id = input("请输入卡号:")
# pwd = input("请输入密码:")

# print(card_id)
# print(type(card_id))
# print(pwd)


#1.print打印字符串
# print("hello python")

#2.print打印变量值
# name = "zhangsan"
# print(name)

#3.print格式化输出
#使用格式化占位符格式化输出
# print("您输入的卡号是：%s"%card_id)
# print("您输入的密码是：%s"%pwd)

#多个变量同时输出
# card_id = "234567"
# pwd = 123
# print("您输入的卡号是：%s，您输入的密码是：%d"%(card_id,pwd))

#格式化输出浮点数,并指定精度
# height = 180.35
# print("您的身高是：%.2f"%height)

#格式化输出时，打印%，要使用%%表示是字符串而不是转换说明符
# p = 99.99
# print("您战胜了全国%.2f%%的用户"%p)

#print无换行输出
# print("hello",end="")
# print("python")

#输出换行符
# print("中国\n北京")

#转义字符\
# print("中国\\n北京")

#format函数
# card_id = "234567"
# pwd = 123
# print("您输入的卡号是：{}，您输入的密码是：{}".format(card_id,pwd))

# height = 180.35
# print("您的身高是：{:.2f}".format(height))


#类型转换
# name = input("请输入姓名：")
# age = input("请输入年龄")
# print("name：%s,age:%d"%(name,int(age)))

# print(int("123"))

# a = int("123") #字符串转整型
# b = float("3.14")#字符串转浮点型
# c = str(345)
# print(type(a))
# print(type(b))
# print(type(c))

# b = int("3.14")
# print(b)  # 这样输出会报错

#eval(str)把字符串自动转换成合适的数据类型
a1= eval("123")
a2 = eval("3.14")
print(a1)
print(a2)
print(type(a1))
print(type(a2))





