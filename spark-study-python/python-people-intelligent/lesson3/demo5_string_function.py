'''
字符串常用内置方法
'''
# find 
line = "hello world hello python"
# hello第一次出现的脚标
# print(line.find("hello"))
# 指定查找的起始脚标
# print(line.find("hello",6))
# print(line.find("hello", 6, ))
# 不存在的子字符串返回-1
# print(line.find("java"))

# count 统计指定字符串出现的个数
# print(line.count("python"))

# replace 使用新的字符串替换老的字符串，可以指定替换的个数
# new_line = line.replace("hello","hi",1)  # 表示替换一个
# print(new_line)

# split 指定分隔符分割字符串
# split_list = line.split(" ")
# print(line)
# print(split_list)

# split_list = line.split(" ", 1) # 只是对第一个空格进行分割
# print(line)
# print(split_list)

# startswith、endswith 判断字符串是否以指定前缀开头/ 指定后缀结束
# print(line.startswith("hello"))  # True
# print(line.endswith("java"))     # False

# 应用场景
# files = ["20171201.txt","20171201.log","20180101.txt","20180101.log"]
# for file in files:
#     if file.startswith("2018") and file.endswith("log"):
#         print("2018年待处理日志：%s"%file)
#         print("2018年待处理日志：{}".format(file))

# upper和lower 字符串所有字符大写和小写
if_continue = input("是否继续购物，输入yes或者no： ")
if if_continue.lower() == "yes":
    print("继续购物")
else:
    print("拜拜")





