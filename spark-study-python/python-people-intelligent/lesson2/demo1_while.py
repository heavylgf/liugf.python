'''
while循环
'''

# num = 1
# print(num)
# num += 1
# print(num)

# num = 1
# while num <= 10:
#     print(num)
#     num += 1

# 第一步打印倒三角形
# 1）行号和空格数相等
# 2）*号个数=7-行号*2

# i = 0
# while i < 4:
#     print(" " * i, end = "")
#     j = 0
#     while j < 7 - i * 2:
#         print("*" , end = "")
#         j += 1
#     print("")
#     i += 1
# print(i)

'''
i = 0 #第几行，行号
while i < 4:
    print(" " * i,end="") #打印每行开头的空格
    j = 0 #控制*个数
    while j < 7-i*2:
        print("*",end="")
        j += 1
    print("")#实现每一行打印完换行
    i += 1
    '''
# print(i)

#打印正三角形

# while i > 0:
#     i -= 1
#     print(" " * i, end = "")
#     j = 0
#     while j < 7 - i * 2:
#         print("*", end = "")
#         j += 1
#     print("")

'''
while i > 0:
    i -= 1
    print(" "*i,end="")#打印开头空格
    j = 0
    while j < 7-i*2:
        print("*",end="")
        j +=1
    print("")
'''

#合并倒三角形和正三角形
'''
m = 7     # 控制总行数
i = 0
h = 0     # 空格的个数
middle = m // 2 #中间位置
while i<m:
    if i <= middle:
        h = i
    else:
        h -= 1
    n = m-h*2
    #打印一行开头的空格和星号
    print(" " * h,end="")   # 打印开头空格
    j = 0
    while j < n:
        print("*",end="")
        j +=1
    print("")
    i += 1
'''

#break跳出整个循环
# i = 1
# while i <= 20:
#     if i % 2 == 0:
#         if i % 10 == 0:
#             break
#         print(i)
#     i += 1
# print(">>>>>>>>>>>>>>>>")

#continue 跳出本次循环
i = 1
while i <= 20:
    i += 1
    if i % 2 == 0:
        if i % 10 == 0:
            continue
        print(i)
print(">>>>>>>>>>>>>>>>")


# continue 以下这里造成一个死循环
# i = 1
# while i <= 20:
#     if i % 2 == 0:
#         if i % 10 == 0:
#             continue
#         print(i)
#     i += 1
# print(">>>>>>>>>>>>>>>>")


