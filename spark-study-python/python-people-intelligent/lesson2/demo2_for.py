'''
for循环
'''
#打印0到9十个数字

# for i in range(0, 10):
#     print(i)

#range(0,10):[0,1,2,3,4,5,6,7,8,9)
# for i in range(0,10):
#     print(i)

#打印0到9的偶数
# for i in range(0,10):
#     if i % 2 == 0:
#         print(i)

# #range(0,10,2)=[0,2,4,6,8)
# for i in range(0,10,2):  # 这里设置了步长是2
#     print(i)

# break跳出for 循环
for i in range(1,5):
    for j in range(0,i):
        if j == 3:
            break
        print("*",end="")
    print("")
    print(i)




