'''
项目实践：计算器
'''

num1 = input("请输入第一个数字：")
operator = input("请输入运算符：")
num2 = input("请输入第二个数字：")

num_1 = int(num1)
num_2 = int(num2)

if operator == "+":
    result = num_1 + num_2
    print("计算结果：{}".format(result))
elif operator == "%":
    result = num_1 % num_2
    print("计算结果：{}".format(result))
elif operator == "**":
    result = num_1 ** num_2
    print("计算结果：{}".format(result))
elif operator == "//":
    result = num_1 // num_2
    print("计算结果：{}".format(result))
else:
    print("正在开发..")






