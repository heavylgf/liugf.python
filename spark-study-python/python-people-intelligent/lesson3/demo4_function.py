'''
函数
'''
# 函数的定义  无参数的函数
# def print_user_info():
#     print("name:zhangsan")
#     print("age:20")
#     print("gender:male")
# print_user_info()   # 函数调用

#带参函数注意，1.调用函数时传入的参数个数与函数定义参数个数相同。2.参数顺序要相同
# def print_user_info2(name, age, gender):
#     print("name:%s"%name)
#     print("age:%d"%age)
#     print("gender:%s"%gender)
# print_user_info2("悟空",20,"male")
# print_user_info2("八戒",18,"male")
# print_user_info()

# 有返回值函数
# def x_y_sum_return(x, y):
#     res = x + y
#     return res
# z = x_y_sum_return(2, 3)
# print(z)

# def x_y_comp_list(x, y):
#     res1 = x + y
#     res2 = x * y
#     res_list = [res1, res2]
#     return res_list
# print(x_y_comp_list(2,3))

def x_y_comp_tuple(x,y):
    res1 = x + y
    res2 = x * y
    res_tuple = (res1,res2)
    return res_tuple
#和上边的方法等价
def x_y_comp_tuple2(x,y):
    res1 = x + y
    res2 = x * y
    return res1, res2
print(x_y_comp_tuple2(2,3))
a, b = x_y_comp_tuple2(2,3)
print(a)
print(b)


