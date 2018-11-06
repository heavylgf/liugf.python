'''
集合set
'''
#定义集合
# student_set = {"zhansan","lisi","wangwu"}
# print(type(student_set))
# print(len(student_set))
# print(student_set)

#集合对列表去重
id_list = ["id1","id2","id3","id1","id2"]
distinct_set = set(id_list)
# print(distinct_set)

# string_set = set("hello")
# print(string_set)  #{'l', 'e', 'h', 'o'}

#创建一个空集合
# none_dict = {} #注意这是创建一个空字典
# none_set = set() #空集合

# in 和 not in 的使用
# user_id = "id5"
# if user_id in distinct_set:
#     print(user_id)
# if user_id not in distinct_set:
#     print("{}不存在".format(user_id))

# add添加元素到集合
name_set = {"zhansan","lisi"}
# name_set.add("wangwu")
# print(name_set)

#update(序列)
# name_set.update(["悟空","八戒"],["张飞","李逵"])
# print(name_set)

#remover(元素)
# name_set.remove("悟空")
# print(name_set)

# 使用remove删除一个不存在的元素，会报错
# name_set.remove("西游记")

# dicard(元素),删除一个不存在的元素，不会报错
# name_set.discard("西游记")

# pop()随机删除集合中的某个元素，并返回被删除的元素
# print(name_set)
# name = name_set.pop()
# print(name_set)
# print(name)

# 交集 intersection 和 & 是等价的
num_set1 = {1,2,4,7}
num_set2 = {2,5,8,9}
# inter_set1 = num_set1 & num_set2
# inter_set2 = num_set1.intersection(num_set2)
# print(inter_set1)
# print(inter_set2)

# 并集 union
# union_set1 = num_set1 | num_set2
# union_set2 = num_set1.union(num_set2)
# print(union_set1)
# print(union_set2)

# 差集 difference
# diff_set1 = num_set1 - num_set2
# diff_set2 = num_set1.difference(num_set2)
# print(diff_set1)
# print(diff_set2)

# 对称差集
# sym_diff_set1 = num_set1 ^ num_set2
sym_diff_set2 = num_set1.symmetric_difference(num_set2)
# print(sym_diff_set1)
print(sym_diff_set2)


