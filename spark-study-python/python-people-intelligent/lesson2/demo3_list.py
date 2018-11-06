'''
列表
'''
# name_list = ["zhangsan","lisi","wangwu"]
# print(name_list[0])
# print(name_list[1])
# name_list[0] = "xiaobai"  # 通过下标修改值
# print(name_list[0])

# i = 0
# info_list = ["zhangsan",20,180.5]
# print(info_list[2])
#while遍历
# while i < len(info_list):
#     print(info_list[i])
#     i += 1

#for遍历
# for i in range(0,len(info_list)):
#     print(info_list[i])

#简便方法
# for item in info_list:
#     print(item)

#嵌套列表
# infos_list = [["zhangsan",20,180.5],["lisi",21,170],["wangwu",25,190]]
# print(infos_list[2])
# print(infos_list[0][0])
# print(infos_list[1][0])
# for lst in infos_list:
#     print(lst)
#     for item in lst:
#         print(item)

#append 列表末尾添加元素
# infos_list = [["zhangsan",20,180.5],["lisi",21,170],["wangwu",25,190]]
# infos_list.append(["xiaobai",30,175])
# print(infos_list)

#insert向列表指定位置添加元素
# new_info = ["孙悟空",18,160]
# new_info.insert(1,50)
# print(new_info)   # ['孙悟空', 50, 18, 160]

#+拼接两个列表
# name_list1 = ["zhangsan"]
# name_list2 = ["lisi","wangwu"]
# name_list3 = name_list1 + name_list2
# print(name_list3)   # ['zhangsan', 'lisi', 'wangwu']

#extend 向一个列表中添加另外一个列表的元素
# name_list1.extend(name_list2)
# print(name_list1)     # ['zhangsan', 'lisi', 'wangwu']
# name_list1.append(name_list2)
# print(name_list1)     # ['zhangsan', 'lisi', 'wangwu', ['lisi', 'wangwu']]

#删除列表元素
group = ["唐僧","悟空","八戒","唐僧"]
# del group[1]
# group.remove("悟空")  # 根据值来删除
# group.pop()
# print(group)

# if "唐僧" in group:
#     print("师傅还在")
# else:
#     print("师傅没了")

#sort()
num_list = [5,2,6,1]
num_list.sort()   # 默认升序
num_list.sort(reverse=True)
print(num_list)

#reverse()将列表内容 倒置
# group.reverse()
# print(group)
#count(元素)
# print(group.count("唐僧"))



