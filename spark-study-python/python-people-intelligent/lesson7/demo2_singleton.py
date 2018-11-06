'''
单例类
'''
class DataBaseObj(object):
	def __init__(self, new_name): # 对象初始化
		print("---init构造方法----")
		self.name = new_name
		print(self.name)

	def __new__(cls, name): # 这个方法用于创建对象
		print("cls_id:",id(cls))
		return object.__new__(cls) # 必须有返回值，返回的是创建的对象的引用

# print(id(DataBaseObj))
# db = DataBaseObj("mysql")
# print(db)

# 单例类
# 比如创建数据库连接
class SingleInstance:
	# 定义私有的类属性
	__instance = None
	def __init__(self):
		print("-----init-----")
	def __new__(cls):
		if cls.__instance == None:
			cls.__instance = object.__new__(cls)
		return cls.__instance

s1 = SingleInstance()
print(id(s1))
s2 = SingleInstance()
print(id(s2))



