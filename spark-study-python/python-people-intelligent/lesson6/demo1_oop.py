'''
构造方法
私有变量
私有方法
'''

class Dog:
	def __init__(self,gender,variety,name,age):
		# print("我是构造方法，在创建对象时自动调用")
		self.gender = gender
		self.variety = variety
		self.name = name
		self.__age = age
	# 获取对象属性，并打印出来
	def get_pro(self):
		print("gender:{},variety:{},name:{},age:{}".format(self.gender,self.variety,self.name,self.__age))
	#设置对象内部属性
	def set_pro(self,**kwargs):
		if "gender" in kwargs:
			self.gender = kwargs["gender"]
		elif "age" in kwargs:
			if kwargs["age"] < 0 or kwargs["age"]>20:
				print("非法年龄")
			else:
				self.__age = kwargs["age"]

	def eat(self):
		print("正在吃骨头...")

	def drink(self):
		print("正在喝水....")

# wangcai = Dog("male","golden","wangcai",1)
# wangcai.eat()
# wangcai.drink()
# wangcai.get_pro()
#修改对象属性，方法1直接修改
# wangcai.age = 100
#方法2：通过内部方法修改属性
# wangcai.set_pro(age=10)
# wangcai.get_pro()

# yuanbao = Dog("female","husky","yuanbao")
# yuanbao.get_pro()


# 私有方法的使用
class Comrade:
	#私有方法,方法前面使用两个下划线
	def __send_message(self):
		print("消息已经向上级汇报")

	def answer_secret(self,secret):
		if secret == "芝麻开门":
			print("接头成功!")
			self.__send_message() # 调用私有方法
		else:
			print("接头失败！")
# 类的外部是不能调用私有方法的
comrade = Comrade()
comrade.answer_secret("芝麻开门")




