'''
继承：
	单继承
'''

class Animal:
	def __init__(self):
		print("---animal构造方法----")

	def __private_method(self):
		print("私有方法")
	def eat(self):
		print("----吃----")
	def drink(self):
		print("----喝----")
	def run(self):
		print("----跑----")

class Dog(Animal):
	def __init__(self):
		print("dog构造方法")
	# 父类方法重写
	def run(self):
		print("摇着尾巴跑")
	def hand(self):
		# 在自类中调用父类的方法
		Animal.run(self)
		print("------握手-----")

class GoldenDog(Dog):
	def guid(self):
		print("我能导航！")

# wangcai = Dog()
# 调用从父类继承的非私有方法
# wangcai.eat()
# wangcai.drink()
# wangcai.run()

#父类的私有方法不能够被子类继承和调用
# wangcai.__private_method

# 调用自身的方法
# wangcai.hand()
# golden = GoldenDog()
# golden.hand()
# golden.run()

#如果在子类中没有定义init构造方法，则自动调用父类的init构造方法，如果在子类中定义了init构造方法，则不会调用父类的构造方法
duoduo = Dog()
duoduo.run()


