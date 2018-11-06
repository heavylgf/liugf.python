'''
面向对象
'''
#定义类
class Dog:
	# 定义方法默认要传入一个参数self
	def eat(self):
		print("小狗正在啃骨头...")
	def drink(self):
		print("小狗正在喝水...")
# 创建对象: 对象变量名 = 类名()
wang_cai = Dog()
print(id(wang_cai))   # 通过id函数获取对象的内存地址
wang_cai.eat()        
wang_cai.drink()


a_fu = Dog()
print(id(a_fu))
a_fu.eat()
a_fu.drink()




