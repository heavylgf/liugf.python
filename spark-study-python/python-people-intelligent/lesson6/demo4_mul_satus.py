'''
多态
'''

class Animal:
	def eat(self):
		print("Animal正在吃饭")

class Dog(Animal):
	def eat(self):
		print("Dog正在吃饭")

class Cat(Animal):
	def eat(self):
		print("Cat正在吃饭")

def show_eat(obj):
	obj.eat()

wangcai = Dog()
show_eat(wangcai)
tom = Cat()
show_eat(tom)



