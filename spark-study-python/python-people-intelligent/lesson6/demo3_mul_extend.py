'''
多继承
'''
class AI:
	#人脸识别
	def face_recongnition(self):
		print("人脸识别")
	def data_handle(self):
		print("AI数据处理")

class BigData:
	def data_analysis(self):
		print("数据分析")
	def data_handle(self):
		print("BigData数据处理")

# 继承多个类, 用逗号隔开
class Python(BigData, AI):
	def operation(self):
		print("自动化运维")

# 创建一个对象
py = Python()
# py.face_recongnition()
# py.data_analysis()
# py.operation()

py.data_handle()
# mro()可以搜索需要调用的方法
print(Python.__mro__)





