'''
异常
'''
# print(num)
# open("test.txt","r")

'''
try:
	# print(num)
	# print("==============")
	open("test.txt", "r")
except NameError as err1:
	print("捕获到了异常!", err1)
except Exception as err2:
	print("捕获所有可能的异常", err2)

print("哈哈哈哈哈")
'''

# finally的使用
'''
f = None
try:
	f = open("test.txt","r")
	print("打印文件内容")
except FileNotFoundError as error:
	print("捕获到了异常",error)
finally:
	print("关闭文件")
	if f != None:
		print("正在关闭文件")
		f.close()
'''

# 函数嵌套，异常传递
# '''
def test1():
	print("------test1-1--------")
	print(num) # 打印一个不存在的变量
	print("------test1-2--------")

def test2():
	try:
		print("------test2-1--------")
		test1()
		print("------test2-2--------")
	except Exception as error:
		print("捕获到异常",error)

	print("------test2-3--------")

test2()
# '''




