'''
文件操作
'''
# 引入 os 模块
import  os
import shutil
#文件不存在报错
# f = open("test.txt","r")
# f.close() #切结要关闭

#文件不存在则创建
'''
f = open("test.txt","w",encoding="utf-8") # 覆盖已存在的内容,encoding="utf-8"解决中文乱码
f.write("你好")
f.close()
'''
'''
f = open("d://test.txt","w",encoding="utf-8")
f.write("你好 python")
f.close()
'''

#read读文件
# f = open("test.txt","r",encoding="utf-8")
# print(f.read())
# f.close()

# 追加数据
# f = open("test.txt","a",encoding="utf-8") #覆盖已存在的内容,encoding="utf-8"解决中文乱码
# f.write("\n大家好")
# f.close()

#readlines按行全部读取文件数据，返回一个文件数据列表，每一行是列表的一个元素
'''
f = open("test.txt","r",encoding="utf-8")
data = f.readlines()
print(data)
print("-----------")
i = 1
for line in data:
	print("第{}行：{}".format(i,line),end="")
	i += 1
f.close()
'''

# readline：读取文件高效，光标会自动下移
'''
f = open("test.txt","r",encoding="utf-8")
line1 = f.readline()
print(line1,end="")
line2 = f.readline()
print(line2,end="")
line3 = f.readline()
print(line3,end="")
f.close()
'''

# writelines向文件写入一个字符串序列
'''
f = open("test.txt","w",encoding="utf-8")
f.writelines(["张三\n","李四\n","王五\n"])  # 加入 \n可以换行
f.close()
'''

# os.rename(oldname, newname) # 文件重命名
# os.remove(filepath)  # 删除文件

# os.mkdir("d://testdir123")  创建文件夹
# print(os.getcwd())  获取当前目录
#print(os.listdir("d://"))   查看指定目录下的文件列表
#os.rmdir("d://testdir123") 删除指定路径的空文件夹

# shutil.rmtree("d://testdir123")  删除非空文件

path = os.getcwd() # 程序运行的当前路径
print(path)
os.chdir("../")  # 切换到上一级目录
path = os.getcwd()
print(path)
os.chdir("d://") # 切换到上一级目录
path = os.getcwd()
print(path)



