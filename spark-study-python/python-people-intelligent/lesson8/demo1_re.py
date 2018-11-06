'''
正则表达式
'''
import re

# rs = re.match("chinahadoop","chinahadoop.cn")
# print(rs)
# print(rs.group())   

#.匹配非空字符
# '''
# rs = re.match(".","a")
# print(rs.group())

# rs = re.match(".","1")
# print(rs.group())

# rs = re.match("...","abc")  # 三个. ,代表匹配三个字符
# print(rs.group())

# rs = re.match(".","\n")
# print(rs)
# '''

#\s 匹配任意空白字符
# '''
# rs = re.match("\s","\t")
# print(rs)
# rs = re.match("\s","\n")
# print(rs)
# rs = re.match("\s"," ")
# print(rs)
# '''

#\S 匹配非空白字符
# '''
# rs = re.match("\S","\t")
# print(rs)
# rs = re.match("\S","abc")
# print(rs)
# '''

#\w 匹配任意单词字符
# '''
# rs = re.match("\w","a")
# print(rs)
# rs = re.match("\w","A")
# print(rs)
# rs = re.match("\w","1")
# print(rs)
# rs = re.match("\w","_")
# print(rs)
# rs = re.match("\w","中")
# print(rs)
# rs = re.match("\w","*")  #非单词字符
# print(rs)
# '''

#[]
# '''
# rs = re.match("[Hh]","hello")
# print(rs)
# rs = re.match("[Hh]","Hello")
# print(rs)
# rs = re.match("[0123456789]","32")
# print(rs)
# #等价
# rs = re.match("[0-9]","3")
# print(rs)
# '''

#数量表示方法：
# *任意次
# '''
# rs = re.match("1\d*","1234567")   # 1开头，\d代表匹配数字
# print(rs.group())
# rs = re.match("1\d*","1234567abc")
# print(rs.group())
# '''

# + 至少出现一次
# '''
# rs = re.match("\d+","abc")
# print(rs)
# rs = re.match("\d+","1abc")
# print(rs)
# rs = re.match("\d+","123345abc")
# print(rs)
# '''

# ?至多1次（0次或者1次）
# '''
# rs = re.match("\d?","abc")
# print(rs)
# rs = re.match("\d?","123abc")
# print(rs)

# # {m} 固定次数
# rs = re.match("\d{3}","123abc")
# print(rs)
# # {m,} 至少m次
# rs = re.match("\d{6,}","123467abc") # 等价于+至少一次
# print(rs)

# {m,n} 至多m到n次
# rs = re.match("\d{0,1}","abc") # 等价于?至多一次
# rs = re.match("\d{0,3}","123abc") 
# print(rs)
# '''

#匹配11位的手机号
#11位，第一位1，第二位3,5,7,8 第3位到第11为0到9的数字
# '''
# rs = re.match("1[3578]\d{9}","13623198765")
# print(rs)
# rs = re.match("1[3578]\d{9}","14623198765")    #非法手机号
# print(rs)
# rs = re.match("1[3578]\d{9}","13623198765abc")   #非法手机号
# print(rs)
# '''

# 转义字符处理
# str1 = "hello\\world"
# print(str1)
# str2 = "hello\\\\world"
# print(str2)
# str3 = r"hello\\world"   # 前面加上r,代表原生字符串
# print(str3)
# rs = re.match("\w{5}\\\\\\\\\w{5}",str3)  # \w{5}表示5个字符，两个\\转义成一个\
# print(rs)
# rs = re.match(r"\w{5}\\\\\w{5}",str3)  # 原生字符串就代表原生的几个
# print(rs)

# 边界表示
# $ 字符串结尾
# '''
# rs = re.match("1[3578]\d{9}$","13623456767")
# print(rs)
# print(rs.group())
# rs = re.match("1[3578]\d{9}$","13623456767abc")
# print(rs)
# '''

# 邮箱匹配
# '''
# rs = re.match("\w{3,10}@163\.com$","hello_124@163.com")
# print(rs)
# rs = re.match("\w{3,10}@163.com$","he@163.com")
# print(rs)
# rs = re.match("\w{3,10}@163\.com$","hello_124@163mcom")
# print(rs)
# '''

# \b 单词边界
# rs = re.match(r".*\bpython\b","hi python hello")  # \bpython\b 表示python作为边界
# print(rs)
# # \B非单词边界
# rs = re.match(r".*\Bth\B","hi python hello")      
# print(rs)

# 匹配分组
# 匹配0到100之间的数字
# '''
# 1)0-9单独的数字，不能是01,02等这种格式
# 2)十位：1-9，个位：0-9
# 3）最大数字是100
# '''
# rs = re.match(r"[1-9]\d?$|0$|100$","100")   # |表示或
# print(rs)
# rs = re.match(r"[1-9]\d?$|0$|100$","0")
# print(rs)
# rs = re.match(r"[1-9]\d?$|0$|100$","12")
# print(rs)
# rs = re.match(r"[1-9]\d?$|0$|100$","01")
# print(rs)
# rs = re.match(r"[1-9]\d?$|0$|100$","200")
# print(rs)
# rs = re.match(r"[1-9]?\d?$|100$","0")
# print(rs)

# ()分组
# '''
# rs = re.match("\w{3,10}@(163|qq|outlook)\.com$","hello@163.com")  # $代表结束符
# print(rs)
# rs = re.match("\w{3,10}@(163|qq|outlook)\.com$","1234567@qq.com")
# print(rs)
# '''

# \num
html_str = "<head><title>python</title></head>"
# rs = re.match(r"<.+><.+>.+</.+></.+>", html_str)  # .+ 表示至少出现一次
# print(rs)

html_str2 = "<head><title>python</head></title>"
# rs = re.match(r"<.+><.+>.+</.+></.+>", html_str2)
# print(rs)

# rs = re.match(r"<(.+)><(.+)>.+</\2></\1>",html_str)
# print(rs)
# rs = re.match(r"<(.+)><(.+)>.+</\2></\1>",html_str2)
# print(rs)

# rs = re.match(r"<(?P<g1>.+)><(?P<g2>.+)>.+</(?P=g2)></(?P=g1)>",html_str)
# print(rs)

# search
# rs = re.search("car","haha car carbal abcar carbal")
# print(rs)

# findall 查找出结果，返回一个列表
# rs = re.findall("car","haha car carbal abcar carbal")
# print(rs)
mail_str = "zhangsan:helloworld@163.com,li:123456@qq.cn"
# list = re.findall(r"(\w{3,20}@(163|qq)\.(com|cn))",mail_str)
# print(list)

# finditer
# itor = re.finditer(r"\w{3,20}@(163|qq)\.(com|cn)",mail_str)
# for it in itor:
# 	print(it.group())

# sub
# str = "java python c cpp java"
# rs = re.sub(r"java","python",str)
# print(rs)
# '''

str_test = "apple=5,banana=3,orange=2"
# def update_price(result):
# 	price = result.group()
# 	new_price = int(price) + 1
# 	return str(new_price)
# rs = re.sub(r"\d+",update_price,str_test)
# print(rs)

#split
# price_list = str_test.split(",")
# for price in price_list:
# 	print(price)
# '''

# 贪婪模式
# rs = re.findall(r"hello\d*","hello12345")  # \d* 数字出现任意次
# print(rs)
# rs = re.findall(r"hello\d+","hello12345")  # \d+ 数字至少出现一次
# print(rs)
# rs = re.findall(r"hello\d?","hello12345")  # \d? 数字至多出现一次
# print(rs)
# rs = re.findall(r"hello\d{2,}","hello12345")   # \d? 数字两次及以上
# print(rs)
# rs = re.findall(r"hello\d{1,3}","hello12345")  # \d? 数字1次到3次
# print(rs)

# 非贪婪模式
rs = re.findall(r"hello\d*?","hello12345")
print(rs)
rs = re.findall(r"hello\d+?","hello12345")
print(rs)
rs = re.findall(r"hello\d??","hello12345")
print(rs)
rs = re.findall(r"hello\d{2,}?","hello12345")
print(rs)
rs = re.findall(r"hello\d{1,3}?","hello12345")
print(rs)





