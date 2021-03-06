'''
数据分析系统
'''
# from log import LogMangerSys

import LogMangerSys
import datetime
import re
import itchat
import matplotlib.pyplot as plt

class DataAnalysisSys:
	def __init__(self):
		# 数据分析系统通过日志管理系统操作日志文件，分析数据
		self.log_manage = LogMangerSys()

	'''
	功能：根据指定时间间隔，获取当前时间前days天的日期
	参数：
		days：时间间隔，整数，如果获取当前日期则传入0
		format：日期格式化方式，“%Y%m%d”
	返回值：返回指定格式化后的日期
	'''
	def get_date(self,days,format):
		# 获取当前日期
		today = datetime.datetime.today()
		# 时间间隔
		timedelta = datetime.timedelta(days=days)
		#获取指定日期
		target_date = today - timedelta
		return target_date.strftime(format)
		
	'''
	功能：新增用户日报表
	'''
	def user_report(self):
		#获取统计报表的月份
		ym = self.get_date(3,"%Y%m")
		#通过日志管理系统读取用户信息csv文件
		datas = self.log_manage.read_log_csv("d://user_info.csv")
		new_user_dict = {} #格式：{"20180421":5,20180422":3,...}
		for data in datas:
			dt = data[1]
			rs = re.match("{}".format(ym),dt)
			if rs != None:
				#先从从存储新增用户数的字典中获取指定日期的新增用户数，然后+1
				user_num = new_user_dict.get(dt,0) + 1
				new_user_dict[dt] = user_num

		m_new_user_count = 0 #月新增用户
		#m_new_user_dict = {}
		for key,value in new_user_dict.items():
			m_new_user_count += value
		#m_new_user_dict[ym] = m_new_user_count
		print("{}月每日新增用户数：{}".format(ym,new_user_dict))
		print("{}月新增用户数：{}".format(ym,m_new_user_count))
	'''
	功能：销售日报表
	'''
	def sale_report(self):
		# 获取统计报表的月份
		ym = self.get_date(3, "%Y%m")
		file_dir = "d://buy_log//"
		files = self.log_manage.list_dir_file(file_dir)
		sale_money_dict = {}
		sale_count_dict = {}
		for file in files:
			if re.match("user_buy_log_{}".format(ym),file):
				file_path = file_dir + file
				datas = self.log_manage.read_log_csv(file_path)
				money = 0 #销售额
				count = 0 #销量
				for data in datas:
					money += float(data[1])#每个订单的金额累加，从文本读取出来的数据是字符串类型，需要转成浮点型
					items = data[2].split("|")
					count += len(items) #每天的销量 = 每个订单的商品列表数量累加
				file_date = file[13:21]
				sale_money_dict[file_date] = money
				sale_count_dict[file_date] = count
		print("{}月每日销量：{}".format(ym,sale_count_dict))
		print("{}月每日销售额：{}".format(ym,sale_money_dict))

	'''
	功能：微信好友性别分析报表
	'''
	def wechat_user_gender_report(self):
		itchat.login()
		friends = itchat.get_friends()
		male_count = 0
		female_count = 0
		other_count = 0
		for friend in friends[1:]:
			gender = friend["Sex"]
			if gender == 1:
				male_count += 1
			elif gender == 2:
				female_count += 1
			else:
				other_count += 1
		total = len(friends[1:]) #含有总数
		print("------------*微信好友分析报告*----------------")
		print("好友总数：{}".format(total))
		print("男性好友数：%d，占比：%.2f%%"%(male_count,float(male_count)/total*100))
		print("女性好友数：%d，占比：%.2f%%"%(female_count,float(female_count)/total*100))
		print("未知性别好友数：%d，占比：%.2f%%" % (other_count, float(other_count) / total * 100))

		datas = [male_count,female_count,other_count]
		labels = ["Male","Female","other"]
		self.get_pie(datas,labels)
	'''
	功能：微信好友地域分布分析报表
	'''
	def wechat_user_location_report(self):
		itchat.login()
		friends = itchat.get_friends()
		province_dict = {}
		for friend in friends[1:]:
			province = friend["Province"]
			if province == "":
				province = "未知"
			else:
				province_dict[province] = province_dict.get(province,0) + 1

		print(province_dict)
		data_list = []
		for item in province_dict.items():
			data_list.append(item)
		data_list.sort(key=lambda item:item[1],reverse=True)
		top10_datas = data_list[:11]
		print(top10_datas)
		datas_list = []
		labels_list = []
		for data in top10_datas:
			datas_list.append(data[1])
			labels_list.append(data[0])
		self.get_bar(datas_list,labels_list)


	'''
	功能：根据数据生成饼图
	参数说明：
		datas：展示的数据列表
		labels：展示的数据标签
	'''
	def get_pie(self,datas,labels):
		#设置字符集
		plt.rcParams["font.sans-serif"] = ["SimHei"]
		plt.figure(figsize=(8,6),dpi=80)
		plt.axes(aspect= 1)
		plt.pie(datas,labels=labels,autopct="%.2f%%",shadow=False)
		plt.title("微信好友性别分析图")
		plt.show()

	def get_bar(self,datas,labels):
		# 设置字符集
		plt.rcParams["font.sans-serif"] = ["SimHei"]
		plt.xlabel("province")
		plt.ylabel("count")
		plt.xticks(range(len(datas)),labels)
		plt.bar(range(len(datas)),datas,color="rgb")
		plt.title("微信好友地域分布图")
		plt.show()






das = DataAnalysisSys()
# das.user_report()
# das.sale_report()
# das.wechat_user_gender_report()
das.wechat_user_location_report()