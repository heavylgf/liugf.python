import datetime
import csv
import os
#日志管理系统
class LogMangerSys:
	def __init__(self):
		self.buy_logs = []
	'''
	功能：获取写日志的当前时间
	参数：
	    format：日期格式化方式，如："%Y%m%d"
	'''
	def get_log_time(self, format):
		log_time = datetime.datetime.now().strftime(format)
		return log_time

	'''
	功能：将日志追加到csv文件持久化存储
	参数：
	    file_path：文件路径
	    file_name：文件名称
	    header：文件标题
	    data：日志数据，[{key:value}]
	'''
	def write_log_append_csv(self, file_path, file_name, header, data):
		# 写日志时间
		log_time = self.get_log_time("%Y%m%d")
		print("log_time:{}".format(log_time))
		# 文件格式：file_path + file_name+log_time
		# 输出的csv文件名称
		new_file_name = file_path + file_name + "_" + log_time + ".csv"
		with open(new_file_name, "a", newline="", encoding="utf-8") as f:
			writer = csv.DictWriter(f, header)
			# writer.writeheader()
			writer.writerows(data)

	'''
	功能：用户购买日志写入到日志文件
	参数说明：
		user_id：用户编号
		money：消费金额
		items：购买商品列表，格式:[{"user_id":"user_id1","money":20,"items":(item1,itme2....)}]
	'''
	def buy_log_manage(self,user_id, money, *items):
		buy_log = {"user_id": user_id, "money": money, "items": items}
		self.buy_logs.append(buy_log)
		# ------------v4 start------------------#
		item_str = ""  # 格式：老干妈|王中王
		for item in items:
			if item_str == "":
				item_str = item
			else:
				item_str += "|" + item

		file_path = "d://"
		file_name = "user_buy_log"
		header = ["user_id", "money", "items"]
		buy_log = [{"user_id": user_id, "money": money, "items": item_str}]
		#调用自身将日志数据写入到CSV文件的方法
		self.write_log_append_csv(file_path, file_name, header, buy_log)
	'''
	功能：根据文件路径，读取CSV文件
	参数：
		file_path：csv文件路径
	返回值：
		返回列表类型数据，格式：[[user_id,datetime],...]
	'''
	def read_log_csv(self,file_path):
		datas = []
		with open(file_path,"r",encoding="utf-8") as f:
			reader = csv.reader(f)
			for row in reader:
				datas.append(row)
		return datas
	'''
	功能：读取指定文件夹下的所有文件
	参数说明：
		file_dir：指定文件夹
	返回值：
		文件下的所有文件名称
	'''
	def list_dir_file(self,file_dir):
		files = os.listdir(file_dir)
		return files

