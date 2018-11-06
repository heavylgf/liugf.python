import datetime
import csv
'''
用户管理系统
仓库管理系统
货架管理系统
升级购物车
推荐系统
'''

#仓库管理系统
class WarehouseManageSys:
	def __init__(self):
		#商品清单
		self.item_detail = {"老坛酸菜":5, "红烧牛肉":4, "酸辣粉":6, "拉面":7,"老干妈":10, "乌江":2,"王中王":2, "蒜肠":12, "淀粉肠":8}
	'''
	功能：根据商品类型返回商品列表
	参数说明：
		item_type：商品类型
	'''
	def get_item_list(self,item_type):
		# 泡面
		pm_list = ["老坛酸菜", "红烧牛肉", "酸辣粉", "拉面"]
		# 榨菜
		zc_list = ["老干妈", "乌江"]
		# 香肠
		xc_list = ["王中王", "蒜肠", "淀粉肠"]
		if item_type == "pm":
			return pm_list
		elif item_type == "zc":
			return zc_list
		elif item_type == "xc":
			return xc_list

	'''
	功能：添加或者更新商品的价格
	参数说明：
		kwargs：商品名称和价格的键值对，可以传入多个
	'''
	def add_update_item_info(self,**kwargs):
		for item,price in kwargs.items():
			self.item_detail[item] = price
#货架管理系统
class RackManageSys:
	'''
	功能：检测货架上的商品是否需要补货
	参数说明：
		rack：货架列表
		item_type：商品类型
		item_counts：货架可摆放的商品数量
		warehouse_manage：仓库管理系统的对象
	'''
	def check_add_rack(self,rack, item_type, item_counts,warehouse_manage):
		if len(rack) == 0:
			print("---正在更新货架，请稍等---")
			#根据商品类型从仓库中获取商品列表
			item_list = warehouse_manage.get_item_list(item_type)
			while len(rack) < item_counts:
				rack_index = len(rack) % len(item_list)
				rack.append(item_list[rack_index])
			print("----商品已上架----")


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

#用户管理系统
class UserManageSys:
	def __init__(self):
		self.user_id_set = set()
	'''
	功能：添加新用户
	参数说明：
		user_id：用户编号
	'''
	def add_new_user(self,user_id):
		if user_id not in self.user_id_set:
			self.user_id_set.add(user_id)
	'''
	功能：验证用户是否是VIP
	参数说明：
		user_id：用户编号
	'''
	def if_vip(self,user_id):
		if user_id in self.user_id_set:
			return True
		else:
			return False
#购物车
class BuyCar:
	def __init__(self,user_id,user_manage):
		self.user_id = user_id
		#验证用户是否是VIP
		self.if_vip = user_manage.if_vip(self.user_id)
		#初始化一个购物车的车筐
		self.buy_car = []
	'''
	功能：向购物车添加商品
	参数说明：
		pm_rack：泡面货架
		zc_rack：榨菜货架
		xc_rack：香肠货架
		item_id：商品编号
	'''
	def add_item_2_car(self,pm_rack,zc_rack,xc_rack,item_id):
		if int(item_id) == 1:
			if len(pm_rack) >= 1:
				self.buy_car.append(pm_rack[len(pm_rack) - 1])
				pm_rack.pop()
			else:
				print("亲！非常抱歉，泡面已卖完。")
		elif int(item_id) == 2:
			if len(zc_rack) >= 1:
				self.buy_car.append(zc_rack[len(zc_rack) - 1])
				zc_rack.pop()
			else:
				print("亲！非常抱歉，榨菜已卖完。")
		elif int(item_id) == 3:
			if len(xc_rack) >= 1:
				self.buy_car.append(xc_rack[len(xc_rack) - 1])
				xc_rack.pop()
			else:
				print("亲！非常抱歉，香肠已卖完。")
		else:
			print("亲！您输入的商品还在火星，请输入在售的商品编号！")

	'''
	功能：购物车结算
	参数说明：
		warehouse_manage：仓库管理系统对象，用于获取商品价格清单
	'''
	def account(self,warehouse_manage):
		total_money = 0
		for item in self.buy_car:
			total_money += warehouse_manage.item_detail.get(item, 0)
		if self.if_vip:
			vip_money = total_money * 0.9
			total_money = float("%.2f" % vip_money)
		return total_money
#推荐系统父类
class RecommendSys:
	def recommend(self):
		print("推荐商品")

#基于物品的推荐系统
class BaseItemRecommendSys(RecommendSys):
	def recommend(self,user_id,buy_logs):
		user_item_set = set()  #被推荐人历史购买商品
		other_user_item_dict = {}  # 其他用户历史购买商品 {"user_id":{item1,item2}}
		for log in buy_logs:
			user_id_key = log["user_id"]
			items_value = log["items"]
			if user_id_key == user_id:
				user_item_set.update(items_value)
			else:
				items_set = other_user_item_dict.get(user_id_key)
				if items_set == None:
					other_user_item_dict[user_id_key] = set(items_value)
				else:
					items_set.update(items_value)
					other_user_item_dict[user_id_key] = items_set

		recommend_list = []  # 被推荐列表
		for value_set in other_user_item_dict.values():
			inner_set = user_item_set & value_set
			length = len(inner_set)
			if length > 0 and length < len(value_set):
				diff_set = value_set - user_item_set
				recommend_list.append({"common_num": length, "items": diff_set})
		if len(recommend_list) > 0:
			recommend_list.sort(key=lambda x: x["common_num"], reverse=True)
			recommend_set = recommend_list[0]["items"]
			return list(recommend_set)  # 集合转列表
class UnstaffedStore:
	# 购物大厅
	def shopping_hall(self):
		#仓库管理系统初始化
		warehouse_manage = WarehouseManageSys()
		#货架管理系统初始化
		rack_manage = RackManageSys()
		#用户管理系统初始化
		user_manage = UserManageSys()
		#日志管理系统初始化
		log_manage = LogMangerSys()
		#推荐系统初始化
		recommend_sys = BaseItemRecommendSys()

		# 三个空货架
		pm_rack = []
		zc_rack = []
		xc_rack = []
		# 货架摆放商品数量
		pm_rack_counts = 1
		zc_rack_counts = 1
		xc_rack_counts = 1

		while True:
			print("欢迎光临")
			user_id = ""
			while True:
				user_id = input("请输入手机号作为用户id使用：")
				if user_id != "":
					user_manage.add_new_user(user_id)
					break
				else:
					print("输入的手机号不能为空，请输入正确的手机号！")
			#给用户分配一个购物车
			buy_car = BuyCar(user_id,user_manage)

			while True:
				# 自动检测货架是否需要补货
				rack_manage.check_add_rack(pm_rack, "pm", pm_rack_counts,warehouse_manage)
				rack_manage.check_add_rack(zc_rack, "zc", zc_rack_counts,warehouse_manage)
				rack_manage.check_add_rack(xc_rack, "xc", xc_rack_counts,warehouse_manage)
				item_id = input("==本店售卖商品：1 泡面，2 榨菜，3 香肠。请输入想要购买的商品编号：")
				#向购物车添加商品
				buy_car.add_item_2_car(pm_rack,zc_rack,xc_rack,item_id)
				if_buy = input("请输入y或者n选择是否继续购物：")
				if if_buy == "n":
					#购物车结算
					if len(buy_car.buy_car) > 0:
						total_money = buy_car.account(warehouse_manage)
						print("您的购物车商品如下：", buy_car.buy_car)
						print("$您本次消费金额{}元：".format(total_money))
						# 购物日志管理
						log_manage.buy_log_manage(user_id, total_money, *buy_car.buy_car)
						recommend_item_list = recommend_sys.recommend(user_id,log_manage.buy_logs)
						if recommend_item_list != None:
							print("买了该商品的其他用户，还买了{}".format(recommend_item_list))
						print("欢迎下次光临")
					else:
						print("您没有购买任何商品")
						print("欢迎下次光临")
					break
store = UnstaffedStore()
store.shopping_hall()




