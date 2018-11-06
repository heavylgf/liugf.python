#用户管理系统
import re
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

	'''
	功能：验证用户id是否合法
	参数说明：
		user_id：用户编号
	'''
	def user_id_check(self,user_id):
		rs = re.match(r"1[3578]\d{9}$",user_id)
		if rs != None:
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

	'''
	功能：验证商品id是否合法
	参数说明：
		item_id：商品id
	'''
	def item_id_check(self,item_id):
		if item_id != "":
			item_ids = ["1","2","3"]
			if item_id in item_ids:
				return True
			else:
				return False
		else:
			return False