import datetime
import csv
'''
用户管理系统
仓库管理系统
货架管理系统
升级购物车
推荐系统
'''
from infrastructure.warehouse import WarehouseManageSys
from infrastructure.rack import RackManageSys
from infrastructure.user import UserManageSys,BuyCar
from infrastructure.log import LogMangerSys
from infrastructure.recommend import BaseItemRecommendSys

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




