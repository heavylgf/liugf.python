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




	


	