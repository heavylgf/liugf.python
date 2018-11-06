
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
