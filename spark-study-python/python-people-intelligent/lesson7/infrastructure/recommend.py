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