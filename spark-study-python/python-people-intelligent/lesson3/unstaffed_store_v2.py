'''
无人便利店升级:
    1.将具有相同功能的代码封装成函数
    2.添加购物车结算功能
    3.货架商品卖完，自动补货
'''

# 定义仓库
def warehouse(item_type):
    # 泡面
    pm_list = ["老坛酸菜", "红烧牛肉", "酸辣粉", "拉面"]
    # 榨菜
    zc_list = ["老干妈", "乌江"]
    # 香肠
    xc_list = ["王中王", "蒜肠", "淀粉肠"]
    if item_type == "pm":
        return  pm_list
    elif item_type == "zc":
        return  zc_list
    elif item_type == "xc":
        return  xc_list

# 检测货架上的商品是否需要补货
def check_add_rack(rack,item_type,item_counts):
    if len(rack) == 0:
        print("---正在更新货架，请稍等---")
        item_list = warehouse(item_type)
        while len(rack) < item_counts:
            rack_index = len(rack) % len(item_list)
            rack.append(item_list[rack_index])
        print("----商品已上架----")

# 购物车结算系统
def buy_car_account(buy_car):
    item_detail = {"老坛酸菜":5, "红烧牛肉":4, "酸辣粉":6, "拉面":7,"老干妈":10, "乌江":2,"王中王":2, "蒜肠":12, "淀粉肠":8}
    total_money = 0
    for item in buy_car:
        total_money += item_detail.get(item,0)
    return total_money

# 购物大厅
def shopping_hall():
    # 三个空货架
    pm_rack = []
    zc_rack = []
    xc_rack = []
    # 货架摆放商品数量
    pm_rack_counts = 1
    zc_rack_counts = 1
    xc_rack_counts = 1

    # 定义一个空购物车
    buy_car = []
    if_new_user = True
    while True:
        # 自动检测货架是否需要补货
        check_add_rack(pm_rack,"pm",pm_rack_counts)
        check_add_rack(zc_rack,"zc",zc_rack_counts)
        check_add_rack(xc_rack,"xc",xc_rack_counts)
        if if_new_user:
            print("欢迎光临")
        item_id = input("==本店售卖商品：1 泡面，2 榨菜，3 香肠。请输入想要购买的商品编号：")
        if int(item_id) == 1:
            if len(pm_rack) >= 1:
                buy_car.append(pm_rack[len(pm_rack) - 1])
                pm_rack.pop()
            else:
                print("亲！非常抱歉，泡面已卖完。")
        elif int(item_id) == 2:
            if len(zc_rack) >= 1:
                buy_car.append(zc_rack[len(zc_rack) - 1])
                zc_rack.pop()
            else:
                print("亲！非常抱歉，榨菜已卖完。")
        elif int(item_id) == 3:
            if len(xc_rack) >= 1:
                buy_car.append(xc_rack[len(xc_rack) - 1])
                xc_rack.pop()
            else:
                print("亲！非常抱歉，香肠已卖完。")
        else:
            print("亲！您输入的商品还在火星，请输入在售的商品编号！")
            continue

        if_buy = input("请输入y或者n选择是否继续购物：")
        if if_buy == "n":
            if len(buy_car) > 0:
                total_money = buy_car_account(buy_car)
                print("您的购物车商品如下：",buy_car)
                print("$您本次消费金额{}元：".format(total_money))
                buy_car = []
                if_new_user = True
                print("欢迎下次光临")
            else:
                print("您没有购买任何商品")
                print("欢迎下次光临")
        else:
            if_new_user = False

shopping_hall()



