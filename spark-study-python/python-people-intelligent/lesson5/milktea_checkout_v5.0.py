"""
    项目名称：小象奶茶馆结算系统
    作者：Jiessie
    时间：2018-4-12
    版本：第五版（记录今日时间，增加系统服务时长，手机号输入位数必须为11位，否则提示继续输入，增加会员信息，将购物信息文件输出到csv中，将会员信息记录在csv文件中）
"""
import csv
import datetime


# 定义购物过程函数
def shopping_procedure(vip_milkteano_list):
    # 字典记录奶茶编号和数量
    goods_dic = {}
    buy_y_or_n = input('今日推荐招牌4号蒟蒻冰奶茶，是否购买（y/n)?')

    if buy_y_or_n.lower() == 'y':
        milk_tea_no = '4'
    elif buy_y_or_n.lower() == 'n':
        milk_tea_no = input('请选择您要购买的奶茶编号：')
    # 集合记录购买的奶茶品种，因为可能会出现相同品种，所以选择集合去重
    chosen_milk_tea_no = set()
    while True:
        if milk_tea_no not in goods_dic.keys():
            if int(milk_tea_no) <= 5 and int(milk_tea_no) >= 1:
                milk_tea_amount = input('请输入您要购买的数量：')
                goods_dic[milk_tea_no] = int(milk_tea_amount)
                chosen_milk_tea_no.add(milk_tea_no)
            else:
                print('Woops!我们只售卖以上五种奶茶哦！新口味敬请期待！')
        else:
            milk_tea_amount = input('请输入您要购买的数量：')
            goods_dic[milk_tea_no] += int(milk_tea_amount)

        milk_tea_no = input('您还需要其它口味吗？请输入您要购买的奶茶编号，选择T查看买了以上口味的其他顾客还喜欢什么口味，完成购物请选择Q：')

        # 如果非T非Q，继续选择奶茶编号
        if milk_tea_no.upper() != 'T' and milk_tea_no.upper() != 'Q':
            chosen_milk_tea_no.add(milk_tea_no)

        # 选择推荐
        elif milk_tea_no.upper() == 'T':
            # 交集列表
            common_choose_list = []
            # 差集列表
            recommend_choose_list = []

            # 记录交集和差集
            for i in range(len(vip_milkteano_list)):
                common_choose = chosen_milk_tea_no & vip_milkteano_list[i]
                recommend_choose = vip_milkteano_list[i] - chosen_milk_tea_no
                # 如果交集和差集都非空，记入列表
                if len(common_choose) != 0 and len(recommend_choose) != 0:
                    common_choose_list.append(common_choose)
                    recommend_choose_list.append(recommend_choose)

            # 如果交集差集列表为空，推荐默认奶茶或者等待推荐
            if len(common_choose_list) == 0 :
                if '4' not in chosen_milk_tea_no:
                    print('您可以尝尝我们的招牌4号蒟蒻冰奶茶！')
                else:
                    print('与您口味相同的其它顾客正在推荐的道路上……')

            else:
                # 记录交集元素长度
                len_common_choose_list = []
                for common in common_choose_list:
                    len_common_choose_list.append(len(common))

                # 记录长度最大值索引值
                rec_index = []
                for i in range(len(len_common_choose_list)):
                    if len_common_choose_list[i] == max(len_common_choose_list):
                        rec_index.append(i)

                # 记录推荐元素集合
                rec_set = set()
                for index in rec_index:
                    for rec in recommend_choose_list[index]:
                        rec_set.add(rec)

                print('买了以上口味的其他顾客还喜欢{}号奶茶'.format(max(rec_set)))


            milk_tea_no = input('您还需要其它口味吗？请选择您要购买的奶茶编号，完成购物请选择Q：')
            if milk_tea_no.upper() == 'Q':
                vip_milkteano_list.append(chosen_milk_tea_no)
                break

        # 点单完成，退出程序
        elif milk_tea_no.upper() == 'Q':
            vip_milkteano_list.append(chosen_milk_tea_no)
            break

    return goods_dic


# 定义价格结算函数
def original_money(goods_dic):
    total_money = 0
    for milk_tea_no,milk_tea_amount in goods_dic.items():
        price = 0
        if milk_tea_no == "1":
            price = 3
        elif milk_tea_no == "2" or milk_tea_no == "3":
            price = 5
        elif milk_tea_no == "4" or milk_tea_no == "5":
            price = 7

        total_money += milk_tea_amount * price

    return total_money


# 定义购物信息打印函数
def shopping_print(goods_dic):
    print('点单完成！您的购买详情为：')

    for milk_tea_no, milk_tea_amount in goods_dic.items():
        print('{}号奶茶:{}杯'.format(milk_tea_no, milk_tea_amount))

    total_money = original_money(goods_dic)
    print('您的总消费额为：{}元'.format(total_money))
    return total_money


# 定义记录所有顾客购物日志函数
def shopping_log(goods_dic,vip_no,total_consumer_record,today):
    for milk_tea_no, milk_tea_amount in goods_dic.items():
        single_consumer_record = {}
        single_consumer_record['vip_no'] = vip_no
        single_consumer_record['milk_tea_no'] = milk_tea_no
        single_consumer_record['mile_tea_amount'] = milk_tea_amount
        single_consumer_record['date'] = today
        total_consumer_record.append(single_consumer_record)
    return total_consumer_record


# 判断手机号格式是否正确的函数
def if_phone_num_right(vip_phone_no):
    while True:
        if len(vip_phone_no) == 11:
            break
        else:
            vip_phone_no = input('对不起，您输入的手机号有误，请重新输入：')
    return vip_phone_no

# 记录会员信息
def vip_dic_record(vip_dic,vip_no,today):
    vip_phone_no_str = input('请输入您的手机号激活会员：')
    vip_phone_no = if_phone_num_right(vip_phone_no_str)
    birthday = input('请输入您的生日（yyyymmdd):')
    sex = input('请输入您的性别（M/F):')
    constellation = input('请输入您的星座：')
    place = input('请输入您的所在地：')

    vip_dic[vip_no] = [vip_phone_no, birthday, sex, constellation, place,today]
    return vip_dic


# 主函数
def main():
    vip_dic = {}
    vip_milkteano_list = []
    j = 1

    while True:
        total_consumer_record = []
        today = datetime.date.today()
        # 实际运用时不需要写下行，这里为了演示代码可以给日期增加天数
        today += datetime.timedelta(days = j-1)

        print('\n今天是{},小象奶茶馆营业第{}天，美好的一天开始了！'.format(today,j))
        i = 1

        while True:
            print('\n欢迎光临小象奶茶馆！本店售卖宇宙无敌奶茶，奶茶虽好，可不要贪杯哦！\n 1）原味冰奶茶 3元  2）香蕉冰奶茶 5元 '
                  ' 3) 草莓冰奶茶 5元  4）蒟蒻冰奶茶 7元  5）珍珠冰奶茶 7元')
            print('本店每日接待20位顾客，您是今天第{}位幸运儿'.format(i))

            goods_dic = shopping_procedure(vip_milkteano_list)
            vip_no = input('请输入您的专属会员号(新会员直接设置会员号即可，激活手机号方可享受会员价）：')
            total_money = shopping_print(goods_dic)

            if vip_no in vip_dic.keys():
                total_money *= 0.9
                total_money = round(total_money,2)
                print('您可以享受会员价，折后总价：{}元'.format(total_money))
            else:  # 非会员则添加到会员表中
                vip_dic = vip_dic_record(vip_dic,vip_no,today)

            total_consumer_record = shopping_log(goods_dic, vip_no, total_consumer_record,today)

            print("\n********************************************************")
            print('\t小象奶茶馆力争做一枚有态度、有思想的奶茶馆（傲娇脸）！\n\t祝您今日购物愉快！诚挚欢迎您再次光临！')
            print("********************************************************")

            i += 1

            if i > 20:
                print('今日已闭店，欢迎您明天光临！')
                break

        # 将会员信息写入文件中
        f_vip_no = open('vip_no.csv', 'a')
        writer = csv.writer(f_vip_no)
        # 第一天时写入标题行，之后无需写入
        if j == 1:
            writer.writerow(['vip_no', 'phone_no', 'birthday', 'sex', 'constellation', 'place','enrolldate'])
        for key, values in vip_dic.items():
            # 只记录当天新会员
            if values[5] == today:
                values.insert(0, key)
                writer.writerow(values)
                del values[0]
        f_vip_no.close()

        # 将顾客购物信息写入文件
        f_record = open('user_buy_log_{}.csv'.format(today), 'w')
        writer2 = csv.writer(f_record)
        #标题行写入
        header = []
        for key in total_consumer_record[0].keys():
            header.append(key)

        writer2.writerow(header)

        # i 为文件的行数
        for i in range(len(total_consumer_record)):
            data = []
            for z in total_consumer_record[i].values():
                data.append(z)
            writer2.writerow(data)
        f_record.close()

        j += 1

        if j > 30:
            break

main()





