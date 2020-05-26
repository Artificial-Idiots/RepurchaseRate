# coding = utf-8
import pandas as pd
import time


class RepurchaseRate(object):
    """计算产品复购率，百万条数据运行时间在20min左右
        1. 版本 0.5 包含 2 种计算方法
        2. 增加与MySQL数据连接后直接从数据获取数据的方法
    """

    def cal_repurchase_rate(self, method):
        if method == 1:
            # 算法一：单位时间内（按每月）：R = 复购人数/总购买人数
            total_transactions_dict, data_dict = self.data_processing(1)
        elif method == 2:
            # 算法二：单位时间内：R = 复购交易次数/总交易次数
            total_transactions_dict, data_dict = self.data_processing(2)
        # 复购交易总数表：字典
        repurchase_transactions_dict = {}
        # 对应月份表
        month_list = []
        # 对每月总交易数进行遍历并进行比较，得出每月复购交易数
        for month1 in data_dict.keys():
            repurchase_transactions_list = []  # 每月复购交易数列表
            # 计算每个月在所有月份的复购交易数
            for month2 in data_dict.keys():
                month_list.append(month2)
                # 判断month2对month1是否为后一个月，如果是，则进入复购数计算
                if month2 > month1:
                    i = 0
                    # 对month1来说，计算month2里有多少交易复购的
                    for num in data_dict[month2]:
                        # 该交易数据（手机号）在month1中出现，则认为是复购的，计数器+1
                        if num in data_dict[month1]:
                            i += 1
                    # 将month2中复购数据添加到复购交易列表
                    repurchase_transactions_list.append(i)
                # 如果month2对month1是前一个月，则复购数计为 0，并添加到复购交易列表进行占位，方便后续计算
                else:
                    repurchase_transactions_list.append(0)
            # 将对应月份列表加入到复购交易数据中，方便后续计算或查看
            repurchase_transactions_dict['month'] = month_list
            # 将每月复购交易数列表加入到复购交易数总表中
            repurchase_transactions_dict[month1] = repurchase_transactions_list

        # 计算复购率：R
        repurchase_rate_dict = {}  # 复购率总表
        month_list = []  # 对应月份
        # 对每月总交易数进行遍历
        for key1,value1 in total_transactions_dict.items():
            month_list.append(key1)
            repurchase_rate_list = []  # 每月复购率列表
            # 对每月复购交易数进行遍历
            for key2,value2 in repurchase_transactions_dict.items():
                if key1 == key2:
                    for num in value2:
                        # 计算每月复购率，格式化输出，按百分比保存，保留小数点后2位
                        repurchase_rate = format(num/value1, '.2%')
                        repurchase_rate_list.append(repurchase_rate)
            # 加入相对应的月份列表，方便后续计算或查看
            repurchase_rate_dict['month'] = month_list
            # 将每月复购率加入到复购率总表
            repurchase_rate_dict[key1] = repurchase_rate_list
        return repurchase_rate_dict

    def data_processing(self, x):
        # 数据处理：删除缺失值、对数据进行去重、按月对数据进行分组等
        # 读取文件
        raw_data = pd.read_csv('./repurchase_data')
        # 判断缺失值是否存在，存在就删除该条（行）数据
        i = 0
        while i < raw_data.shape[0]:
            if pd.isnull(raw_data['收货手机'][i]):
                # 根据行索引来删除该条数据，axis=0 代表行
                raw_data = raw_data.drop(i, axis=0)
            i += 1

        # 通过对订货年月分组得出所有月份和每月总交易数、每月购买人数
        # 每月总交易数：不去重数据
        total_transactions = raw_data.groupby(['订货年月']).count()
        # 将Dateframe数据转化为字典
        total_transactions_dict = total_transactions.to_dict()
        total_transactions_dict = total_transactions_dict['收货手机']

        # 每月购买人数：去重数据
        total_buyers = raw_data.groupby(['订货年月']).收货手机.nunique()
        # 将Dataframe数据转化为字典
        total_buyers_dict = total_buyers.to_dict()

        # 按月对数据进行分组，交易次数为不去重数据，购买人数为去重数据
        # 将raw_data转化成字典
        raw_data_dict = raw_data.to_dict()
        # 这里一个手机号即代表一个交易订单，即按月筛选手机号，存入字典
        data_dict = {}  # 不去重数据:交易次数
        uniq_data_dict = {}  # 去重数据：购买人数
        month_list = [x for x in total_transactions_dict.keys()]
        # 按月遍历添加交易数据
        for month in month_list:
            num_list = []  # 不去重列表
            uniq_num_list = []  # 去重列表
            for key,value in raw_data_dict['订货年月'].items():
                # 如果是这个月的交易数据，则加入列表
                if value == month:
                    num_list.append(raw_data_dict['收货手机'][key])  # 不去重：交易数
                    if raw_data_dict['收货手机'][key] not in uniq_num_list:
                        uniq_num_list.append(raw_data_dict['收货手机'][key])  # 去重：购买人数
            data_dict[month] = num_list
            uniq_data_dict[month] = uniq_num_list

        # 测试程序用
        print('数据处理完成！')

        # 需要补充完善类里面不同函数数据传递方式??????
        if x == 1:
            return total_buyers_dict, uniq_data_dict
        elif x == 2:
            return total_transactions_dict, data_dict
        else:
            print('请输入数字1或者2')


def main():
    # 开始计时
    time_start = time.time()
    print('开始计时。。。')
    repurchase_rate = RepurchaseRate()
    result = repurchase_rate.cal_repurchase_rate(1)
    print('计时结束！！！')
    time_end = time.time()
    # 运行所花的时间
    time_c = time_end - time_start
    print(result)
    print('本次计算耗时：%d 秒' % time_c)


if __name__ == "__main__":
    main()

