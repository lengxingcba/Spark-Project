import pandas as pd
import json
import matplotlib.pyplot as plt

# 读取Business_Parking 数据包含business_id 和 BusinessParking 列
data = pd.read_json("E:/download/yelp数据集/数据分析/Business_Parking.json", lines=True)
data = data.query("BusinessParking!='None'")  # 去除BusinessParking为None的数据
BusinessParking = data["BusinessParking"]  # 只需要BusinessParking
BusinessParking = pd.DataFrame(BusinessParking)
# 去除空值和统计没有BusinessParking的数量
len1 = len(BusinessParking)
BusinessParking.dropna(inplace=True)
len_no_parking = len1 - len(BusinessParking)

# 由于BusinessParking存在嵌套数据且需要转化的数据存在不规范问题导致不能转化为json格式，这里使用先将数据转换为list格式
dic = []
dic = BusinessParking.values.tolist()
len_dic = len(dic)
garage = 0
street = 0
validated = 0
lot = 0
valet = 0

# 再用字符替换将数据规范化，并统计每种额外服务的数量，结果以字典形式保存在parking中
for i in range(len_dic):
    if dic[i][0] != "{}":
        a = dic[i][0].replace("'", '"')
        # print(a)
        b = a.replace("F", "f")
        # print(b)
        c = b.replace("T", "t")
        c = c.replace("None", "false")
        # print(c)
        d = json.loads(c)
        # print(d)
        if "garage" in d.keys() and d["garage"] == True:
            garage += 1
        if "street" in d.keys() and d["street"] == True:
            street += 1
        if "validated" in d.keys() and d["validated"] == True:
            validated += 1
        if "lot" in d.keys() and d["lot"] == True:
            lot += 1
        if "valet" in d.keys() and d["valet"] == True:
            valet += 1
parking = {}
parking.update({"garage": garage, "street": street, "validated": validated, "lot": lot, "valet": valet,
                "no_parking": len_no_parking})
print(parking)


def plot_parking():
    labels = parking.keys()
    X = parking.values()
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['font.sans-serif'] = ['KaiTi']
    fig = plt.figure()
    plt.pie(X, labels=labels, autopct='%1.2f%%')  # 画饼图（数据，数据对应的标签，百分数保留两位小数点）
    plt.title("外卖配送范围情况")

    plt.show()
    plt.close()


plot_parking()
