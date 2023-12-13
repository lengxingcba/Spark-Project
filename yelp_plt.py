import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D as AX3
import seaborn as sns
import numpy as np
import pandas as pd

def Toronto_category_top50_plt():
    data=pd.read_csv("city_category_amount.csv")
    data=data.query("city=='Toronto'")
    plt.rcParams['axes.unicode_minus'] = False
    # plt.rcParams['font.sans-serif'] = ['KaiTi']
    plt.tight_layout()
    sns.set(rc={"figure.figsize": (100, 16)})
    sns.despine(bottom=True)
    sns.barplot(data["category"][:50], data["amount"[:50]])
    plt.ylabel("amount", fontsize=10)
    plt.xlabel("category", fontsize=10)
    plt.xticks(size=5)
    plt.title("Toronto category top50")
    plt.show()
#Toronto_category_top50_plt()

def city_business_top10():
    data = pd.read_json("E:/download/yelp数据集/数据分析/city_business_count.json",lines=True)
    plt.rcParams['axes.unicode_minus'] = False
    # plt.rcParams['font.sans-serif'] = ['KaiTi']
    plt.tight_layout()
    sns.set(rc={"figure.figsize": (50, 16)})
    sns.despine(bottom=True)
    sns.barplot(data["city"][:10], data["count"][:10])
    plt.ylabel("business_count", fontsize=10)
    plt.xlabel("city", fontsize=10)
    plt.xticks(size=8)
    plt.title("city_business_top10")
    plt.show()

def category_review_top10():
    data = pd.read_json("E:/download/yelp数据集/数据分析/most_popular_category.json", lines=True)
    plt.rcParams['axes.unicode_minus'] = False
    # plt.rcParams['font.sans-serif'] = ['KaiTi']
    plt.tight_layout()
    sns.set(rc={"figure.figsize": (50, 16)})
    sns.despine(bottom=True)
    sns.barplot(data["category"][:10], data["review_count"][:10])
    plt.ylabel("review_count", fontsize=15)
    plt.xlabel("category", fontsize=15)
    plt.xticks(size=8)
    plt.title("category_review_top10")
    plt.show()

def category_most_popular_top10():
    data = pd.read_json("E:/download/yelp数据集/数据分析/avg_stars.json", lines=True)
    plt.figure(figsize=(10,5))
    plt.barh(data["category"][:10],data["avg_stars"][:10])
    plt.xlabel("avg_stars",fontsize=15)
    plt.ylabel("category",fontsize=15)
    plt.title("category_MostPopular_top10")
    plt.show()
    plt.close()

def businessparking():
    data = pd.read_json("E:/download/yelp数据集/数据分析/Business_Parking.json", lines=True)
    num={}
    print(type(data))
    no_parking = len(data.query("BusinessParking=='None'"))

    # plt.pie(num.values(),labels=num.keys(),autopct="%3.1f%%")
    # plt.title("BusinessParking")
    # plt.show()
    # plt.close()



if __name__=="__main__":
    #Toronto_category_top50_plt()
    #city_business_top10()
    category_review_top10()
    #category_most_popular_top10()
    #businessparking()

