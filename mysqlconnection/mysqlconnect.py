import pymysql
import pandas as pd
con =pymysql.connect(host="localhost",port=1433,user="sa",password="123456",db="business_analysis")

# 通用读取数据语句
data=pd.read_json("E:/download/yelp数据集/数据分析/结果数据/category_count.json",lines=True)
sql_category_count="insert into category_count(category,count) values(%s,%s)"
sql_review_count="insert into category_review_count(category,review_count) values(%s,%s)"
sql_city_category_count="insert into city_category_count(city,category,count) values(%s,%s,%s)"
sql_category_avg_stars="insert into category_avgstars(category,avgstars) values(%s,%s)"

while True:
    try:
        cs = con.cursor() # 获取游标
        cs.executemany(sql_category_count,data)
        con.commit()
        cs.close()
        con.close()
        print('OK')
        break
    except Exception as error:
        con.ping(True)


