from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def data_analysis_top10(path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business=spark.read.json(path)
    business.createOrReplaceTempView("business")

    #使用explode函数将类别属性的多个值展开变为一一对应关系方便统计
    new_data=spark.sql("SELECT explode(categories) AS category FROM business")
    new_data.createOrReplaceTempView("new_data")


    all_category=spark.sql("SELECT distinct(new_data.category) AS all_category FROM new_data")
    #按 category进行分组，输出为临时表categories_counti
    categories_count=spark.sql("SELECT category,count(*) AS count FROM new_data group by category")
    categories_count.createOrReplaceTempView("categories_count")
    #按count列降序排列并只输出前10个，结果保存在categories_top10.json里
    categories_count=spark.sql("SELECT * FROM categories_count ORDER BY count desc limit 10")
    #categories_count = spark.sql("SELECT * FROM categories_count ORDER BY count desc")
    categories_count.write.format("json").save("categories_count.json")

def data_analysis_city_category(path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business=spark.read.json(path)
    business.createOrReplaceTempView("business")
    # 使用explode函数将类别属性的多个值展开变为一一对应关系方便统计
    city_category=spark.sql("SELECT city,explode(categories) AS category FROM business")
    city_category.createOrReplaceTempView("city_category")
    #筛选出city 和category以及category的个数 按city和category聚合，按category的个数降序排列。
    city_category_count=spark.sql("SELECT city,category,count(*) AS amount FROM city_category Group by city,category"
                                  " ORDER BY amount desc")
    #结果保存在city_category_amount.json文件夹下。
    city_category_count.write.format("json").save("city_category_amount.json")


def data_analysis_most_buiness(path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.json(path)
    business.createOrReplaceTempView("business")
    #筛选出city，business_id
    city_business_count=spark.sql("SELECT city,business_id  FROM business")
    city_business_count.createOrReplaceTempView("city_business_count")
    #有多少给business_id就有多少个城市名,所以按各个城市名个数来统计每个城市的商家数量，降序排列
    city_business_count=spark.sql("SELECT city,count(*) AS count FROM city_business_count GROUP BY city"
                                  " ORDER BY count desc")
    # 结果保存在city_business_count.json文件夹下。
    city_business_count.write.format("json").save("city_business_count.json")

def data_analysis_most_review_category(path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.json(path)
    business.createOrReplaceTempView("business")
    #筛选出review_count 和展开的categories。
    new_data=spark.sql("SELECT review_count,explode(categories) AS category FROM business")
    new_data.createOrReplaceTempView("new_data")
    #按每个商业类别的总评论树统计，按总评论数降序排列
    d1=spark.sql("SELECT category,sum(review_count) AS review_count FROM new_data GROUP BY category"
                 " ORDER BY review_count desc")
    # 结果保存在most_popular_category.json文件夹下。
    d1.write.format("json").save("most_popular_category.json")

def data_analysis_most_popular_category(path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.json(path)
    business.createOrReplaceTempView("business")
    #筛选出每种商业类型以及他们的星级
    new_data=spark.sql("SELECT stars,explode(categories) AS category FROM business")
    new_data.createOrReplaceTempView("new_data")
    #按每种商业类型的平均星级来选出最受欢迎的商业类型，降序排列
    d1=spark.sql("SELECT category,avg(stars) AS avg_stars FROM new_data GROUP BY category ORDER BY avg_stars desc")
    # 结果保存在most_popular_category2.json文件夹下。
    d1.write.format("json").save("most_popular_category2.json")

def businessParking():
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.json(path)
    business.createOrReplaceTempView("business")

    business_parking=spark.sql("SELECT business_id,attributes.BusinessParking.garage AS {s} FROM business".format(s="garage"))
    #business_parking.createOrReplaceTempView("business_parking")
    business_parking.write.format("json").save("parking_garage.json")

    #parking_garage.write.format("json").save("parking_garage.json")





if __name__ == "__main__":
    path = 'file:///root/spark_homework/new_business.json'
    #data_analysis_top10(path)
    #data_analysis_city_category(path)
    #data_analysis_most_buiness(path)
    #data_analysis_most_review_category(path)
    #data_analysis_most_popular_category(path)
    businessParking()