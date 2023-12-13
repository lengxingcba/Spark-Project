from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun


def data_process(path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.json(path)
    business_categories = fun.split(business['categories'], ',')   #将商业类别按“，”分割

    business = business.withColumn("categories", business_categories)   #用新分割出的 business_categories代替原来的类别
    business=business.filter(business["city"] != "") #去除city为空的商家

    #通过美国本土的经纬度范围去除异常数据
    business.createOrReplaceTempView("business")     #创建临时表供查询

    business=spark.sql("SELECT * FROM business where longitude between -130 and -69")
    business.createOrReplaceTempView("business")
    business=spark.sql("SELECT * FROM business where latitude between 25 and 50")
    business.createOrReplaceTempView("business")


    #筛选出需要的列business_id,city,stars,review_count,attributes,categories
    new_business=spark.sql("SELECT business_id,city,stars,review_count,attributes,categories FROM business")
    #new_business.createOrReplaceTempView(new_business)
    new_business.write.format("json").save("new_business.json")   #保存到new_business.json文件夹下

if __name__ == "__main__":
    path = 'file:///data/yelp/yelp_academic_dataset_business.json'
    data_process(path)