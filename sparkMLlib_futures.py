from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

from pyspark.ml.feature import HashingTF,IDF,Tokenizer
spark = SparkSession.builder.master("local").appName("futures").getOrCreate()
sentenceData = spark.createDataFrame([(0, "I heard about Spark and I love Spark"),(0, "I wish Java could use case classes"),(1, "Logistic regression models are neat")]).toDF("label", "sentence")
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
wordsData.show()
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2000)
featurizedData = hashingTF.transform(wordsData)
featurizedData.select("words","rawFeatures").show(truncate=False)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)

rescaledData = idfModel.transform(featurizedData)
rescaledData.select("features", "label").show(truncate=False)

