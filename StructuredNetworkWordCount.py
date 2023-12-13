from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import explode


if __name__ == "__main__":
    spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )
    query = words.writeStream.outputMode(
        "append").format(
        "parquet").option(
        "path","file:///usr/local/sparkprojects/structured_streaming/filesink").option(
        "checkpointLocation","file:///usr/local/sparkprojects/structured_streaming/file-sink-cp").trigger(
        processingTime="8 seconds").start()
    query.awaitTermination()



