#!/usr/bin/env python3
from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext


conf = SparkConf()
conf.setAppName('PythonStreamingWindowedNetworkWordCount')
conf.setMaster('local[2]') #当使用Local模式启动Spark时，master URL必须为"local[n]"，且"n"的值必须大于"receivers"的数量：
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 10)
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: WindowedNetworkWordCount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    ssc.checkpoint("file:///usr/local/sparkprojects/streaming/socket/checkpoint")
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  . reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()