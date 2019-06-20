import argparse
from pyspark import SparkContext, rdd
from pyspark.streaming import StreamingContext

import time
import os

def count(x):
    print(x.count())

if __name__ == "__main__":
    sc = SparkContext()

    ssc = StreamingContext(sc, 3)
    ssc.textFileStream(path).foreachRDD(count)
    ssc.start()
    ssc.awaitTermination()
