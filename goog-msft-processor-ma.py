#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 22:19:07 2023

@author: yuanyuan
"""
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: goog-msft-processor-ma.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="PythonStreamingGoogMsftprice")
    ssc = StreamingContext(sc, 1)
    
    ssc.checkpoint("~/big-data-repo")

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    googPrice = lines.map(lambda line: ('googPrice', (line.split(" ")[0], line.split(" ")[1])))
    msftPrice = lines.map(lambda line: ('msftPrice',(line.split(" ")[0], line.split(" ")[2])))
    
    
    googMA10 = googPrice.map(lambda p: (p[1][1])).reduceByWindow(lambda x, y: float(x) + float(y),lambda x, y: float(x) - float(y), 10, 1).map(lambda x: ('googPrice', float(x)/10))
    msftMA10 = msftPrice.map(lambda p: (p[1][1])).reduceByWindow(lambda x, y: float(x) + float(y),lambda x, y: float(x) - float(y), 10, 1).map(lambda x: ('msftPrice', float(x)/10))
    
    goog_date_ma10 = googPrice.join(googMA10).map(lambda x: (x[1][0][0], x[1][1]))
    msft_date_ma10 = msftPrice.join(msftMA10).map(lambda x: (x[1][0][0], x[1][1]))
    
    
    googMA40 = googPrice.map(lambda p: (p[1][1])).reduceByWindow(lambda x, y: float(x) + float(y),lambda x, y: float(x) - float(y), 40, 1).map(lambda x: ('googPrice', float(x)/40))
    msftMA40 = msftPrice.map(lambda p: (p[1][1])).reduceByWindow(lambda x, y: float(x) + float(y),lambda x, y: float(x) - float(y), 40, 1).map(lambda x: ('msftPrice', float(x)/40))
    
    goog_date_ma40 = googPrice.join(googMA40).map(lambda x: (x[1][0][0], x[1][1]))
    msft_date_ma40 = msftPrice.join(msftMA40).map(lambda x: (x[1][0][0], x[1][1]))
    
    googjoinedStream = goog_date_ma10.join(goog_date_ma40).map(lambda x : (x[0], x[1][0]>x[1][1]))
    msftjoinedStream = msft_date_ma10.join(msft_date_ma40).map(lambda x : (x[0], x[1][0]>x[1][1]))

    # googPrice.pprint()
    # msftPrice.pprint()
    # goog_date_ma10.pprint()
    # msft_date_ma10.pprint()
    # goog_date_ma40.pprint()
    # msft_date_ma40.pprint()
    googjoinedStream.pprint()
    msftjoinedStream.pprint()
    
    ssc.start()
    ssc.awaitTermination()