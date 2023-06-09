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
    ssc = StreamingContext(sc, 1) #data are emitted every second
    
    ssc.checkpoint("~/big-data-repo")

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # generate data
    # map data to the form (name, (date, price))
    googPrice = lines.map(lambda line: ('googPrice', (line.split(" ")[0], line.split(" ")[1])))
    msftPrice = lines.map(lambda line: ('msftPrice',(line.split(" ")[0], line.split(" ")[2])))
    
    # average price with form (name, average price)
    googMA10 = googPrice.map(lambda p: (p[1][1])).reduceByWindow(lambda x, y: float(x) + float(y),lambda x, y: float(x) - float(y), 10, 1).map(lambda x: ('googPrice', float(x)/10))
    msftMA10 = msftPrice.map(lambda p: (p[1][1])).reduceByWindow(lambda x, y: float(x) + float(y),lambda x, y: float(x) - float(y), 10, 1).map(lambda x: ('msftPrice', float(x)/10))
    # join with name to produce average price with form (date, average price)
    goog_date_ma10 = googPrice.join(googMA10).map(lambda x: (x[1][0][0], x[1][1])) 
    msft_date_ma10 = msftPrice.join(msftMA10).map(lambda x: (x[1][0][0], x[1][1]))
    
    
    googMA40 = googPrice.map(lambda p: (p[1][1])).reduceByWindow(lambda x, y: float(x) + float(y),lambda x, y: float(x) - float(y), 40, 1).map(lambda x: ('googPrice', float(x)/40))
    msftMA40 = msftPrice.map(lambda p: (p[1][1])).reduceByWindow(lambda x, y: float(x) + float(y),lambda x, y: float(x) - float(y), 40, 1).map(lambda x: ('msftPrice', float(x)/40))
    
    goog_date_ma40 = googPrice.join(googMA40).map(lambda x: (x[1][0][0], x[1][1]))
    msft_date_ma40 = msftPrice.join(msftMA40).map(lambda x: (x[1][0][0], x[1][1]))
    # streaming with form (date, True/False), True means 10days ma is greater than 40days ma, otherwise False.
    googjoinedStream = goog_date_ma10.join(goog_date_ma40).map(lambda x : (x[0], x[1][0]>x[1][1]))
    msftjoinedStream = msft_date_ma10.join(msft_date_ma40).map(lambda x : (x[0], x[1][0]>x[1][1]))

    state = True
    
    def makedecision(rdd):
        data = rdd.collect()
        global state
        if len(data) !=0:
            if data[0][1] != state and data[0][1]==False: # 10days ma crosses below 40days ma
                state = data[0][1]    
                print (data[0][0], 'sell')
                
            elif data[0][1] != state and data[0][1]==True: # 10days ma crosses above 40days ma
                state = data[0][1]
                print(data[0][0], 'buy')
            
    # googPrice.pprint()
    # msftPrice.pprint()
    # goog_date_ma10.pprint()
    # msft_date_ma10.pprint()
    # goog_date_ma40.pprint()
    # msft_date_ma40.pprint()
    googjoinedStream.pprint()
    # msftjoinedStream.pprint()
    googjoinedStream.foreachRDD(makedecision)
    # msftjoinedStream.foreachRDD(makedecision)
    
    ssc.start()
    ssc.awaitTermination()
