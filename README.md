# MovingAverage-Trading-pysparkDStreams
This small project applys the specific MovingAverage (MA) strategy that is to apply two moving averages to a chart: one longer (40-day) and one shorter (10-day). When the shorter-term MA crosses above the longer-term MA, it's a buy signal, as it indicates that the trend is shifting up. This is known as a golden cross. Meanwhile, when the shorter-term MA crosses below the longer-term MA, it's a sell signal, as it indicates that the trend is shifting down. This is known as a dead/death cross.

The pyspark Discretized Streams (DStreams) code is tested in a GCP cluster and on a configuration with 1 master and 0 workers.

DStreams are generated by the code from https://github.com/singhj/big-data-repo/blob/main/spark-examples/goog-msft-prices.py which produces stock prices for Google (Symbol: goog) and Microsoft (Symbol: msft) and pipes it, line by line for the past 3 years. Each line looks like this: 2020-03-10 64.01950073242188 64.01950073242188. Trading day’s data are emitted every second.

The generated Data streams are feeded into pyspark using netcat on the master machine of a spark cluster:

The sending terminal with command: ~/yourfoldername/spark-examples/goog-msft-prices.py | nc -lk 9999 (The code file 'goog-msft-prices.py' is provided by the link mentioned above)\
The Receiving terminal with command: `which spark-submit` ~/yourfoldername/goog-msft-processor-ma.py localhost 9999 (The code file goog-msft-processor-ma.py is created by me with pyspark data stream)\

The result streams are the buy/sell recommendations for each stock in the form: [(date, buy symbol), (date, sell symbol), etc].
