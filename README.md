# MovingAverage-Trading-pysparkDStreams
This experimental implement uses strategy that is to apply two moving averages to a chart: one longer and one shorter. \
When the shorter-term MA crosses above the longer-term MA, it's a buy signal, as it indicates that the trend is shifting up. \
This is known as a golden cross. Meanwhile, when the shorter-term MA crosses below the longer-term MA, it's a sell signal, \
as it indicates that the trend is shifting down. This is known as a dead/death cross.
