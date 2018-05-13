#!/usr/bin/python
r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: flume_wordcount.py <hostname> <port>
 To run this on your local machine, you need to setup Flume first, see
 https://flume.apache.org/documentation.html
 and then run the example
    `$ bin/spark-submit --jars \
      external/flume-assembly/target/scala-*/spark-streaming-flume-assembly-*.jar \
      examples/src/main/python/streaming/flume_wordcount.py \
      localhost 12345
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: flume_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingFlumeWordCount")
    ssc = StreamingContext(sc, 20)

    hostname, port = sys.argv[1:]
    kvs = FlumeUtils.createStream(ssc, hostname, int(port))
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
