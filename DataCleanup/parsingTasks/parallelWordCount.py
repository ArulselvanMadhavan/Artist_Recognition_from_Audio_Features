from __future__ import print_function
__author__ = 'arul'

import sys
from operator import add

from pyspark import SparkContext

class test(object):

    def __init__(self,filePath):
        self.file = filePath

    def test(self):
        stringRDD = sc.textFile(self.file)
        return stringRDD.flatMap(lambda x:x.split('\n'))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    testObj = test(sys.argv[1])
    s = []
    s.le
    # lines = sc.textFile(sys.argv[1], 1)
    print(testObj.test().collect())
    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #               .map(test) \
    #               .reduceByKey(add)
    # output = counts.collect()
    # for (word, count) in output:
    #     print("%s: %i" % (word, count))
    sc.stop()
