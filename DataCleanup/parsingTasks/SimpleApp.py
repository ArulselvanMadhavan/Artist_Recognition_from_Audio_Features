__author__ = 'arul'

"""SimpleApp.py"""
from pyspark import SparkContext

logFile = "/home/arul/cs6240/wordcount/input/hw1.txt"  # Should be some file on your system
sc = SparkContext("Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))