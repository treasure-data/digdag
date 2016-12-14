import sys
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("hello")\
    .getOrCreate()

print "Hello " + " ".join(sys.argv[1:]) + "! " + "${session_time}"

spark.stop()
