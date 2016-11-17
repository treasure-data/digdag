import sys

if len(sys.argv) != 3:
    print "Usage: spark-submit --files <file> %s <file> <result-destination>" % (sys.argv[0], )
    print
    print "Example:"
    print "  spark-submit --files query.sql %s query.sql s3://my-bucket/results/output.csv" % (sys.argv[0], )
    print
    sys.exit(1)

query_file = sys.argv[1]
result_uri = sys.argv[2]

from pyspark.sql import SparkSession

with open(query_file) as f:
    query = f.read()

spark = SparkSession\
    .builder\
    .appName("spark-sql")\
    .getOrCreate()

print >> sys.stderr, 'Running query: %s' % (query_file, )
result = spark.sql(query)

print >> sys.stderr, 'Writing result: %s' % (result_uri, )
result.write.csv(result_uri)

spark.stop()
