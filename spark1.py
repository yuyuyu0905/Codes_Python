try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
import sys
sc = SparkContext()
address=sys.argv[1]
lines=sc.textFile('/Users/Yu/Downloads/ml-latest-small/ratings.csv')
a=lines.map(lambda x:x.split(',')).\
    filter(lambda x:'userId' not in x).map(lambda x:(int(x[1]),float(x[2])))\
    .aggregateByKey((0,0),lambda X,y:(X[0]+y,X[1]+1),lambda X,Y:(X[0]+Y[0],X[1]+Y[1]))\
    .map(lambda (x,(y,z)):(x,y/z))\
    .sortByKey(True)\
    .collect()
file_object = open('/Users/Yu/Desktop/Yu_Ru_task1_small.txt', 'w')
for x in a:
    file_object.write(str(x[0])+','+str(x[1]))
    file_object.write('\n')
file_object.close()

