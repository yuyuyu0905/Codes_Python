try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
sc = SparkContext()
lines1=sc.textFile('/Users/Yu/Downloads/ml-latest-small/ratings.csv')
lines2=sc.textFile('/Users/Yu/Downloads/ml-latest-small/tags.csv')
a=lines1.map(lambda x:x.split(',')).\
    filter(lambda x:'userId' not in x).map(lambda x:(int(x[1]),float(x[2])))
b=lines2.map(lambda x:x.split(',')).\
    filter(lambda x:'userId' not in x).map(lambda x:(int(x[1]),x[2]))
c=b.join(a)
d=c.map(lambda x:x[1]).aggregateByKey((0,0),lambda X,y:(X[0]+y,X[1]+1),lambda X,Y:(X[0]+Y[0],X[1]+Y[1]))\
    .map(lambda (x,(y,z)):(x,y/z)).sortByKey(False).collect()
import csv
csv_object=file('/Users/Yu/Desktop/Yu_Ru_task2_small.csv', 'w')
writer=csv.writer(csv_object)
writer.writerow(['tags','ratings'])
for cc in d:
    writer.writerow(cc)
