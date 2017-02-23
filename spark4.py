# -*- coding:utf-8 -*-
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
sc = SparkContext()
lines1=sc.textFile('/Users/Yu/Downloads/ml-20m/ratings.csv')
lines2=sc.textFile('/Users/Yu/Downloads/ml-20m/tags.csv')
a=lines1.map(lambda x:x.split(',')).\
    filter(lambda x:'userId' not in x).map(lambda x:(int(x[1]),float(x[2])))
b=lines2.map(lambda x:x.split(',')).\
    filter(lambda x:'userId' not in x).map(lambda x:(int(x[1]),x[2]))
c=b.join(a)
d=c.map(lambda x:x[1]).aggregateByKey((0,0),lambda X,y:(X[0]+y,X[1]+1),lambda X,Y:(X[0]+Y[0],X[1]+Y[1]))\
    .map(lambda (x,(y,z)):(x,y/z)).sortByKey(False).collect()

csv_object=open('/Users/Yu/Desktop/Yu_Ru_task2_big.csv', 'w')
csv_object.write('tags'+','+'ratings'+'\n')
for cc in d:
    csv_object.write(cc[0].encode('utf-8')+','+str(cc[1]))
    csv_object.write('\n')
