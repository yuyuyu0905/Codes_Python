#append all movies rated by one user together


def apd (x):
    x=list(set(x))
    d={}
    for xx in x:
        if xx[0] in d:
            d[xx[0]].append(xx[1])
        else:
            d[xx[0]]=[xx[1]]
    i=0
    for s in d:
        d[s].sort()
        x[i]=[s,d[s]]
        i+=1

    return x[:i]

def builddict(x,thr):
    x=list(x)
    le=len(x)
    d={}
    for xx in x:
        for xxx in xx:
            if xxx in d:
                d[xxx]+=1
            else:
                d[xxx]=1
    l=[]
    for s in d:
        l.append((s,d[s],le/thr))
    return l

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
import sys
from itertools import combinations
sc = SparkContext()
select=sys.argv[1]
threshold=sys.argv[3]
#if select==1:
#    lines = sc.textFile('/Users/Yu/Downloads/Assignment2/Data/movies.small1.csv')
#else:
#    lines = sc.textFile('/Users/Yu/Downloads/Assignment2/Data/movies.small2.csv')
address=sys.argv[2]
lines = sc.textFile(address)
select=int(sys.argv[1])
if select==1:aa,bb=1,0
elif select == 2: aa,bb=0,1
threshold=int(sys.argv[3])
if threshold>2000:discount=0.8
else:discount=0.2
start=lines.map(lambda x:x.split(',')).\
    filter(lambda x: 'userID' not in x and 'userId' not in x)
#single
a=start.map(lambda x: (int(x[bb]),int(x[aa])))\
    .mapPartitions(lambda x:apd(x)).map(lambda x:list(combinations(x[1],1)))
l=float(a.count())
one=a.repartition(2).mapPartitions(lambda x: builddict(x,l/threshold))\
    .filter(lambda x:x[1]>=x[2]*discount-1)
temp=one.repartition(1).map(lambda x:(x[0],x[1]))\
    .aggregateByKey(0, lambda x, y: x + y, lambda x, y: x + y)\
    .filter(lambda x:x[1]>=threshold).sortByKey(True).map(lambda x:(x[0][0],x[1]))
single=temp.map(lambda x:x[0]).collect()

#join prepared
joi=start.map(lambda x: (int(x[bb]),int(x[aa])))\
    .filter(lambda x:x[1] in single)
#double

a=joi.mapPartitions(lambda x:apd(x)).map(lambda x:list(combinations(x[1],2)))
one=a.repartition(2).mapPartitions(lambda x: builddict(x,l/threshold))\
    .filter(lambda x:x[1]>=x[2]*discount-1)
temp=one.repartition(1).map(lambda x:(x[0],x[1]))\
    .aggregateByKey(0, lambda x, y: x + y, lambda x, y: x + y)\
    .filter(lambda x:x[1]>=threshold).sortByKey(True)
double=temp.map(lambda x:x[0]).collect()

#triple
file_object = open('/Users/Yu/Desktop/Yu_Ru_result.txt', 'w')
file_object.write('('+(str(single[0]))+')')
for x in single[1:]:
    file_object.write(', ('+(str(x))+')')
file_object.write('\n\n')
file_object.write(str(double)[1:-1])
file_object.write('\n\n')

i=3
while (True):
    j = temp.flatMap(lambda x: x[0]).map(lambda x: (x, 1)).mapPartitions(lambda x: apd(x)). \
        map(lambda x: x[0]).collect()
    joi = joi.filter(lambda x: x[1] in j)
    a = joi.mapPartitions(lambda x: apd(x)).map(lambda x: list(combinations(x[1], i)))
    one = a.repartition(2).mapPartitions(lambda x: builddict(x, l / threshold)) \
        .filter(lambda x: x[1] >= x[2] * discount - 1)
    temp = one.repartition(1).map(lambda x: (x[0], x[1])) \
        .aggregateByKey(0, lambda x, y: x + y, lambda x, y: x + y) \
        .filter(lambda x: x[1] >= threshold).sortByKey(True)
    triple = temp.map(lambda x: x[0]).collect()
    if triple!=[]:
        file_object.write(str(triple)[1:-1])
        file_object.write('\n\n')
        i+=1
    else:break



