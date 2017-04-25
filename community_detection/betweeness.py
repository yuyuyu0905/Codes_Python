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


def graph(x):
    l = len(x)
    matrix = {}
    for i in range(l):
        for j in range(i+1,l):
            if len(x[i].intersection(x[j])) >= 3:
                matrix[(i+1,j+1)]=0
    return matrix


def degree(x,l):
    matrix = [[0]*l for i in range(l)]
    for xx in x:
        matrix[xx[0]-1][xx[1]-1] = 1
        matrix[xx[1]-1][xx[0]-1] = 1

    return map(lambda x:sum(x),matrix)


def build_dict(dic):
    d={}
    for x in dic:
        if x[0] in d:
            d[x[0]].append(x[1])
        else:
            d[x[0]]=[x[1]]
        if x[1] in d:
            d[x[1]].append(x[0])
        else:
            d[x[1]]=[x[0]]
    return d

import random
def bfs_findpath(dic,start_point,end_point):
    print start_point,end_point
    start = [start_point]
    finish = {start_point:True}
    while end_point not in finish:
        if start==[]:break
        temp = []
        for x in start:
            for y in dic[x]:
                if y==end_point:
                    return True
                if y not in finish:
                    finish[y]=True
                    temp.append(y)
        start = temp
        start.sort()

    return end_point in finish


def bfs(start_point):
    print 'running bfs start from: ',start_point
    start=[start_point]
    finish={start_point:True}
    edges={}
    can_go=True
    same_ends={}
    while can_go:
        temp = []
        tempfinish = {}
        for x in start:
            for y in dic[x]:
                if y not in tempfinish and y not in finish:
                    tempfinish[y]=1.0
                elif y in tempfinish and finish[y]:
                    tempfinish[y]+=1
                if y not in finish:
                    edges[(x,y)]=0
                    finish[y]=True
                    temp.append(y)
                    same_ends[y]=[]
                elif y in tempfinish and tempfinish[y]>1:
                    same_ends[y].append((x,y))

        for x in edges:
            if x[1] in tempfinish:
                edges[x]=edges[x]+(1/tempfinish[x[1]])
                y=x[0]
                while y!=start_point:
                    for z in edges:
                        if z[1]==y:
                            edges[z]+=edges[x]
                            y=z[0]
                            break
        edgeslist=edges.keys()
        edgeslist.sort()
        start=temp
        start.sort()
        if temp==[]:can_go = False
    tempedges={}
    tempvalues={}
    for x in same_ends:
        tempedges[x]=[]
        if same_ends[x]:
            for y in edges:
                if y[1]==x:
                    result=edges[y]
                    tempvalues[x]=result
                    break
            for t in same_ends[x]:
                tempedges[x].append(t)
                tt=t[0]
                while tt!=start_point:
                    for z in edges:
                        if z[1]==tt:
                            edges[z]+=result
                            tt=z[0]
                            break
    for x in tempedges:
        for y in tempedges[x]:
            edges[y]=tempvalues[x]
    return edges




def bfs2(start_point):
    print start_point
    start=[start_point]
    finish=[start_point]
    edges={}
    can_go=True
    while can_go:
        temp = []
        tempfinish={}
        for x in start:
            count1=count2=0
            for y in dd[x]:
                if y not in tempfinish and y not in finish:
                    tempfinish[y]=1.0
                elif y in tempfinish and y in finish:
                    tempfinish[y]+=1
                if y not in finish:
                    edges[(x,y)]=0
                    finish.append(y)
                    temp.append(y)
        start=temp
        start.sort()
        if temp==[]:can_go = False
    return finish
    #d.pop(mtuple)
    #dic[mtuple[0]].remove(mtuple[1])
    #dic[mtuple[1]].remove(mtuple[0])

try:
    from collections import Counter
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
sc = SparkContext()
import sys
inputpath=sys.argv[1]
outputpath=sys.argv[2]
lines = sc.textFile(inputpath)
a = lines.map(lambda x:x.split(',')).\
    filter(lambda x:'userId' not in x).map(lambda x:(int(x[0]),int(x[1])))
b=a.coalesce(1).mapPartitions(lambda x:apd(x))
c=b.map(lambda x:set(x[1])).collect()
length_user=len(c)
d=graph(c)
l=degree(d,length_user)
dic=build_dict(d)
ss=sc.parallelize([i for i in range(1,672)]).map(lambda x:Counter(bfs(x))).reduce(lambda a,b:a+b)

count=0
d2={}
ds=dict(ss)
print len(ds)
poplist=[]
for x in ds:
    if x[1]<x[0]:
        if (x[1],x[0]) in ds:
            ds[(x[1],x[0])]=ds[x[1],x[0]]+ds[x]
            poplist.append(x)
        else:
            ds[(x[1],x[0])]=ds[x]
            ds.pop(x)
for x in poplist:
    ds.pop(x)
betweenness={}
for x in ds:
    betweenness[x]=ds[x]
dd=build_dict(ds)#key:i,value:[1,2,3,4]
degree_dict={}
m = len(ds)
l1 = len(bfs(1))
for x in dd:
    degree_dict[x]=len(dd[x])
count=0
status=True

component={}
l2=l1
dss=sorted(ds.iteritems(),key=lambda d:d[1],reverse=True)
file=open(outputpath, 'w')
dswrite=sorted(ds.iteritems(),key=lambda d:d[0])
for x in dswrite:
    file.write('('+str(x[0][0])+','+str(x[0][1])+','+str(x[1]/2)+')'+'\n')
