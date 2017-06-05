import csv
import time


csvfile=file('/Users/Yu/Desktop/result.csv','rb')
csvwrite=file('/Users/Yu/Desktop/result3.csv','wb')
reader=csv.reader(csvfile)
writer=csv.writer(csvwrite)

l=[]
for x in reader:
    l+=[x]
l=l[1:]
writer.writerow(['REPORTID','USERID','REPORTTYPEID','ADDEDBY','HOSTID','LATITUDE','LONGTITUDE','RADIUSSIMPACT'
                ,'DESCRIPTION','PRICE','REPORTSTATUSID','DATE','TIME','CREATEDTIME','LASTMODIFIEDTIME','ADDRESS','TOTALNUMBERINJURED','TOTALNUMBERKILLED','VEHICLETYPECODE'])
l=map(lambda x:x[:3]+x[3].split(' ',1)+x[4:],l)
l=map(lambda x:[x[2],' ',' ',' ',' ',x[7],x[8],' ',' ',' ',' ',x[3],x[4],' ',' ',time.strftime('%d/%m/%Y %H:%M:%S',time.localtime(time.time())),' ',' ',x[5]],l)
writer.writerows(l)