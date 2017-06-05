import csv
csvfile=file('/Users/Yu/Desktop/result.csv','rb')

csvwrite=file('/Users/Yu/Desktop/result2.csv','wb')
reader=csv.reader(csvfile)
writer=csv.writer(csvwrite)

for x in reader:
    writer.writerow([x[2],x[3],x[4],x[5],(x[6],x[7])])