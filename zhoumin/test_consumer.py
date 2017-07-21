from consumer import *

for xx in topic_set:
    print '--------------------topic-------------------'
    print '--------------------', xx, '-------------------'
    logs_list = recent_logs(xx)
    for x in logs_list:
        print x
    print '-----------------------end------------------'
