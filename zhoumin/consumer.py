from kafka import KafkaConsumer, TopicPartition


address = ('localhost', 4000)

consumer = KafkaConsumer(bootstrap_servers='10.160.0.168:9092')
topic_set = consumer.topics()
topic_set = map(lambda x: TopicPartition(x, 0), topic_set)

consumer.assign(list(topic_set))


def recent_logs(topic_partition, log_numbers=2000):
    current_offset = consumer.position(topic_partition)
    print current_offset
    if current_offset < 2000:
        current_offset = 2000
    consumer.seek(topic_partition, current_offset - 2000)
    x = consumer.poll(timeout_ms=20000, max_records=log_numbers)
    for xx in x:
        result_list = x[xx]
    result_list = map(lambda x: x.value, result_list)
    return result_list


def run_consumer():
    for x in consumer:
        print 'receiving'
        print x.value, x.offset, x.partition

