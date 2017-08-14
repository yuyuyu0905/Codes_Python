import boto3
import os
import sys
import threading


s3_resource = boto3.resource('s3')
s3 = boto3.client('s3')


class DownloadPercentage(object):

    def __init__(self, client, bucket, filename, current_seen_so_far, total_size):
        self._filename = filename
        self._size = client.head_object(Bucket=bucket, Key=filename)['ContentLength']
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._seen_so_far_current = current_seen_so_far
        self._total = total_size

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        self._seen_so_far += bytes_amount
        self._seen_so_far_current += bytes_amount
        percentage = (float(self._seen_so_far) / self._size) * 100
        percentage_total = (float(self._seen_so_far_current) / self._total) * 100
        sys.stdout.write(
            "\rDownloading: %s  %s / %s  (%.2f%%) total: %s / %s  (%.2f%%)" % (
                self._filename, self._seen_so_far, self._size,
                percentage, self._seen_so_far_current, self._total, percentage_total))
        sys.stdout.flush()


def onprogress(client, bucket, src, last_size, total_size):
    DownloadPercentage(client, bucket, src, last_size, total_size)
    return


def oncomplete(src):
    print "Download "+src+" completed."


def onerror(err):
    print "Error"
    print err


def download(bucket, src, dst):
    if src[-1] != '/':
        src += '/'
    try:
        # assert dst is local folder
        assert os.path.exists(dst)
        # assert bucket is s3 bucket
        assert s3.head_bucket(Bucket=bucket)
    except Exception as es:
        err = 'error:' + type(es).__name__ + ' ' + es.message
        onerror(err)
        return
    threading.Thread(target=_download, args=(bucket, src, dst)).start()


def total_bytes_s3(bucket, src, resource=boto3.resource('s3')):
    bucket = resource.Bucket(bucket)
    size = 0.0
    for obj in bucket.objects.filter(Prefix=src):
        size += obj.size
    return size


def _download(bucket_name, src, dst, resource=boto3.resource('s3')):
    total_size = total_bytes_s3(bucket_name, src)
    print total_size
    local_length = 0
    last_size = 0.0
    bucket = resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=src):
        local_path = os.path.join(dst, obj.key)
        if os.path.exists(local_path):
            local_length = os.path.getsize(local_path)
        try:
            assert os.path.exists(local_path)
            s3_length = obj.size
            assert local_length == s3_length
            print 's3 path:   ', obj.key, 'size:', obj.size
            print 'local path:', local_path, 'size:', local_length
            print 'Size indicates file', obj.key, 'exists'
            print 'Aborting'
            print ''
        except:
            download_single_file(bucket_name, obj.key, local_path, last_size, total_size)
        last_size += obj.size
    oncomplete(src)


def download_single_file(bucket, src, local_path, last_size, total_size, client=boto3.client('s3')):
    try:
        dir_name = os.path.dirname(local_path)
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        s3.download_file(bucket, src, local_path,
                         Callback=onprogress(client, bucket, src, last_size, total_size))
    except Exception as es:
        err = 'error: ' + type(es).__name__ + ' ' + es.message + ' local:' +\
              local_path + ' bucket:' + bucket + ' s3_path' + src
        onerror(err)


# test usage
# download('bucket-name', 'S3 path', 'local path')
download('tusimple-us2cn', 'folder/', '/home/yuru/Desktop/')
