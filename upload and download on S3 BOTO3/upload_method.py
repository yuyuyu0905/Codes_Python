import os
import sys
import threading
import boto3


s3 = boto3.client('s3')


class UploadPercentage(object):

    def __init__(self, filename, current_seen_so_far, total_size):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0.0
        self._seen_so_far_current = float(current_seen_so_far)
        self._total = total_size
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            self._seen_so_far_current += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            total_percentage = (self._seen_so_far_current / self._total) * 100
            # -------------------------------------
            # |Modify what you need to trace here |
            # -------------------------------------
            sys.stdout.write(
                "\rUploading %s  %s / %s  (%.2f%%), total: %.2f / %.2f (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage, self._seen_so_far_current, self._total, total_percentage))
            sys.stdout.flush()


def onprogress(local_file, current_size, total_size):
    return UploadPercentage(local_file, current_size, total_size)


def oncomplete(src):
    print "Upload "+src+" completed."


def onerror(err):
    print "Error"
    print err


def upload(src, bucket, dst):
    if dst[0] == '/':
        dst = dst[1:]
    try:
        # assert src is local folder
        assert os.path.exists(src)
        # assert dst is s3 bucket
        assert s3.head_bucket(Bucket=bucket)
    except Exception as es:
        err = 'error:' + type(es).__name__ + ' ' + es.message
        onerror(err)
        return

    # upload
    # _upload(src, bucket, dst)
    threading.Thread(target=_upload, args=[src, bucket, dst]).start()


def total_bytes(src):
    size = 0
    for file_path, directories, file_list in os.walk(src):
        for _file in file_list:
            size += os.path.getsize(os.path.join(file_path, _file))
    return size


def _upload(src, bucket, dst):
    total_size = total_bytes(src)
    local_length = 0
    last_size = 0
    for file_path, directories, file_list in os.walk(src):
        for _file in file_list:
            last_size += local_length
            # check integrity of each file on s3
            local_path = os.path.join(file_path, _file)
            relative_path = os.path.relpath(local_path, src)
            s3_path = os.path.join(dst, relative_path)
            local_length = float(os.path.getsize(local_path))
            try:
                head_length = s3.head_object(Bucket=bucket, Key=s3_path)['ContentLength']
                assert head_length == local_length
                print 's3 path:   ', s3_path, 'size:', head_length
                print 'local path:', local_path, 'size:', local_length
                print 'Size indicates file', _file, 'exists'
                print 'Aborting'
                print ''
            except:
                upload_single_file(local_path, bucket, s3_path, last_size, total_size)
    oncomplete(src)


def upload_single_file(local_file, bucket, s3_path, last_size, total_size):
    try:
        s3.upload_file(local_file, bucket, s3_path, Callback=onprogress(local_file, last_size, total_size))
    except Exception as es:
        err = 'error:' + type(es).__name__ + es.message + 'local:' +\
              local_file + 'bucket:' + bucket + 's3_path' + s3_path
        onerror(err)

# usage: upload('local path to the directory', 's3 bucket', 's3 bucket path')

