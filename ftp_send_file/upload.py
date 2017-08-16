import os
import sys
from ftplib import FTP


ftp = FTP()
# ftp.set_debuglevel(2)e2
ftp.connect(host='10.160.1.123', port=21)
ftp.login()


def get_total_size_local(directory):
    size = 0
    for file_path, directory, filename_list in os.walk(directory):
        for filename in filename_list:
            size += os.path.getsize(os.path.join(file_path, filename))
    return size


class UploadProgress:
    def __init__(self, directory, dst):
        self.directory = directory
        self.current_file = ''
        self.size = get_total_size_local(directory)
        self.seen_so_far = 0.0
        self.dst = dst

    def handle_each_chunk(self, chunk):
        self.seen_so_far += len(chunk)
        sys.stdout.write('\r Uploading: %s, %.2f%%' % (self.current_file, (self.seen_so_far / self.size)*100))
        sys.stdout.flush()
        open(self.dst + self.current_file, 'rb')

    def upload_directory(self):
        for file_path, directory, filename_list in os.walk(self.directory):
            for filename in filename_list:
                local_path = os.path.join(file_path, filename)
                relative_path = os.path.relpath(local_path, self.directory)
                ftp_path = os.path.join(self.dst, relative_path)
                self.current_file = local_path
                dir_list = []
                if '.' not in filename and (not ftp_path in ftp.nlst(self.dst)):
                    dir_list.append(ftp_path)
                dir_name = os.path.dirname(self.dst + filename)
                print
                while dir_name != '/':
                    dir_list.append(dir_name)
                    dir_name = os.path.dirname(dir_name)
                for _dir in dir_list[::-1]:
                    ftp.mkd(_dir)
                try:
                    assert '.' in ftp_path
                    ftp.storbinary('STOR ' + ftp_path, self.handle_each_chunk)

                except Exception as e:
                    self.directory = self.current_file
                    self.upload_directory()

#
download_instance = UploadProgress('/home/yuru/Desktop/folder', '/scratch/yuru')
download_instance.upload_directory()
