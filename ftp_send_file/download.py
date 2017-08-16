import os, sys
from ftplib import FTP


ftp = FTP()
# ftp.set_debuglevel(2)
ftp.connect(host='10.160.1.123', port=21)
ftp.login()


def get_total_size(directory):
    size = 0
    for filename in ftp.nlst(directory):
        try:
            ftp.cwd(filename)
            size += get_total_size(filename)
        except:
            ftp.voidcmd('TYPE I')
            size += ftp.size(filename)
    return size


class DownloadProgress:
    def __init__(self, directory, dst):
        self.directory = directory
        self.current_file = ''
        self.size = get_total_size(directory)
        self.seen_so_far = 0.0
        self.dst = dst

    def handle_each_chunk(self, chunk):
        self.seen_so_far += len(chunk)
        sys.stdout.write('\r Downloading: %s, %.2f%%' % (self.current_file, (self.seen_so_far / self.size)*100))
        sys.stdout.flush()
        open(self.dst + self.current_file, 'wb').write(chunk)

    def download_directory(self):
        for filename in ftp.nlst(self.directory):
            self.current_file = filename
            dir_list = []
            if '.' not in filename and (not os.path.exists(self.dst + filename)):
                dir_list.append(self.dst + filename)
            dir_name = os.path.dirname(self.dst + filename)
            while not os.path.exists(dir_name):
                dir_list.append(dir_name)
                dir_name = os.path.dirname(dir_name)
            for dir in dir_list[::-1]:
                os.mkdir(dir)
            try:
                assert '.' in filename
                ftp.retrbinary('RETR ' + filename, self.handle_each_chunk)

            except Exception as e:
                self.directory = self.current_file
                self.download_directory()


download_instance = DownloadProgress('/datasets/v2/2017-03-04-08-34-20/', '/home/yuru/Desktop')
download_instance.download_directory()
