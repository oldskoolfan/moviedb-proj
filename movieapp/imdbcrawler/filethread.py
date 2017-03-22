"""
thread class for getting imdb ftp files
"""

import zlib
import threading
from ftplib import FTP

class FileThread(threading.Thread):
    """ thread class for getting data files """

    FTP_POST = 'ftp.fu-berlin.de'
    FTP_DIR = '/pub/misc/movies/database/'

    def __init__(self, f):
        threading.Thread.__init__(self)
        self.file = f
        self.ftp = FTP(self.FTP_POST)
        self.zipObj = zlib.decompressobj(zlib.MAX_WBITS | 16)

        # login to ftp
        self.ftp.login()
        self.ftp.cwd(self.FTP_DIR)

    def run(self):
        retrCmd = 'RETR %s' % self.file['zipname']
        callback = lambda x: self.file['fileobj'].write(self.zipObj.decompress(x))
        self.ftp.retrbinary(retrCmd, callback)
        