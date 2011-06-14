#!/usr/bin/env python

from __future__ import with_statement

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

class IOStat:
    
    def __init__(self, prefix = None):
        self.prefix = prefix
        self.n_open = 0
        self.n_close = 0
        self.n_unlink = 0
        self.n_mkdir = 0
        self.n_rmdir = 0
        self.n_rename = 0
        self.n_getxattr = 0
        self.n_statfs = 0
        self.n_create = 0
        self.n_setattr = 0
        self.n_getattr = 0
        self.n_read = 0
        self.n_write = 0

    def __str__(self):
        stat  = """
STORAGE, %s,
open, %d,
close, %d,
unlink, %d,
mkdir, %d,
rmdir, %d,
rename, %d,
getxattr, %d,
statfs, %d,
create, %d,
setattr, %d,
getattr, %d,
read, %d,
write, %d,
        """ % (self.prefix, self.n_open, self.n_close, self.n_unlink, self.n_mkdir, self.n_rmdir, self.n_rename, self.n_getxattr, self.n_statfs, 
               self.n_create, self.n_setattr, self.n_getattr, self.n_read, self.n_write)
        return stat


class Loopback(LoggingMixIn, Operations):    
    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()
        self.iostat = IOStat("NATIVE")
    
    def __call__(self, op, path, *args):
        return super(Loopback, self).__call__(op, self.root + path, *args)
    
    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    # chmod = os.chmod
    def chmod(self, path, mode):
        self.iostat.n_settattr += 1
        return os.chmod(path, mode)

    # chown = os.chown
    def chown(self, path, uid, gid):
        self.iostat.n_setattr += 1
        return os.chown(path, uid, gid)
    
    def create(self, path, mode, fi):
        #fi.fh = os.open(path, os.O_WRONLY | os.O_CREAT, mode)
        self.iostat.n_create += 1
        fi.fh = os.open(path, fi.flags, mode)
        return 0
    
    def flush(self, path, fh):
        self.iostat.n_close += 1
        return os.fsync(fh.fh)

    def fsync(self, path, datasync, fh):
        return os.fsync(fh.fh)
                
    def getattr(self, path, fh=None):
        self.iostat.n_getattr += 1
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
    
    getxattr = None
    
    def link(self, target, source):
        return os.link(source, target)
    
    listxattr = None

    # mkdir = os.mkdir
    def mkdir(self, path, mode):
        self.iostat.n_mkdir += 1
        return os.mkdir(path, mode)

    mknod = os.mknod

    def open(self, path, fi):
        self.iostat.n_open += 1
        fi.fh = os.open(path, fi.flags)
        return 0
        
    def read(self, path, size, offset, fh):
        self.iostat.n_read += 1
        with self.rwlock:
            os.lseek(fh.fh, offset, 0)
            return os.read(fh.fh, size)

    
    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(path)

    readlink = os.readlink
    
    def release(self, path, fh):
        self.iostat.n_close += 1
        return os.close(fh.fh)
        
    def rename(self, old, new):
        self.iostat.n_rename += 1
        return os.rename(old, self.root + new)
    
    #rmdir = os.rmdir
    def rmdir(self, path):
        self.iostat.n_rmdir += 1
        return os.rmdir(path)

    
    def statfs(self, path):
        self.iostat.n_statfs += 1
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))
    
    def symlink(self, target, source):
        return os.symlink(source, target)
    
    def truncate(self, path, length, fh=None):
        with open(path, 'r+') as f:
            f.truncate(length)

    # unlink = os.unlink
    def unlink(self, path):
        self.iostat.n_unlink += 1
        return os.unlink(path)

    # utimens = os.utime
    def utimens(self, path, buf):
        self.iostat.n_setattr += 1
        return os.utime(path, buf)
    
    def write(self, path, data, offset, fh):
        self.iostat.n_write += 1
        with self.rwlock:
            os.lseek(fh.fh, offset, 0)
            return os.write(fh.fh, data)
    

    def destroy(self, private_data):
        print self.iostat

if __name__ == "__main__":
    if len(argv) != 3:
        print 'usage: %s <root> <mountpoint>' % argv[0]
        exit(1)
    #fuse = FUSE(Loopback(argv[1]), argv[2], raw_fi=True, foreground=True, debug=True)
    fuse = FUSE(Loopback(argv[1]), argv[2], raw_fi=True, foreground=True)
