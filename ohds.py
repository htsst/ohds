#!/usr/bin/env python

from __future__ import with_statement

import os
import socket
import shutil
import stat
import time
#import memcache

from os.path import realpath
from sys import argv, exit
from threading import Lock

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class DataStore:

    def __init__(self):
        self.store = {}

    def get(self, path):
        print "GET %s " % path
        return self.store.get(path)

    def set(self, path, md):
        # self.store.set(path, metadata)
        print "SET %s, " % path,
        self.store[path] = md

    def delete(self, path):
        self.store.pop(path)

#class Memcached:
#
#    def __init__(self):
#        self.store = memcache.Client(['172.16.14.129:11211'], debug=1)
#        self.store.flush_all()
#
#    def get(self, path):
#        print "GET %s " % path
#        return self.store.get(path)
#
#    def set(self, path, md):
#        print "SET %s, " % path,
#        print md
#        self.store.set(path, md)
#
#    def delete(self, path):
#        self.store.delete(path)


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
        

class SecondaryFS:

    def __init__(self, mnt):
        self.mnt = os.path.abspath(mnt)
        print self.mnt
        
        self.iostat = IOStat("SecondaryFS")

    def path(self, path):
        _path = None
        if os.path.isabs(path):
            _path = self.mnt  + path
        else:
            _path = os.path.join(self.mnt, path)
        return os.path.normpath(_path)

    def listdir(self, path):
        _path = self.path(path)
        self.iostat.n_statfs += 1
        if os.path.isdir(_path):
            ### FIXME: How many stat operations ocuur here?            
            listdir = os.listdir(_path)
            self.iostat.n_statfs += len(listdir)
            return listdir
        else:
            return []

    def getmd(self, path):
        basepath = self.path(path)
        self.iostat.n_statfs += 1
        stat = os.lstat(basepath)
        md = dict((key, getattr(stat, key)) 
                  for key in ('st_atime', 'st_ctime',
                              'st_gid', 'st_mode',
                              'st_mtime', 'st_nlink',
                              'st_size', 'st_uid'))
        ### doubt
        md['children'] = self.listdir(path)
        md['locations'] = []
            
        return md

    def readlink(self, path):
        _path = self.path(path)
        return os.readlink(_path)

    def rmdir(self, path):
        _path = self.path(path)
        self.iostat.n_rmdir += 1
        return os.rmdir(_path)

    def symlink(self, path, source):
        localpath = self.path(path)
        os.symlink(source, localpath)
        
    def unlink(self, path):
        _path = self.path(path)
        self.iostat.n_unlink += 1
        os.unlink(_path)

class Scratch:

    def __init__(self, hostname, scratch):
        self.hostname = hostname

        self.scratch = os.path.realpath(scratch)
        if os.path.exists(self.scratch):
            shutil.rmtree(self.scratch)
        os.makedirs(self.scratch)

        self.rwlock = Lock()

        self.iostat = IOStat(hostname)

    def cache(self, src, dst, secondary = None):
        _dst = self.path(dst)
        dir = os.path.dirname(_dst)
        self.iostat.n_statfs += 1
        if not os.path.exists(dir):
            self.iostat.n_mkdir +=1
            os.makedirs(dir)

        # shutil.copy2(src, _dst)

        try:
            secondary.iostat.n_open += 1
            src_fd = os.open(src, os.O_RDONLY)
            self.iostat.n_open += 1
            dst_fd = os.open(_dst, os.O_WRONLY | os.O_CREAT)

            while 1:
                secondary.iostat.n_read += 1
                buf = os.read(src_fd, 1048576)
                if len(buf) == 0:
                    break
                self.iostat.n_write += 1
                os.write(dst_fd, buf)
        finally:
            secondary.iostat.n_close +=1
            os.close(src_fd)
            self.iostat.n_close += 1
            os.close(dst_fd)

            secondary.iostat.n_statfs += 1
            st = os.stat(src)
            mode = stat.S_IMODE(st.st_mode)

            os.utime(_dst, (st.st_atime, st.st_mtime))
            os.chmod(_dst, mode)

        return self.getmd(dst)

    def makedirs(self, path):
        _path = self.path(path)
        self.iostat.n_mkdir += 1
        os.makedirs(_path)

    def close(self, fd):
        self.iostat.n_close += 1
        return os.close(fd)

    def exists(self, path):
        _path = self.path(path)
        self.iostat.n_statfs += 1
        return os.path.exists(_path)

    def flush(self, fd):
        self.iostat.n_close += 1
        return os.close(os.dup(fd))

    def fsync(self, fd):
        return os.fsync(fd)

    def getmd(self, path):
        _path = self.path(path)
        self.iostat.n_statfs += 1
        stat = os.lstat(_path)
        md = dict((key, getattr(stat, key)) 
                  for key in ('st_atime', 'st_ctime',
                              'st_gid', 'st_mode',
                              'st_mtime', 'st_nlink',
                              'st_size', 'st_uid'))
        md['children'] = []
        md['locations'] = []
        return md        

    def mkdir(self, path, mode):
        _path = self.path(path)
        self.iostat.n_mkdir += 1
        os.mkdir(_path, mode)

    def open(self, path, flags, mode=0777):
        _path = self.path(path)
        self.iostat.n_open += 1
        return os.open(_path, flags, mode)

    def path(self, path):
        return self.scratch + path

    def read(self, size, offset, fd):
        with self.rwlock:
            os.lseek(fd, offset, 0)
            self.iostat.n_read += 1
            return os.read(fd,size)

    def rename(self, old, new):
        _old, _new = self.path(old), self.path(new)
        return os.rename(old, new)

    def rmdir(self, path):
        _path = self.path(path)
        self.iostat.n_rmdir += 1
        os.rmdir(_path)

    def truncate(self, path, length, fh=None):
        _path = self.path(path)
        self.iostat.n_write += 1
        with open(_path, 'r+') as f:
            f.truncate(length)

    def unlink(self, path):
        _path = self.path(path)
        self.iostat.n_unlink += 1
        return os.unlink(_path)

    def write(self, data, offset, fd):
        with self.rwlock:
            os.lseek(fd, offset, 0)
            self.iostat.n_write += 1
            return os.write(fd, data)

class MDS:

    def __init__(self, base, config, ds):
        # data store
        self.ds = ds

        # secondary
        self.secondary = SecondaryFS(base)

        # scratches
        self.scratches = {}
        for c in config:
            hostname, path = c[0], c[1]
            self.scratches[hostname]= Scratch(hostname, path)

        self.iostat = IOStat("MDS")

    def __extract_dirinfo(self, path, dir=False):
        parent = os.path.split(path)[0]
        st = self.ds.get(parent)
        
        st['children'].remove(path)
        if dir == True:
            st['st_nlink'] -= 1

        self.ds.set(parent, st)

    def __insert_dirinfo(self, path, dir=False):
        if path == '/':
            return 

        parent = os.path.dirname(path)
        assert parent != None

        if not self.exists(parent):
            _md = self.secondary.getmd(parent)
            assert _md != None
            self.regmd(parent, _md)

        md = self.ds.get(parent)
        assert md != None

        md['children'].append(path)
        if dir == True:
            md['st_nlink'] += 1

        self.ds.set(parent, md)
        
    def chmod(self, path, mode):
        md = self.getmd(path)
        md['st_mode'] &= 0770000
        md['st_mode'] |= mode
        self.setmd(path, md)
        return 0

    def chown(self, path, uid, gid):
        md = self.getmd(path)
        md['st_uid'], md['st_gid'] = uid, gid
        self.setmd(path, md)

    def exists(self, path):
        self.iostat.n_statfs += 1
        if not self.ds.get(path) == None:
            return True
        return False

    def children(self, path):
        md = self.ds.get(path)
        assert md != None
        return md['children']

    def getmd(self, path):
        self.iostat.n_statfs += 1
        return self.ds.get(path)

    def increment_size(self, path, size):
        md = self.getmd(path)
        assert md != None
        md['st_size'] += size
        self.setmd(path, md)

    def locations(self, path):
        md = self.ds.get(path)
        assert md['locations'] != None
        
        scratches = []
        for loc in md['locations']:
            scratches.append(self.scratches[loc])
        return scratches

    def mkmd(self, path, st_mode, st_nlink, locations= [], dir=False):
        now = time.time()
        md = dict(st_mode=st_mode, st_nlink=st_nlink, st_size=0,
                  st_ctime=now, st_mtime=now, st_atime=now,
                  st_uid=os.getuid(), st_gid=os.getgid(), 
                  children = [], locations = locations)
        self.ds.set(path, md) 

        md = self.getmd('/')
        if not path == '/':
            self.__insert_dirinfo(path, dir)

        md = self.getmd('/')

        return

    def regmd(self, path, md):
        self.ds.set(path, md)
        self.__insert_dirinfo(path)

    def rmmd(self, path, dir=False):
        self.ds.delete(path)
        self.__extract_dirinfo(path, dir)

    def rename(self, old, new):
        md = self.getmd(old)
        self.mds.regmd(new, md)

    def schedule(self, path):
        location = list(self.scratches)[0]
        return self.scratches[location]

    def setmd(self, path, md):
        self.ds.set(path, md)

    def truncate_size(self, path, size):
        md = self.getmd(path)
        md['st_size'] = size
        self.setmd(path, md)

class OHDS(LoggingMixIn, Operations):

    def __init__(self, base, config):

        ## self.spool = LocalSpool(local_spool)
        
        self.rwlock = Lock()

        ds = DataStore()
        #ds = Memcached()
        self.mds = MDS(base, config, ds)
        self.secondary = self.mds.secondary

        ## first cache
        #if self.mds.getmd('/') == None:
        if not self.mds.exists('/'):
            self.mds.mkmd('/', (stat.S_IFDIR | 0755), 2)

        self.open_files = {}

#    def access(self, path, mode):
#        localpath = self.localpath(path)
#        if not self.mds.has_key(path) or not os.access(localpath, mode):
#            raise FuseOSError(EACCES)

    def chmod(self, path, mode):
        return self.mds.chmod(path, mode)

    def chown(self, path, uid, gid):
        return self.mds.chown(path, uid, gid)
    
    def create(self, path, mode, fi):
        ## TODO: Scheduling
        scr = self.mds.schedule(path)
        fd = scr.open(path, fi.flags, mode) 
        fi.fh  = fd
        self.open_files[fd] = scr

        loc = scr.hostname
        self.mds.mkmd(path, (stat.S_IFREG | mode), 1, locations = [loc])

        return 0

    def destroy(self, private_data):
        print self.mds.iostat

        print self.secondary.iostat

        for scr in self.mds.scratches.values():
            print scr.iostat

    def flush(self, path, fh):
        scr = self.open_files[fh.fh]
        scr.flush(fh.fh)
        #del self.open_files[fh.fh]

    def fsync(self, path, datasync, fh):
        scr = self.open_files[fh.fh]
        return scr.fsync(fh.fh)

    def getattr(self, path, fh=None):
        if self.mds.exists(path): # under odfs
            return self.mds.getmd(path)
        else: # out of odfs ( should cache the metadata at open)
            return self.secondary.getmd(path)

    def mkdir(self, path, mode):
        ## TODO: Scheduling
        scr = self.mds.schedule(path)
        scr.mkdir(path, mode)
        self.mds.mkmd(path, (stat.S_IFDIR | mode), 2, dir=True)
        
    def open(self, path, fi):
        ## TODO: Scheduling
        scr = self.mds.schedule(path)

        if not self.mds.exists(path):
            _path = self.secondary.path(path)
            md = scr.cache(_path, path, self.secondary)
            md['locations'].append(scr.hostname)
            self.mds.regmd(path, md)

        fd = scr.open(path, fi.flags)
        fi.fh = fd
        self.open_files[fd] = scr

        return 0

    def opendir(self, path):
        ## TODO: Scheduling
        if not self.mds.exists(path):
            scr = self.mds.schedule(path)
            if not scr.exists(path):
                basepath = self.secondary.path(path)
                scr.makedirs(path)

            md = self.secondary.getmd(path)
            self.mds.regmd(path, md)

        return 0

    def read(self, path, size, offset, fh):
        with self.rwlock:
            scr = self.open_files[fh.fh]
            return scr.read(size, offset, fh.fh)

    def readdir(self, path, fh):
        files = []

        _path = self.secondary.path(path)
        if os.path.exists(_path):
            dirs = os.listdir(_path)
            self.secondary.iostat.n_statfs += len(dirs)
            files += dirs

        for child in self.mds.children(path):
            file = os.path.split(child)[1]
            files.append(file)

        files = list(set(files))
        return ['.', '..' ] + files

    def readlink(self, path):
        return self.secondary.readlink(path)

    getxattr=None
#    def getxattr(self, path, name, position=0):
#        if self.mds.has_key(path):
#            st = self.mds.get(path)
#            st.get('attrs', {})
#            try:
#                return attrs[name]
#            except KeyError:
#                return '' # should return 

    def release(self, path, fh):
        scr = self.open_files[fh.fh]
        scr.close(fh.fh)
        del self.open_files[fh.fh]

    def rename(self, old, new):
        # FIXME
        # for scrs in self.mds.locations(old):
        # scr.rename(old, new)
        self.mds.rename(old, new)

    def rmdir(self, path):
        if not self.mds.exists(path):
            ## FIXME?
            self.secondary.rmdir(path)
        else:
            for scr in self.mds.locations(path):
                scr.rmdir(path)
            self.mds.rmmd(path, True)

    def statfs(self, path):
        # FIXME?
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, path, source):
        # FIXME
        _source = self.secondary.path(source)
        rv = self.secondary.symlink(path, _source)
        md = self.secondary.getmd(path)
        #md['st_mode'] = (S_IFLINK | 07777)
        ## ?? regmd
        self.mds.regmd(path, md)
        return rv

    def truncate(self, path, length, fh=None):
        scr = self.mds.schedule(path)
        scr.truncate(path, length, fh)
        self.mds.truncate_size(path, length)

    def unlink(self, path):
        if not self.mds.exists(path):
            # FIXME?
            self.secondary.unlink(path)
        else:
            for scr in self.mds.locations(path):
                scr.unlink(path)

            self.mds.rmmd(path)

    def write(self, path, data, offset, fh):
        with self.rwlock:
            scr = self.open_files[fh.fh]
            size = scr.write(data, offset, fh.fh)
            self.mds.increment_size(path, size)
            return size

if __name__ == "__main__":

    if len(argv) != 3:
        print 'usage: %s <root> <mountpoint>' % argv[0]
        exit(1)
        
    hostname = socket.gethostname()
    config = [(hostname, "/tmp/ohds")]
    fuse = FUSE(OHDS(argv[1], config), argv[2], raw_fi=True, foreground=True)
