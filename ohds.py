#!/usr/bin/env python

from __future__ import with_statement

import os
import shutil
import stat
import time
# import memcache

from os.path import realpath
from sys import argv, exit
from threading import Lock

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class DataStore:

    def __init__(self):
        self.store = {}

    def get(self, path):
        return self.store.get(path)

    def set(self, path, metadata):
        # self.store.set(path, metadata)
        self.store[path] = metadata

    def delete(self, path):
        self.store.pop(path)

#class Memcachd:

#    def __init__(self):
#        self.store = memcache.Client(['172.16.14.129:11211'], debug=1)
#        self.store.flush_all()

#    def get(self, path):
#        self.store.get(path)
#
#    def set(self, path, metadata):
#        self.store.set(path, metadata)
#
#    def delete(self, path):
#        self.store.delete(path)

class SecondaryFS:

    def __init__(self, mnt):
        self.mnt = os.path.abspath(mnt)
        print self.mnt

    def path(self, path):
        _path = None
        if os.path.isabs(path):
            _path = self.mnt  + path
        else:
            _path = os.path.join(self.mnt, path)
        return os.path.normpath(_path)

    def listdir(self, path):
        basepath = self.path(path)
        if os.path.isdir(basepath):
            return os.listdir(basepath)
        else:
            return []

    def getmd(self, path):
        basepath = self.path(path)
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

    def symlink(self, path, source):
        localpath = self.path(path)
        os.symlink(source, localpath)
        
    def unlink(self, path):
        _path = self.path(path)
        os.unlink(_path)

class Scratch:

    def __init__(self, hostname, scratch):
        self.hostname = hostname
        self.scratch = os.path.realpath(scratch)

        if os.path.exists(self.scratch):
            shutil.rmtree(self.scratch)
        os.makedirs(self.scratch)

    def cache(self, src, dst):
        _dst = self.path(dst)
        dir = os.path.dirname(_dst)
        if not os.path.exists(dir):
            os.makedirs(dir)
        shutil.copy2(src, _dst)
        return self.getmd(dst)

    def cachetree(self, src, dst):
        _dst = self.path(dst)
        shutil.copytree(src, _dst)
        return self.getmd(dst)

    def close(self, fd):
        return os.close(fd)

    def exists(self, path):
        _path = self.path(path)
        return os.path.exists(_path)

    def flush(self, fd):
        return os.close(os.dup(fd))

    def fsync(self, fd):
        return os.fsync(fd)

    def getmd(self, path):
        _path = self.path(path)
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
        os.mkdir(_path, mode)

    def open(self, path, flags, mode=0777):
        _path = self.path(path)
        return os.open(_path, flags, mode)

    def path(self, path):
        return self.scratch + path

    def read(self, size, offset, fd):
        try:
            os.lseek(fd, offset, 0)
        except:
            print "lseek"
            
        rv = 0
        try:
            rv = os.read(fd,size)
        except:
            print "read, fd %s, size %s" % (fd, size)
        return rv

    def rename(self, old, new):
        _old, _new = self.path(old), self.path(new)
        return os.rename(old, new)

    def rmdir(self, path):
        _path = self.path(path)
        os.rmdir(_path)

    def truncate(self, path, length, fh=None):
        _path = self.path(path)
        with open(_path, 'r+') as f:
            f.truncate(length)

    def unlink(self, path):
        _path = self.path(path)
        return os.unlink(_path)

    def write(self, data, offset, fd):
        os.lseek(fd, offset, 0)
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
        
    def __extract_dirinfo(self, path, dir=False):
        parent = os.path.split(path)[0]
        st = self.ds.get(parent)
        
        st['children'].remove(path)
        if dir == True:
            st['st_nlink'] -= 1

        self.ds.set(parent, st)

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


    def rmmd(self, path, dir=False):
        self.ds.delete(path)
        self.__extract_dirinfo(path, dir)

    def regmd(self, path, md):
        self.ds.set(path, md)
        self.__insert_dirinfo(path)

    def schedule(self, path):
        return self.scratches['d0']

    def setmd(self, path, md):
        self.ds.set(path, md)

    def getmd(self, path):
        return self.ds.get(path)

    def getscr(self, location):
        return self.scratches[location]

    def getscrs(self, locations):
        scratches = []
        for location in locations:
            scratches.append(self.scratches[location])
        return scratches

    def exists(self, path):
        if not self.ds.get(path) == None:
            return True
        return False

    def _exists(self, path):
        md = self.ds.get(path)
        if md == None:
            return False

        if location==None:
            return True
        else:
            return location in md['locations']
        
    def children(self, path):
        md = self.ds.get(path)
        return md['children']


class OHDS(LoggingMixIn, Operations):

    def __init__(self, base, config):

        ## self.spool = LocalSpool(local_spool)
        
        self.rwlock = Lock()

        ds = DataStore()
        self.mds = MDS(base, config, ds)
        self.secondary = self.mds.secondary

        ## first cache
        if self.mds.getmd('/') == None:
            self.mds.mkmd('/', (stat.S_IFDIR | 0755), 2)

        self.open_files = {}

    def __getscr(self):
        ## TENTATIVE
        return self.mds.getscr("d0")

#    def access(self, path, mode):
#        localpath = self.localpath(path)
#        if not self.mds.has_key(path) or not os.access(localpath, mode):
#            raise FuseOSError(EACCES)

    def chmod(self, path, mode):
        md = self.mds.getmd(path)
        md['st_mode'] &= 0770000
        md['st_mode'] |= mode
        self.mds.setmd(path, md)
        return 0

    def chown(self, path, uid, gid):
        md = self.mds.getmd(path)
        md['st_uid'] = uid
        md['st_gid'] = gid
        self.mds.setmd(path, md)
    
    def create(self, path, mode, fi):
        ## TODO: Scheduling
        scr = self.mds.schedule(path)
        fd = scr.open(path, fi.flags, mode) 
        fi.fh  = fd
        self.open_files[fd] = scr

        loc = scr.hostname
        self.mds.mkmd(path, (stat.S_IFREG | mode), 1, locations = [loc])

        return 0

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
        scr = self.__getscr()
        scr.mkdir(path, mode)
        self.mds.mkmd(path, (stat.S_IFDIR | mode), 2, dir=True)
        
    def open(self, path, fi):
        ## TODO: Scheduling
        scr = self.mds.schedule(path)

        if not self.mds.exists(path):
            _path = self.secondary.path(path)
            md = scr.cache(_path, path)
            md['locations'].append(scr.hostname)
            self.mds.regmd(path, md)

        fd = scr.open(path, fi.flags)
        fi.fh = fd
        self.open_files[fd] = scr

        return 0

    def opendir(self, path):
        ## TODO: Scheduling
        if not self.mds.exists(path):
            ## FIXME
            scr = self.mds.getscr("d0")
            if not scr.exists(path):
                basepath = self.secondary.path(path)
                scr.cachetree(basepath, path)

            md = self.secondary.getmd(path)
            self.mds.regmd(path, md)

        return 0

    def read(self, path, size, offset, fh):
        scr = self.open_files[fh.fh]
        with self.rwlock:
            return scr.read(size, offset, fh.fh)

    def readdir(self, path, fh):
        files = []

        _path = self.secondary.path(path)
        if os.path.exists(_path):
            files += os.listdir(_path)

        for child in self.mds.children(path):
            file = os.path.split(child)[1]
            files.append(file)

        files = list(set(files))
        return ['.', '..' ] + files

    def readlink(self, path):
        basepath = self.secondary.path(path)
        return os.readlink(basepath)

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
        md = self.mds.getmd(old)
        assert md != None
        #scrs = self.mds.getscrs(md['locations'])
        #for scr in scrs:
        #scr.rename(old, new)

        self.mds.regmd(new, md)

    def rmdir(self, path):
        if not self.mds.exists(path):
            print "cannot remove %s" % path

        self.mds.rmmd(path, True)

        scr = self.__getscr()
        scr.rmdir(path)

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, path, source):
        _source = self.secondary.path(source)
        rv = self.secondary.symlink(path, _source)
        md = self.secondary.getmd(path)
        #md['st_mode'] = (S_IFLINK | 07777)
        self.mds.setmd(path, md)
        return rv

    def truncate(self, path, length, fh=None):
        scr = self.__getscr()
        scr.truncate(path, length, fh)

    def unlink(self, path):
        if not self.mds.exists(path):
            ## raise exception
            #print "cannot remove %s" % path
            self.secondary.unlink(path)
        else:
            md = self.mds.getmd(path)
            scrs = self.mds.getscrs(md['locations'])
            for scr in scrs:
                scr.unlink(path)

            self.mds.rmmd(path)


    def write(self, path, data, offset, fh):
        scr = self.open_files[fh.fh]
        with self.rwlock:
            size = scr.write(data, offset, fh.fh)
            md = self.mds.getmd(path)
            md['st_size'] += size
            self.mds.setmd(path, md)
            return size

if __name__ == "__main__":

    if len(argv) != 3:
        print 'usage: %s <root> <mountpoint>' % argv[0]
        exit(1)

    config = [("d0", "/tmp/ohds")]
    fuse = FUSE(OHDS(argv[1], config), argv[2], raw_fi=True, foreground=True)
