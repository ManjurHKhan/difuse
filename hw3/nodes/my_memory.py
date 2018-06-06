#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging
import os
import socket
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import json
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, node_id, bootstrap_port = 8000):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        self.node_id = node_id
        self.bootstrap_port = bootstrap_port
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)

    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0o770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
        print('========== creating a new file ===========')
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        
        bootstrap = Bootstrap(self.bootstrap_port)
        sock = bootstrap.connect()
        try:
            _msg = '%s %s %s' % ('CLIE_ADDFILES', self.node_id, path)
            _msg = bootstrap.ret_str(_msg)
            sock.sendall(_msg)
            sock.close()
        except:
            pass
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        print('++++++++++ getattr +++++++++++')
        to_ret = None
        bootstrap = Bootstrap(self.bootstrap_port)
        nodes = None
        if path not in self.files:
            print(' ======= File does not exist ======')
            sock = bootstrap.connect()
            try:
                print('\n\n here \n\n')
                _msg = '%s %s' % ('CLIE_FILELOCATION', path)
                _msg = bootstrap.ret_str(_msg)
                sock.sendall(_msg)

                _data = sock.recv(4096)
                _data = _data.decode('utf-8')
                sock.close()
                if 'BOOT_FILELOCATION' in _data:
                    print('\n\n>>>>>>>>>>>>>>>>..here<<<<<<<<<<<<<<<<<\n\n')
                    _data_len = _data.split(' ', 1)[0]
                    _data_len = int(_data_len)
                    _data = _data[:_data_len]
                    _data = _data.split()
                    
                    _ip = _data[2]
                    _port = int(_data[3])
                    nodes = Nodes(_ip, _port)
                    # connecting to node for file
                    sock = nodes.connect()

                    try:
                        _msg = '%s %s' % ('CLIE_GETFILEATTR', path)
                        _msg = bootstrap.ret_str(_msg)
                        sock.sendall(_msg)

                        _data = sock.recv(4096)
                        _data = _data.decode('utf-8')

                        if 'NODE_GETATTRSUCCESS' in _data:
                            _data_len = _data.split(' ', 1)[0]
                            _data_len = int(_data_len)
                            _data = _data[:_data_len]

                            _data = _data.split(' ', 2)[2]
                            print('\n>>>>>>>>Found pre data <<<<<<<<<<<\n')
                            print(_data)
                            # to_ret = _data
                            to_ret = dict(json.loads(_data))
                            return to_ret
                            # print('\n>>>>>>>>Found data <<<<<<<<<<<\n')
                            # print(to_ret)
                    except:
                        print("\n\n++++++++++ node server not found +++++++++ ip = %s port = %d\n\n" % (_ip, _port))
                        to_ret = None
            except:
                print("\n\n++++++++++ node server not found +++++++++\n\n")
                to_ret = None

            if to_ret == None:
                raise FuseOSError(ENOENT)
            return to_ret
        _data = self.files[path]
        print(_data)
        return _data

    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        print('======= OPEN FILE ============')
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        print('============ READ FILE ==========')
        print(path, size, offset)
        bootstrap = Bootstrap(self.bootstrap_port)
        nodes = None
        to_ret = None
        if path not in self.files:
            print(' ======= File does not exist ======')
            sock = bootstrap.connect()
            try:
                _msg = '%s %s' % ('CLIE_FILELOCATION', path)
                _msg = bootstrap.ret_str(_msg)
                sock.sendall(_msg)
                print('file location asking')
                _data = sock.recv(4096)
                _data = _data.decode('utf-8')
                print('file location asking')
                sock.close()
                if 'BOOT_FILELOCATION' in _data:
                    print('file location found???')
                    _data_len = _data.split(' ', 1)[0]
                    _data_len = int(_data_len)
                    _data = _data[:_data_len]
                    print('file location found???')
                    _data = _data.split()
                    
                    _ip = _data[2]
                    _port = int(_data[3])
                    print('file location found???')
                    nodes = Nodes(_ip, _port)
                    # connecting to node for file
                    sock = nodes.connect()
                    print('file location found???')
                    try:
                        _msg = '%s %s %d %d %d' % ('CLIE_READFILE', path, size, offset, fh)
                        _msg = bootstrap.ret_str(_msg)
                        print('file location found???')
                        sock.sendall(_msg)

                        _data = sock.recv(4096)
                        _data = _data.decode('utf-8')
                        print('file location found???')
                        print(_data)
                        sock.close()
                        if 'NODE_READREPLY' in _data:
                            _data_len = _data.split(' ', 1)[0]
                            _data_len = int(_data_len)
                            _data = _data[:_data_len]
                            print('readreply file location found???')
                            print(_data)
                            _data = _data.split(' ', 2)[2]
                            to_ret =  _data
                    except Exception as e:
                        print(e)
                        to_ret = None
            except Exception as e:
                print(e)
                to_ret = None

            if(to_ret == None):
                return bootstrap.encode('')
                
            return bootstrap.encode(to_ret)


        data = self.data[path][offset:offset + size]
        data = str(data).encode('utf-8')
        print(data)
        return data

    def readdir(self, path, fh):
        print('========= READDIR ==========')
        bootstrap = Bootstrap(self.bootstrap_port)
        sock = bootstrap.connect()
        _data = []
        try:
            _msg = '%s' % ('CLIE_LISTOFFILES')
            _msg = bootstrap.ret_str(_msg)
            sock.sendall(_msg)

            _data = sock.recv(4096)
            _data = _data.decode('utf-8')
            _data_len = _data.split(' ', 1)[0]
            _data_len = int(_data_len)
            _data = _data[:_data_len]
            print('\n\n', _data, '\n\n')
            _data = _data.split()[2:]

        except:
            _data = []
        return ['.', '..'] + [x[1:] for x in _data if x != '/']

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        print(old, new)
        print('===== Renaming file ======')
        bootstrap = Bootstrap(self.bootstrap_port)
        if old not in self.files:
            sock = bootstrap.connect()
            try:
                _msg = '%s %s' % ('CLIE_FILELOCATION', old)
                _msg = bootstrap.ret_str(_msg)
                sock.sendall(_msg)

                _data = sock.recv(4096)
                _data = _data.decode('utf-8')
                sock.close()
                if 'BOOT_FILELOCATION' in _data:
                    _data_len = _data.split(' ', 1)[0]
                    _data_len = int(_data_len)
                    _data = _data[:_data_len]

                    _data = _data.split()
                    _ip = _data[2]
                    _port = int(_data[3])
                    nodes = Nodes(_ip, _port)
                    # connecting to node for file
                    sock = nodes.connect()

                    try:
                        _msg = '%s %s %s' % ('CLIE_RENAMEFILE', old, new)
                        _msg = bootstrap.ret_str(_msg)
                        sock.sendall(_msg)
                        sock.close()
                    except:
                        pass
            except:
                pass
        else:
            print('-------------- renaiming -------------')
            print(self.files[old])
            print(self.data[old])
            self.files[new] = self.files.pop(old)
            print(self.files[new])
            self.data[new] = self.data[old]
            self.files[new]['st_size'] = len(self.data[new])
            # self.files[path]['st_size'] = len(self.data[path])
            sock = bootstrap.connect()
            try:
                _msg = '%s %s %s' % ('CLIE_RENAMEFILE', old, new)
                _msg = bootstrap.ret_str(_msg)
                sock.sendall(_msg)
                sock.close()
            except:
                pass
        

    def rmdir(self, path):
        
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))

        self.data[target] = source

    def truncate(self, path, length, fh=None):
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length

    def unlink(self, path):
        print('======= removing ??? =======')
        bootstrap = Bootstrap(self.bootstrap_port)
        if path not in self.files:
            sock = bootstrap.connect()
            try:
                _msg = '%s %s' % ('CLIE_FILELOCATION', path)
                _msg = bootstrap.ret_str(_msg)
                sock.sendall(_msg)

                _data = sock.recv(4096)
                _data = _data.decode('utf-8')
                sock.close()
                if 'BOOT_FILELOCATION' in _data:
                    _data_len = _data.split(' ', 1)[0]
                    _data_len = int(_data_len)
                    _data = _data[:_data_len]

                    _data = _data.split()
                    _ip = _data[2]
                    _port = int(_data[3])
                    nodes = Nodes(_ip, _port)
                    # connecting to node for file
                    sock = nodes.connect()
                    try:
                        _msg = '%s %s' % ('CLIE_RMFILE', path)
                        _msg = bootstrap.ret_str(_msg)
                        sock.sendall(_msg)
                        sock.close()
                        return
                    except:
                        return
            except:
                return
        else:
            self.files.pop(path)
            sock = bootstrap.connect()
            try:
                _msg = '%s %s' % ('CLIE_REMOVEFILE', path)
                _msg = bootstrap.ret_str(_msg)
                sock.sendall(_msg)
                sock.close()
            except:
                return

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        print(path, data, offset, fh)
        print('======= WRITE ===========')
        if not path in self.files:
            sock = bootstrap.connect()
            try:
                _msg = '%s %s' % ('CLIE_FILELOCATION', path)
                _msg = bootstrap.ret_str(_msg)
                sock.sendall(_msg)

                _data = sock.recv(4096)
                _data = _data.decode('utf-8')
                sock.close()
                if 'BOOT_FILELOCATION' in _data:
                    _data_len = _data.split(' ', 1)[0]
                    _data_len = int(_data_len)
                    _data = _data[:_data_len]

                    _data = _data.split()
                    _ip = _data[2]
                    _port = int(_data[3])
                    nodes = Nodes(_ip, _port)
                    # connecting to node for file
                    sock = nodes.connect()
                    try:
                        _msg = '%s %s, %d, %d, %s' % ('CLIE_WRITEFILE', path, offset, fh, data.decode('utf-8'))
                        _msg = bootstrap.ret_str(_msg)
                        sock.sendall(_msg)
                        
                        _data = sock.recv(4096)
                        _data = _data.decode('utf-8')
                        if 'NODE_WRITESUCCESS' in _data:
                            _data_len = _data.split(' ', 1)[0]
                            _data_len = int(_data_len)
                            _data = _data[:_data_len]

                            _data = _data.split()[2]
                            return _data
                        return 0
                    except:
                        return 0

            except:
                return 0
        else:
            data = data.decode('utf-8')
            self.data[path] = self.data[path][:offset] + data
            self.files[path]['st_size'] = len(self.data[path])
            return len(data)


class Bootstrap:
    """docstring for Bootstrap"""
    def __init__(self, port = 8000):
        self.ip = '127.0.0.1'
        self.port = port

    def connect(self):
        return socket.create_connection((self.ip, self.port))

    def ret_str(self, ret):
        _str = str(len(ret) + len(str(len(ret))) + 1) + ' ' + ret
        return self.encode(_str)

    def encode(self, string):
        return string.encode('utf-8')
        
class Nodes:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

    def connect(self):
        return socket.create_connection((self.ip, self.port))

    def ret_str(self, ret):
        _str = str(len(ret) + len(str(len(ret))) + 1) + ' ' + ret
        return self.encode(_str)

    def encode(self, string):
        return string.encode('utf-8')



if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(None), argv[1], foreground=True)
