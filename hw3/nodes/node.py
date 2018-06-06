#!/usr/bin/env python

from __future__ import print_function, absolute_import, division
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
import socket
import threading
import sys
import os
import json
from hashlib import md5, sha1
import my_memory

############## GLOBAL VARIABLES ###################
memory = None

ip = '127.0.0.1'
port = 10000

bootstrap_ip = '127.0.0.1'
bootstrap_port = 9000

special_id = None


def listen():
    global port
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.bind(('0.0.0.0', port))
    try:
        while True:
            connection.listen()

            conn, addr = connection.accept()
            t = threading.Thread(target=handle_requests, args=(conn, addr))
            t.daemon = True
            t.start()
    except:
        pass
    connection.close()

def handle_requests(conn, addr):
    print(conn)
    print(addr)

    while True:
        data = conn.recv(40000)
        if not data:
            break;

        data = data.decode('utf-8')
        print(data)

        command_length = 0
        extra = command_and_extra = None

        try:
            command_length = data.split(' ', 1)[0]
            command_length = int(command_length)
            data = data[:command_length]

            command_length, command_and_extra = data.split(' ', 1)
            command_length = int(command_length)
            
            command_and_extra = command_and_extra.split(' ', 1)
            if(len(command_and_extra) > 1):
                extra = command_and_extra[1]
            command_and_extra = command_and_extra[0]

            if (command_and_extra.split('_', 1)[0] == 'BOOT'):
                res = handle_boot_commands(conn, addr, command_and_extra, extra)
                if res == 'break':
                    break
            elif (command_and_extra.split('_', 1)[0] == 'CLIE'):
                res = handle_client_commands(conn, addr, command_and_extra, extra)
                if res == 'break':
                    break
            else:
                my_return(conn, 'NODE_INVALIDINPUT')
                break

        except ValueError:
            my_return(conn, 'NODE_INVALIDINPUT')
            break

    conn.close()

def handle_boot_commands(conn, addr, command, body):
    return

def handle_client_commands(conn, addr, command, body):
    global memory
    # read and write files
    if command == 'CLIE_READFILE':
        handle_read_commands(conn, body)
        return 'break'
    elif command == 'CLIE_GETFILEATTR':
        handle_get_file_attr(conn, body)
        return 'break'
    elif command == 'CLIE_WRITEFILE':
        handle_write_commands(conn, body)
        return 'break'
    elif command == 'CLIE_RENAMEFILE':
        handle_rename_file(conn, body)
        return 'break'
    elif command == 'CLIE_RMFILE':
        handle_remove_file(conn, body)
        return 'break'
    else:
        my_return(conn, 'NODE_INVALIDINPUT')
        return 'break'

        # do something with reading files.
        # uh so what am i supposed to do...........
        # i am not really sure
        # hmmmmmmm

def handle_remove_file(conn, body):
    # do remove file
    try:
        path = body.strip()
        memory.unlink(path)
        my_return(conn, 'NODE_RMFILESUCCESS')
    except:
        my_return(conn, 'NODE_INVALIDREAD')

    return 'break'

def handle_get_file_attr(conn, body):
    global memory

    try:
        path = body.strip()
        _data = memory.getattr(path)
        _data = json.dumps(_data)
        my_return(conn, 'NODE_GETATTRSUCCESS %s' % (_data))
    except:
        my_return(conn, 'NODE_INVALIDREAD')
    return 'break'

def handle_read_commands(conn, body):
    global memory
    # read file return file data
    # raw bytes
    try:
        print(body)
        path, size, offset, fh = body.split(' ', 4)
        size = int(size)
        offset = int(offset)
        fh = int(fh)
        _read = memory.read(path, size, offset, fh)
        my_return(conn, 'NODE_READREPLY' + ' ' +  _read.decode('utf-8'))
    except Exception as e:
        print(e)
        my_return(conn, 'NODE_INVALIDREAD')
    
    return 'break'

def handle_write_commands(conn, body):
    global memory
    try:
        path, offset, fh, data = body.split(' ', 4)
        offset = int(offset)
        fh = int(fh)
        data = encode(data)
        _len_written = memory.write(path, data, offset, fh)
        my_return(conn, 'NODE_WRITESUCCESS %d'% (_len_written))
    except:
        my_return(conn, 'NODE_WRITEERROR')
    
    return 'break'

def handle_rename_file(conn, body):
    global memory, special_id
    try:
        old, new = body.split(' ', 1)
        memory.rename(old, new)
        my_return(conn, 'NODE_MVSUCCESS')
    except:
        my_return(conn, 'NODE_MVERROR')
    return 'break'
            

def create_bootstrap_conn():
    global bootstrap_ip, bootstrap_port
    return socket.create_connection((bootstrap_ip, bootstrap_port))

def special_ret(conn, normal_str, byte_str):
    _temp = encode( '%d %s ' % ((len(normal_str) + len(byte_str)) + 1)) + byte_str
    print(_temp)
    conn.send(_temp)
    

def my_return(conn, string):
    print(ret_str(string))
    conn.send(ret_str(string))

def my_md5(string):
    return md5(encode(string)).hexdigest()

def ret_str(ret):
    _str = str(len(ret) + len(str(len(ret))) + 1) + ' ' + ret
    return encode(_str)

def encode(string):
    return string.encode('utf-8')

def handle_keyboard_args():
    global ip, ports, memory
    while True:
        arguments = input('>>>> ')
        if arguments == '/close':
            bootstrap = my_memory.Bootstrap()
            sock = bootstrap.connect()
            _msg = '%s %s %d' % ('NODE_LETMELEAVE', ip, port)
            _msg = bootstrap.ret_str(_msg)
            sock.sendall(_msg)
            sys.exit(0)
        else:
            print('LIFE IS HARD\ncommands as follows\n\t/close\n')



if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('usage: %s <openport> <mountpoint>' % sys.argv[0])
        exit(1)

    port = int(sys.argv[1])

    try:
        bootstrap_port = int(sys.argv[3])
    except:
        pass

    bootstrap = my_memory.Bootstrap()
    sock = bootstrap.connect()
    _msg = '%s %s %d' % ('NODE_LETMEJOIN', ip, port)
    _msg = bootstrap.ret_str(_msg)
    sock.sendall(_msg)

    _data = sock.recv(4096)
    _data = _data.decode('utf-8')
    if 'BOOT_NODEEXISTS' in _data:
        print('NODE EXISTS. Cannot connect another of the same node')
        exit(1)
    elif 'BOOT_SUCCESSNODEJOIN' in _data:
        special_id = _data.split()[2]
    else:
        print('IDK WHAT THIS RETURN IS', _data)
        exit(1)

    memory = my_memory.Memory(special_id, bootstrap_port)

    t = threading.Thread(target=listen, args=())
    t.daemon = True
    t.start()
    print('\n\n<<<<<<<<<<<<<<<<<<<<<<<<<here>>>>>>>>>>>>>>>>>>\n\n')
    p = threading.Thread(target=handle_keyboard_args, args=())
    p.daemon = True
    p.start()    

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(memory, sys.argv[2], foreground=True)

def initiate_fuse(memory, mount):
    print('\n\ninitiating\n\n')
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(memory, mount, foreground=True)