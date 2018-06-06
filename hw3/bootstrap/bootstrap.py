#!/usr/bin/env python

import socket
import threading
import sys
from hashlib import md5, sha1
'''
node = {
    'node_location' : '0.0.0.0',
    'node_port' : '12335'
    'node_name' : 'MyFirstNode',
}

nodes = ('0.0.0.0', '1.1.1.1',)
file = {
    'node_location' : '0.0.0.0',
    'file_name' : 'MyFILE'
}
files = (file1, file2, file3, )

'''

nodes = {}
files = {}


######### GLOBAL VARIABLES FROM BOOTSTRAP ##########

INVALID_INPUT = 'BOOT_INVALIDINPUT'


############ GLOBAL VARIABLES FROM NODES ###########

LETMEJOIN = 'NODE_LETMEJOIN'
LETMELEAVE = 'NODE_LETMELEAVE'


########## GLOBAL VARIABLES FROM CLIENTS ###########



def listen(port):
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
    global nodes, files, INVALID_INPUT

    print('>>> New Connection was made <<<<')
    print(addr)

    while True:
        data = conn.recv(40000)
        if not data:
            break;
        print(data)
        data = data.decode('utf-8')
        command_length = 0
        command_and_extra = None
        extra = None

        try:
            command_length, command_and_extra = data.split(' ', 1)
            command_length = int(command_length)
            command_and_extra = command_and_extra.split(' ', 1)
            if (len(command_and_extra) > 1):
                extra = command_and_extra[1]
            command_and_extra = command_and_extra[0]
            command_length = command_length - len(command_and_extra) - 1
            if(command_and_extra.split('_', 1)[0] == 'NODE'):
                # this is a node server trying to perform an action
                res = handle_node_commands(conn, addr, command_and_extra, extra)
                if (res == 'break'):
                    break
            elif(command_and_extra.split('_', 1)[0] == 'CLIE'):
                res = handle_client_commands(conn, command_and_extra, extra)
                if (res == 'break'):
                    break
            else:
                my_return(conn, INVALID_INPUT)
                break

        except ValueError:
            my_return(conn, INVALID_INPUT)
            break

    conn.close()

def handle_client_commands(conn, command, body):
    global nodes, files, INVALID_INPUT
    if (command == 'CLIE_LISTOFFILES'):
        my_return(conn, 'BOOT_LISTOFFILES' + ' ' + list_to_str(files.keys()))
        return 'break'
    elif (command == 'CLIE_DEADNODE'):
        _ip = port = None
        try:
            _ip, port = body.split()
            if ip == '' or port == '':
                raise Exception('invalid input')
        except:
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        _node = (_ip, int(port))
        if check_if_node_exists(_node):
            remove_node_and_files(_node)
        
        my_return(conn, 'BOOT_ACK')
        return 'break'
    elif (command == 'CLIE_FILELOCATION'):
        _file = body.strip()
        if _file == '' or _file == None:
            (conn, 'BOOT_INVALIDINPUT')
            'break'
        _file_hash = _file
        if not file_exists(_file_hash):
            my_return(conn, 'BOOT_NOFILEFOUND')
            return 'break'
        _ip, _port = nodes[files[_file_hash]]
        my_return(conn, 'BOOT_FILELOCATION %s %d' % (_ip, _port))
        return 'break'
    elif (command == 'CLIE_RENAMEFILE'):
        _f_old = _f_new = None
        try:
            _f_old, _f_new = body.split()
        except:
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        
        _f_old_hash = _f_old
        _f_new_hash = _f_new
        
        if not file_exists(_f_old_hash):
            my_return(conn, 'BOOT_RENAMEFAILED')
            return 'break'
        
        if _f_old_hash != _f_new_hash:
            _hash = files.pop(_f_old_hash)
            files[_f_new_hash] = _hash
        
        my_return(conn, 'BOOT_RENAMESUCCESS')
        return 'break'
    elif (command == 'CLIE_ADDFILES'):
        l_files = body.split()
        _hash = l_files[0]
        l_files = l_files[1:]
        # _conflict_file = ''
        for _file in l_files:
            _file = _file.strip()
            if(_file == ''):
                pass
            _f_hash = _file
            # if (_f_hash in files):
            #     _conflict_file += ' ' + _file
            #     pass
            # else:
            files[_f_hash] = _hash

        # if _conflict_file != '':
        #     print('File conflicted', _conflict_file)
        #     my_return(conn, 'BOOT_ADDFILECONFLICT' + _conflict_file)
        #     return 'break'
        my_return(conn, 'BOOT_ADDFILESUCCESS')
        return 'break'
    elif (command == 'CLIE_REMOVEFILE'):
        _file = None
        try:
            _file = body.strip()
            if _file == '':
                raise Exception('FILE NAME IS NULL')
        except:
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        
        _file_hash = _file
        if not file_exists(_file_hash):
            my_return(conn, 'BOOT_RMFAILNOFILE')
            return 'break'
        files.pop(_file_hash)
        my_return(conn, 'BOOT_RMSUCCESS')
        return 'break'
    else:
        my_return(conn, 'BOOT_INVALIDINPUT')
        return 'break'



def list_to_str(L):
    _str = ''.join(' {}'.format(key) for key in L)
    return _str.strip()

def handle_node_commands(conn, addr, command, body):
    global nodes, files, INVALID_INPUT
    if (command == 'NODE_LETMEJOIN'):
        _ip, _port = body.split()
        _port = int(_port)
        addr = (_ip, _port)
        if check_if_node_exists(addr):
            my_return(conn, 'BOOT_NODEEXISTS')
            return 'break'
        _hash = add_new_node(addr)
        my_return(conn, 'BOOT_SUCCESSNODEJOIN %s' % (_hash))
        return 'break'
    elif (command == 'NODE_LETMELEAVE'):
        _ip, _port = body.split()
        _port = int(_port)
        addr = (_ip, _port)
        if not check_if_node_exists(addr):
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        remove_node_and_files(addr)
        my_return(conn, 'BOOT_GOODBYENODE')
        return 'break'
    elif (command == 'NODE_ADDFILES'):
        if not check_if_node_exists(addr):
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        l_files = body.split()
        _conflict_file = ''
        for _file in l_files:
            _file = _file.strip()
            if(_file == ''):
                pass
            _f_hash = _file
            _hash = my_md5(str(addr))
            if (_f_hash in files):
                _conflict_file += ' ' + _file
                pass
            else:
                files[_f_hash] = _hash

        if _conflict_file != '':
            print('File conflicted', _conflict_file)
            my_return(conn, 'BOOT_ADDFILECONFLICT' + _conflict_file)
            return 'break'
        my_return(conn, 'BOOT_ADDFILESUCCESS')
        return 'break'
    elif (command == 'NODE_RENAMEFILE'):
        if not check_if_node_exists(addr):
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        _f_old = _f_new = None
        try:
            _f_old, _f_new = body.split()
        except:
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        _f_old_hash = _f_old
        _f_new_hash = _f_new
        
        if not file_exists(_f_old_hash):
            my_return(conn, 'BOOT_RENAMEFAILED')
            return 'break'
        
        if _f_old_hash != _f_new_hash:
            _hash = files.pop(_f_old_hash)
            files[_f_new_hash] = _hash
        
        my_return(conn, 'BOOT_RENAMESUCCESS')
        return 'break'
    
    elif (command == 'NODE_REMOVEFILE'):
        if not check_if_node_exists(addr):
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        _file = None
        try:
            _file = body.strip()
            if _file == '':
                raise Exception('FILE NAME IS NULL')
        except:
            my_return(conn, 'BOOT_INVALIDINPUT')
            return 'break'
        
        _file_hash = _file
        if not file_exists(_file_hash):
            my_return(conn, 'BOOT_RMFAILNOFILE')
            return 'break'
        files.pop(_file_hash)
        my_return(conn, 'BOOT_RMSUCCESS')
        return 'break'
    else:
        my_return(conn, 'BOOT_INVALIDINPUT')
        return 'break'

def add_new_node(addr):
    global nodes, files, INVALID_INPUT
    _hash = my_md5(str(addr))
    nodes[_hash] = addr
    return _hash

def check_if_node_exists(addr):
    _hash = my_md5(str(addr))
    return _hash in nodes

def file_exists(name):
    global nodes, files, INVALID_INPUT
    return name in files

def my_return(conn, string):
    print(ret_str(string))
    conn.send(ret_str(string))

def remove_node_and_files(addr):
    global nodes, files, INVALID_INPUT
    _hash = my_md5(str(addr))
    nodes.pop(_hash)
    _files = files.copy()
    for key, val in _files.items():
        if val == _hash:
            files.pop(key)

    

def my_md5(string):
    return md5(encode(string)).hexdigest()

def ret_str(ret):
    _str = str(len(ret) + len(str(len(ret))) + 1) + ' ' + ret
    return encode(_str)

def encode(string):
    return string.encode('utf-8')

def handle_keyboard_args():
    global files, nodes
    while True:
        arguments = input('>>>> ')
        if arguments == '/files':
            print(list_to_str(files.keys()))
        elif arguments == '/nodes':
            print(list_to_str(nodes.values()))
        elif arguments == '/rawfiles':
            print(files)
        elif arguments == '/rawnodes':
            print(nodes)
        else:
            print('LIFE IS HARD\ncommands as follows\n\t/files\n\t/nodes\n\t/rawfiles\n\t/rawnodes\n')


if __name__ == '__main__':
    
    port = 8000
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    t = threading.Thread(target=handle_keyboard_args, args= ())
    t.daemon = True
    t.start()

    try:
        ip = '0.0.0.0'
        print('listening on %s at %d' %(ip, port))
        listen(port)
    except KeyboardInterrupt:
        exit()

