#!/usr/bin/env python
import hashlib
import json,socket
import logging
ipopVerMjr = "16";
ipopVerMnr = "01";
ipopVerRev = "0";
ipopVerRel = "{0}.{1}.{2}".format(ipopVerMjr, ipopVerMnr, ipopVerRev)

# set default config values
CONFIG = {
    "CFx": {
        "subnet_mask": 16,
        "contr_port": 5801,
        "local_uid": "",
        "uid_size": 40,
        "router_mode": False,
        "ipopVerRel" : ipopVerRel,
        "MTU4": 1200,
        "MTU6": 1200,
        "LocalPrefix6": 64,
        "LocalPrefix4": 16
    },
    "TincanListener": {
        "buf_size": 65507,
        "socket_read_wait_time": 15,
        "dependencies": ["Logger"]
    },
    "TincanSender": {
        "ip6_prefix": "fd50:0dbc:41f2:4a3c",
        "localhost": "127.0.0.1",
        "svpn_port": 5800,
        "localhost6": "::1",
        "dependencies": ["Logger"]
     }
}

def gen_ip6(uid, ip6=None):
    if ip6 is None:
        ip6 = CONFIG["TincanSender"]["ip6_prefix"]
    for i in range(0, 16, 4):
        ip6 += ":" + uid[i:i+4]
    return ip6

def gen_uid(ip4):
    return hashlib.sha1(ip4.encode('utf-8')).hexdigest()[:CONFIG["CFx"]["uid_size"]]

def send_msg(sock, msg):
    if socket.has_ipv6:
        dest = (CONFIG["TincanSender"]["localhost6"],
                CONFIG["TincanSender"]["svpn_port"])
    else:
        dest = (CONFIG["TincanSender"]["localhost"],
                CONFIG["TincanSender"]["svpn_port"])
    return sock.sendto(bytes((msg).encode('utf-8')),dest)
