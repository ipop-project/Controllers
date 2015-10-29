#!/usr/bin/env python

import argparse
import binascii
import datetime
import getpass
import hashlib
import json
import logging
import os
import random
import select
import signal
import socket
import struct
import sys
import time
import urllib2

from threading import Timer

# Set default config values

CONFIG = {
    "CFx": {
        "ip4_mask": 24,
        "ip6_mask": 64,
        "subnet_mask": 32,
        "contr_port": 5801,
        "local_uid": "",
        "uid_size": 40,
        "router_mode": False,
        "tincan_logging": 1,
        "icc": False,  # Inter-Controller Connection
        "icc_port": 30000,
        "trim_enabled": False,
        "multihop_cl": 100,  # Multihop connection count limit
        "multihop_ihc": 3,  # Multihop initial hop count
        "multihop_hl": 10,  # Multihop maximum hop count limit
        "multihop_tl": 1,  # Multihop time limit (second)
        "multihop_sr": True,  # Multihop source route
        "stat_report": False,
        "stat_server": "metrics.ipop-project.org",
        "stat_server_port": 5000
    },
    "TincanListener": {
        "buf_size": 65507,
        "socket_read_wait_time": 15,
        "joinEnabled": True
    },
    "Logger": {
        "controller_logging": "INFO",
        "joinEnabled": True
    },
    "TincanDispatcher": {
        "joinEnabled": True
    },
    "TincanSender": {
        "stun": ["stun.l.google.com:19302", "stun1.l.google.com:19302",
                 "stun2.l.google.com:19302", "stun3.l.google.com:19302",
                 "stun4.l.google.com:19302"],
        "turn": [],
        "ip6_prefix": "fd50:0dbc:41f2:4a3c",
        "switchmode": 0,
        "localhost": "127.0.0.1",
        "svpn_port": 5800,
        "localhost6": "::1",
        "joinEnabled": True
     }
}

IP_MAP = {}

ipop_ver = "\x02"
tincan_control = "\x01"
tincan_packet = "\x02"
tincan_sr6 = "\x03"
tincan_sr6_end = "\x04"
null_uid = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
null_uid += "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
bc_mac = "\xff\xff\xff\xff\xff\xff"
null_mac = "\x00\x00\x00\x00\x00\x00"

# server is cross-module(?) variable
server = None

# server is assigned in each Social/GroupVPN controller and then should be
# assigned in library module too.
def set_global_variable_server(s):
    global server
    server = s

# # When proces killed or keyboard interrupted exit_handler runs then exit
# def exit_handler(signum, frame):
#     logging.info("Terminating Controller")
#     if CONFIG["stat_report"]:
#         if server != None:
#             server.report()
#         else:
#             logging.debug("Controller socket is not created yet")
#     sys.exit(0)

# signal.signal(signal.SIGINT, exit_handler)
# AFAIK, there is no way to catch SIGKILL
# signal.signal(signal.SIGKILL, exit_handler)
# signal.signal(signal.SIGQUIT, exit_handler)
# signal.signal(signal.SIGTERM, exit_handler)

def ip6_a2b(str_ip6):
    return "".join(x.decode("hex") for x in str_ip6.split(':'))

def ip6_b2a(bin_ip6):
    return "".join(bin_ip6[x:x+2].encode("hex") + ":" for x in range(0, 14, 2))\
           + bin_ip6[14].encode("hex") + bin_ip6[15].encode("hex")


def ip4_a2b(str_ip4):
    return "".join(chr(int(x)) for x in str_ip4.split('.'))

def ip4_b2a(bin_ip4):
    return "".join(str(ord(bin_ip4[x])) + "." for x in range(0, 3)) \
           + str(ord(bin_ip4[3]))

def mac_a2b(str_mac):
    return "".join(x.decode("hex") for x in str_mac.split(':'))

def mac_b2a(bin_mac):
    return "".join(bin_mac[x].encode("hex") + ":" for x in range(0, 5)) +\
           bin_mac[5].encode("hex")

def uid_a2b(str_uid):
    return str_uid.decode("hex")

def gen_ip4(uid, peer_map, ip4=None):
    ip4 = ip4 or CONFIG['AddressMapper']["ip4"]
    try:
        return peer_map[uid]
    except KeyError:
        pass

    ips = set(peer_map.itervalues())
    prefix, _ = ip4.rsplit(".", 1)
    # We allocate to *.101 - *.254. This ensures a 3-digit suffix and avoids
    # the broadcast address. *.100 is our IPv4 address.
    for i in range(101, 255):
        peer_map[uid] = "%s.%s" % (prefix, i)
        if peer_map[uid] not in ips:
            return peer_map[uid]
    del peer_map[uid]
    raise OverflowError("Too many peers, out of IPv4 addresses")

def gen_ip6(uid, ip6=None):
    if ip6 is None:
        ip6 = CONFIG["TincanSender"]["ip6_prefix"]
    for i in range(0, 16, 4):
        ip6 += ":" + uid[i:i+4]
    return ip6

def gen_uid(ip4):
    return hashlib.sha1(ip4).hexdigest()[:CONFIG["CFx"]["uid_size"]]

def make_call(sock, payload=None, **params):
    if socket.has_ipv6:
        dest = (CONFIG["TincanSender"]["localhost6"],
                CONFIG["TincanSender"]["svpn_port"])
    else:
        dest = (CONFIG["TincanSender"]["localhost"],
                CONFIG["TincanSender"]["svpn_port"])
    if payload is None:
        return sock.sendto(ipop_ver + tincan_control + json.dumps(params), dest)
    else:
        return sock.sendto(ipop_ver + tincan_packet + payload, dest)

def make_remote_call(sock, dest_addr, dest_port, m_type, payload, **params):
    dest = (dest_addr, dest_port)
    if m_type == tincan_control:
        return sock.sendto(ipop_ver + m_type + json.dumps(params), dest)
    else:
        return sock.sendto(ipop_ver + m_type + payload, dest)

def send_packet(sock, msg):
    if socket.has_ipv6:
        dest = (CONFIG["TincanSender"]["localhost6"],
                CONFIG["TincanSender"]["svpn_port"])
    else:
        dest = (CONFIG["CFx"]["TincanSender"],
                CONFIG["TincanSender"]["svpn_port"])
    return sock.sendto(ipop_ver + tincan_packet + msg, dest)
'''
def make_arp(src_uid=null_uid, dest_uid=null_uid, dest_mac=bc_mac,
             src_mac=bc_mac, op="\x01", sender_mac=bc_mac,
             sender_ip4=CONFIG['BaseTopologyManager']["ip4"], target_mac=null_mac,
             target_ip4=CONFIG['BaseTopologyManager']["ip4"]):
    arp_msg = ""
    arp_msg += src_uid
    arp_msg += dest_uid
    arp_msg += dest_mac
    arp_msg += src_mac
    arp_msg += "\x08\x06"  # Ether type of ARP
    arp_msg += "\x00\x01"  # Hardware Type
    arp_msg += "\x08\x00"  # Protocol Type
    arp_msg += "\x06"  # Hardware address length
    arp_msg += "\x04"  # Protocol address length
    arp_msg += "\x00"  # Operation (ARP reply)
    arp_msg += op  # Operation (ARP reply)
    arp_msg += sender_mac
    arp_msg += sender_ip4
    arp_msg += target_mac
    arp_msg += target_ip4
    return arp_msg
'''
def do_send_msg(sock, method, overlay_id, uid, data):
    return make_call(sock, m=method, overlay_id=overlay_id, uid=uid, data=data)

def do_set_cb_endpoint(sock, addr):
    return make_call(sock, m="set_cb_endpoint", ip=addr[0], port=addr[1])

def do_register_service(sock, username, password, host):
    return make_call(sock, m="register_svc", username=username,
                     password=password, host=host)

def do_create_link(sock, uid, fpr, overlay_id, sec, cas, stun=None, turn=None):
    if stun is None:
        stun = random.choice(CONFIG["CFx"]["stun"])
    if turn is None:
        if CONFIG["CFx"]["turn"]:
            turn = random.choice(CONFIG["CFx"]["turn"])
        else:
            turn = {"server": "", "user": "", "pass": ""}
    return make_call(sock, m="create_link", uid=uid, fpr=fpr,
                     overlay_id=overlay_id, stun=stun, turn=turn["server"],
                     turn_user=turn["user"],
                     turn_pass=turn["pass"], sec=sec, cas=cas)

def do_trim_link(sock, uid):
    return make_call(sock, m="trim_link", uid=uid)

def do_set_local_ip(sock, uid, ip4, ip6, ip4_mask, ip6_mask, subnet_mask,
                    switchmode):
    return make_call(sock, m="set_local_ip", uid=uid, ip4=ip4, ip6=ip6,
                     ip4_mask=ip4_mask, ip6_mask=ip6_mask,
                     subnet_mask=subnet_mask, switchmode=switchmode)

def do_set_remote_ip(sock, uid, ip4, ip6):
    if (CONFIG["TincanSender"]["switchmode"] == 1):
        return make_call(sock, m="set_remote_ip", uid=uid, ip4="127.0.0.1",
                         ip6="::1/128")
    else:
        return make_call(sock, m="set_remote_ip", uid=uid, ip4=ip4, ip6=ip6)

def do_get_state(sock, peer_uid="", stats=True):
    return make_call(sock, m="get_state", uid=peer_uid, stats=stats)

def do_set_logging(sock, logging):
    return make_call(sock, m="set_logging", logging=logging)

def do_set_translation(sock, translate):
    return make_call(sock, m="set_translation", translate=translate)

def do_set_switchmode(sock, switchmode):
    return make_call(sock, m="set_switchmode", switchmode=switchmode)

def do_set_trimpolicy(sock, trim_enabled):
    return make_call(sock, m="set_trimpolicy", trim_enabled=trim_enabled)

def load_peer_ip_config(ip_config):
    with open(ip_config) as f:
        ip_cfg = json.load(f)

    for peer_ip in ip_cfg:
        uid = peer_ip["uid"]
        ip = peer_ip["ipv4"]
        IP_MAP[uid] = ip
        logging.debug("MAP %s -> %s" % (ip, uid))
