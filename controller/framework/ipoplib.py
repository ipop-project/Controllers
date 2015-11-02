#!/usr/bin/env python

#import argparse
#import binascii
#import datetime
#import getpass
#import hashlib
#import json
#import logging
#import os
#import random
#import select
#import signal
#import socket
#import struct
#import sys
#import time
#import urllib2

#from threading import Timer


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

def gen_ip4(uid, peer_map, ip4):
   # ip4 = ip4 or CONFIG['AddressMapper']["ip4"]
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

def do_send_msg(sock, method, overlay_id, uid, data):
    return make_call(sock, m=method, overlay_id=overlay_id, uid=uid, data=data)

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


def do_set_remote_ip(sock, uid, ip4, ip6):
    if (CONFIG["TincanSender"]["switchmode"] == 1):
        return make_call(sock, m="set_remote_ip", uid=uid, ip4="127.0.0.1",
                         ip6="::1/128")
    else:
        return make_call(sock, m="set_remote_ip", uid=uid, ip4=ip4, ip6=ip6)
