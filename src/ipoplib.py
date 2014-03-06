#!/usr/bin/env python

import argparse
import binascii
import getpass
import hashlib
import json
import logging
import os
import random
import select
import socket
import sys
import time

# Set default config values
CONFIG = {
    "stun": ["stun.l.google.com:19302", "stun1.l.google.com:19302",
             "stun2.l.google.com:19302", "stun3.l.google.com:19302",
             "stun4.l.google.com:19302"],
    "turn": [],  # Contains dicts with "server", "user", "pass" keys
    "ip4": "172.16.0.1",
    "localhost": "127.0.0.1",
    "ip6_prefix": "fd50:0dbc:41f2:4a3c",
    "localhost6": "::1",
    "ip4_mask": 24,
    "ip6_mask": 64,
    "subnet_mask": 32,
    "svpn_port": 5800,
    "local_uid": "",
    "uid_size": 40,
    "sec": True,
    "wait_time": 15,
    "buf_size": 4096,
    "router_mode": False,
    "on-demand_connection" : False,
    "on-demand_inactive_timeout" : 600,
    "tincan_logging": 1,
    "controller_logging" : "INFO"
}

IP_MAP = {}

def gen_ip4(uid, peer_map, ip4=None):
    ip4 = ip4 or CONFIG["ip4"]
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
        ip6 = CONFIG["ip6_prefix"]
    for i in range(0, 16, 4): ip6 += ":" + uid[i:i+4]
    return ip6

def gen_uid(ip4):
    return hashlib.sha1(ip4).hexdigest()[:CONFIG["uid_size"]]

def make_call(sock, **params):
    if socket.has_ipv6: dest = (CONFIG["localhost6"], CONFIG["svpn_port"])
    else: dest = (CONFIG["localhost"], CONFIG["svpn_port"])
    return sock.sendto(json.dumps(params), dest)

def do_send_msg(sock, method, overlay_id, uid, data):
    return make_call(sock, m=method, overlay_id=overlay_id, uid=uid, data=data)

def do_set_cb_endpoint(sock, addr):
    return make_call(sock, m="set_cb_endpoint", ip=addr[0], port=addr[1])

def do_register_service(sock, username, password, host):
    return make_call(sock, m="register_svc", username=username,
                     password=password, host=host)

def do_create_link(sock, uid, fpr, overlay_id, sec, cas, stun=None, turn=None):
    if stun is None:
        stun = random.choice(CONFIG["stun"])
    if turn is None:
        if CONFIG["turn"]:
            turn = random.choice(CONFIG["turn"])
        else:
            turn = {"server": "", "user": "", "pass": ""}
    return make_call(sock, m="create_link", uid=uid, fpr=fpr,
                     overlay_id=overlay_id, stun=stun, turn=turn["server"],
                     turn_user=turn["user"],
                     turn_pass=turn["pass"], sec=sec, cas=cas)

def do_trim_link(sock, uid):
    return make_call(sock, m="trim_link", uid=uid)

def do_set_local_ip(sock, uid, ip4, ip6, ip4_mask, ip6_mask, subnet_mask):
    return make_call(sock, m="set_local_ip", uid=uid, ip4=ip4, ip6=ip6,
                     ip4_mask=ip4_mask, ip6_mask=ip6_mask,
                     subnet_mask=subnet_mask)

def do_set_remote_ip(sock, uid, ip4, ip6):
    return make_call(sock, m="set_remote_ip", uid=uid, ip4=ip4, ip6=ip6)

def do_get_state(sock):
    return make_call(sock, m="get_state", stats=True)

def do_set_logging(sock, logging):
    return make_call(sock, m="set_logging", logging=logging)

def do_set_translation(sock, translate):
    return make_call(sock, m="set_translation", translate=translate)

class UdpServer(object):
    def __init__(self, user, password, host, ip4):
        self.state = {}
        self.peers = {}
        self.conn_stat = {}
        if socket.has_ipv6:
            self.sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", 0))

    def trigger_conn_request(self, peer):
        if "fpr" not in peer and peer["xmpp_time"] < CONFIG["wait_time"] * 8:
            self.conn_stat[peer["uid"]] = "req_sent"
            do_send_msg(self.sock, "con_req", 1, peer["uid"],
                        self.state["_fpr"]);

    def check_collision(self, msg_type, uid):
        if msg_type == "con_req" and \
           self.conn_stat.get(uid, None) == "req_sent":
            if uid > self.state["_uid"]:
                do_trim_link(self.sock, uid)
                self.conn_stat.pop(uid, None)
                return False
        elif msg_type == "con_resp":
            self.conn_stat[uid] = "resp_recv"
            return False
        else:
            return True

def setup_config(config):
    """Validate config and set default value here. Return ``True`` if config is
    changed.
    """
    if not config["local_uid"]:
        uid = binascii.b2a_hex(os.urandom(CONFIG["uid_size"] / 2))
        config["local_uid"] = uid
        return True # modified
    return False

def load_peer_ip_config(ip_config):
    with open(ip_config) as f:
        ip_cfg = json.load(f)

    for peer_ip in ip_cfg:
        uid = peer_ip["uid"]
        ip = peer_ip["ipv4"]
        IP_MAP[uid] = ip
        logging.debug("MAP %s -> %s" % (ip, uid))

def parse_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", help="load configuration from a file",
                        dest="config_file", metavar="config_file")
    parser.add_argument("-u", help="update configuration file if needed",
                        dest="update_config", action="store_true")
    parser.add_argument("-p", help="load remote ip configuration file",
                        dest="ip_config", metavar="ip_config")

    args = parser.parse_args()

    if args.config_file:
        # Load the config file
        with open(args.config_file) as f:
            loaded_config = json.load(f)
        CONFIG.update(loaded_config)

    need_save = setup_config(CONFIG)
    if need_save and args.config_file and args.update_config:
        with open(args.config_file, "w") as f:
            json.dump(CONFIG, f, indent=4, sort_keys=True)

    if not ("xmpp_username" in CONFIG and "xmpp_host" in CONFIG):
        raise ValueError("At least 'xmpp_username' and 'xmpp_host' must be "
                         "specified in config file")

    if "xmpp_password" not in CONFIG:
        prompt = "\nPassword for %s: " % CONFIG["xmpp_username"]
        CONFIG["xmpp_password"] = getpass.getpass(prompt)

    if "controller_logging" in CONFIG:
        level = getattr(logging, CONFIG["controller_logging"])
        logging.basicConfig(level=level)

    if args.ip_config:
        load_peer_ip_config(args.ip_config)





