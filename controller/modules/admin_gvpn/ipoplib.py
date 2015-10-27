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
import keyring

from threading import Timer

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
    "contr_port": 5801,
    "local_uid": "",
    "uid_size": 40,
    "sec": True,
    "wait_time": 15,
    "buf_size": 65507,
    "router_mode": False,
    "on-demand_connection" : False,
    "on-demand_inactive_timeout" : 600,
    "tincan_logging": 1,
    "controller_logging" : "INFO",
    "icc" : False, # Inter-Controller Connection
    "icc_port" : 30000,
    "switchmode" : 0,
    "trim_enabled": False,
    "multihop": False,
    "multihop_cl": 100, #Multihop connection count limit
    "multihop_ihc": 3, #Multihop initial hop count
    "multihop_hl": 10, #Multihop maximum hop count limit
    "multihop_tl": 1,  # Multihop time limit (second)
    "multihop_sr": True, # Multihop source route
    "stat_report": False,
    "stat_server" : "metrics.ipop-project.org",
    "stat_server_port" : 5000
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

# PKTDUMP mode is for more detailed than debug logging, especially for dump
# packet contents in hexadecimal to log
logging.addLevelName(5, "PKTDUMP")
logging.PKTDUMP = 5

# server is cross-module(?) variable
server = None

# server is assigned in each Social/GroupVPN controller and then should be 
# assigned in library module too.
def set_global_variable_server(s):
    global server
    server = s

# When proces killed or keyboard interrupted exit_handler runs then exit
def exit_handler(signum, frame):
    logging.info("Terminating Controller")
    if CONFIG["stat_report"]:
        if server != None:
            server.report()
        else:
            logging.debug("Controller socket is not created yet")
    sys.exit(0)

signal.signal(signal.SIGINT, exit_handler)
# AFAIK, there is no way to catch SIGKILL
# signal.signal(signal.SIGKILL, exit_handler)
# signal.signal(signal.SIGQUIT, exit_handler)
signal.signal(signal.SIGTERM, exit_handler)

def pktdump(message, dump=None, *args, **argv):
    hext = ""
    if dump: 
        for i in range(0, len(dump),2):
            hext += dump[i:i+2].encode("hex")
            hext += " "
            if i % 16 == 14:
                hext += "\n"
        logging.log(5, message + "\n" + hext)
    else: 
        logging.log(5, message, *args, **argv)

logging.pktdump = pktdump
 
def ip6_a2b(str_ip6):
    return "".join(x.decode("hex") for x in str_ip6.split(':'))

def ip6_b2a(bin_ip6):
    return "".join(bin_ip6[x:x+2].encode("hex") + ":" for x in range(0, 14, 2))\
           + bin_ip6[14].encode("hex") + bin_ip6[15].encode("hex")


def ip4_a2b(str_ip4):
    return "".join(chr(int(x)) for x in str_ip4.split('.'))

def ip4_b2a(bin_ip4):
    return "".join(str(ord(bin_ip4[x])) + "." for x in range (0,3)) \
           + str(ord(bin_ip4[3]))

def mac_a2b(str_mac):
    return "".join(x.decode("hex") for x in str_mac.split(':'))

def mac_b2a(bin_mac):
    return "".join(bin_mac[x].encode("hex") + ":" for x in range(0,5)) +\
           bin_mac[5].encode("hex")

def uid_a2b(str_uid):
    return str_uid.decode("hex")

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

def make_call(sock, payload=None, **params):
    if socket.has_ipv6: dest = (CONFIG["localhost6"], CONFIG["svpn_port"])
    else: dest = (CONFIG["localhost"], CONFIG["svpn_port"])
    if payload == None:
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
    if socket.has_ipv6: dest = (CONFIG["localhost6"], CONFIG["svpn_port"])
    else: dest = (CONFIG["localhost"], CONFIG["svpn_port"])
    return sock.sendto(ipop_ver + tincan_packet + msg, dest)

def make_arp(src_uid=null_uid, dest_uid=null_uid, dest_mac=bc_mac,\
             src_mac=bc_mac, op="\x01", sender_mac=bc_mac,\
             sender_ip4=CONFIG["ip4"], target_mac=null_mac,\
             target_ip4=CONFIG["ip4"]):
    arp_msg = ""
    arp_msg += src_uid
    arp_msg += dest_uid
    arp_msg += dest_mac
    arp_msg += src_mac
    arp_msg += "\x08\x06" #Ether type of ARP
    arp_msg += "\x00\x01" #Hardware Type
    arp_msg += "\x08\x00" #Protocol Type
    arp_msg += "\x06" #Hardware address length
    arp_msg += "\x04" #Protocol address length
    arp_msg += "\x00" #Operation (ARP reply)
    arp_msg += op #Operation (ARP reply)
    arp_msg += sender_mac
    arp_msg += sender_ip4
    arp_msg += target_mac
    arp_msg += target_ip4
    return arp_msg

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

def do_set_local_ip(sock, uid, ip4, ip6, ip4_mask, ip6_mask, subnet_mask,\
                    switchmode):
    return make_call(sock, m="set_local_ip", uid=uid, ip4=ip4, ip6=ip6,
                     ip4_mask=ip4_mask, ip6_mask=ip6_mask,
                     subnet_mask=subnet_mask, switchmode=switchmode)

def do_set_remote_ip(sock, uid, ip4, ip6):
    if (CONFIG["switchmode"] == 1):
        return make_call(sock, m="set_remote_ip", uid=uid, ip4="127.0.0.1",\
                         ip6="::1/128")
    else: 
        return make_call(sock, m="set_remote_ip", uid=uid, ip4=ip4, ip6=ip6)

def do_get_state(sock,peer_uid = "",stats = True):
    return make_call(sock, m="get_state", uid = peer_uid ,stats=stats)

def do_set_logging(sock, logging):
    return make_call(sock, m="set_logging", logging=logging)

def do_set_translation(sock, translate):
    return make_call(sock, m="set_translation", translate=translate)

def do_set_switchmode(sock, switchmode):
    return make_call(sock, m="set_switchmode", switchmode=switchmode)

def do_set_trimpolicy(sock, trim_enabled):
    return make_call(sock, m="set_trimpolicy", trim_enabled=trim_enabled)

class UdpServer(object):
    def __init__(self, user, password, host, ip4):
        self.ipop_state = {}
        self.peers = {}
        self.peers_ip4 = {}
        self.peers_ip6 = {}
        self.far_peers = {}
        self.conn_stat = {}
        if socket.has_ipv6:
            self.sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self.sock_svr = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self.sock_svr.bind((CONFIG["localhost6"], CONFIG["contr_port"]))
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_svr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_svr.bind((CONFIG["localhost"], CONFIG["contr_port"]))
        self.sock.bind(("", 0))
        self.sock_list = [ self.sock, self.sock_svr ]

    def inter_controller_conn(self):

        self.cc_sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)

        while True:
            try:
                time.sleep(3)
                self.cc_sock.bind((gen_ip6(self.uid), CONFIG["icc_port"]))
            except Exception as e:
                logging.debug("Wait till ipop tap up")
                continue
            else:
                break

        self.sock_list.append(self.cc_sock)

    def trigger_conn_request(self, peer):
        if "fpr" not in peer and peer["xmpp_time"] < CONFIG["wait_time"] * 8:
            self.conn_stat[peer["uid"]] = "req_sent"
            do_send_msg(self.sock, "con_req", 1, peer["uid"],
                        self.ipop_state["_fpr"]);

    def check_collision(self, msg_type, uid):
        if msg_type == "con_req" and \
           self.conn_stat.get(uid, None) == "req_sent":
            if uid > self.ipop_state["_uid"]:
                do_trim_link(self.sock, uid)
                self.conn_stat.pop(uid, None)
            return False
        elif msg_type == "con_resp":
            self.conn_stat[uid] = "resp_recv"
            return False
        else:
            return True

    def packet_handle(self, data):
        ip4 = ip4_b2a(data[72:76])
        if ip4 in self.arp_table:
            if self.arp_table[ip4]["local"]:
                logging.debug("OS arp cache not yet evicted {0}. discarding"
                              " packet".format(ip4))
                return
            make_remote_call(self.cc_sock,dest_addr=self.arp_table[ip4]["ip6"],\
              dest_port=CONFIG["icc_port"], m_type=tincan_packet, 
              payload=data[42:])
            logging.debug("send packet over controller {0} in {1}".format(ip4,\
                          self.arp_table))
            return

        # Not found, broadcast ARP request message
        for k, v in self.peers.iteritems():
            if v["status"] == "online":
                logging.debug("Send arp_request({0}) msg to {1}".format(ip4,\
                              v["ip6"]))
                make_remote_call(sock=self.cc_sock, dest_addr=v["ip6"],\
                  dest_port=CONFIG["icc_port"], m_type=tincan_control,\
                  payload=None, msg_type="arp_request", target_ip4=ip4)

    def icc_packet_handle(self, addr, data):
        if data[0] != ipop_ver:
           #TODO change it to raising exception
            logging.error("ipop version mismatch: tincan:{0} contro ller:{1}"
                          "".format(data[0], ipop_ver))
            sys.exit()
        if data[1] == tincan_control:
            msg = json.loads(data[2:])
            logging.debug("msg:{0}".format(data[2:]))
            msg_type = msg.get("msg_type", None)
            if msg_type == "arp_request":
                target_ip4 = msg["target_ip4"]
                #Set source mac as broadcast 
                arp = make_arp(src_mac=mac_a2b(self.ipop_state["_mac"]), \
                  op="\x01", sender_ip4=ip4_a2b(self.ipop_state["_ip4"]),\
                  target_ip4=ip4_a2b(target_ip4))
                send_packet(self.sock, arp)

            elif msg_type == "arp_reply":
                self.arp_table[msg["target_ip4"]] = msg
                self.arp_table[msg["target_ip4"]]["local"] = False
                arp = make_arp(src_mac=mac_a2b(msg["mac"]),\
                  op="\x02", sender_mac=mac_a2b(msg["mac"]),\
                  sender_ip4=ip4_a2b(msg["target_ip4"]),\
                  target_ip4=ip4_a2b(msg["target_ip4"]))
                send_packet(self.sock, arp)


        elif data[1] == tincan_packet:
            logging.debug("icc_packet_handle packet")
            if ip4_b2a(data[32:36]) in self.arp_table:
                msg = ""
                msg += null_uid
                msg += null_uid
                msg += mac_a2b(self.arp_table[ip4_b2a(data[32:36])]["mac"])
                msg += data[8:14] #MAC
                msg += data[14:]
                send_packet(self.sock, msg)

    def update_farpeers(self, key, hop_count, via):
        if not key in self.far_peers:
            self.far_peers[key] = {}
            self.far_peers[key]["hop_count"] = sys.maxint
        if self.far_peers[key]["hop_count"] >= hop_count:
            self.far_peers[key]["hop_count"] = hop_count
            self.far_peers[key]["via"] = via
        logging.debug("farpeers:{0}".format(self.far_peers))

    def flood(self, dest_ip6, ttl):
        for k, v in self.peers.iteritems():
            if "ip6" in v:
                make_remote_call(sock=self.cc_sock, dest_addr=v["ip6"],\
                  dest_port=CONFIG["icc_port"], m_type=tincan_control,\
                  payload=None, msg_type="lookup_request", target_ip6=dest_ip6,\
                  via=[self.ipop_state["_ip6"], v["ip6"]], ttl=ttl)

    def lookup(self, dest_ip6):
        logging.pktdump("Lookup: {0} pending lookup:{1}".format(dest_ip6, \
                        self.lookup_req))
        if dest_ip6 in self.lookup_req:
            return
        # If no response from the lookup_request message at a certain time.
        # Cancel the request 
        self.lookup_req[dest_ip6] = { "ttl" : CONFIG["multihop_ihc"]}
        timer = Timer(CONFIG["multihop_tl"], self.lookup_timeout, \
                      args=[dest_ip6])
        timer.start()
        self.flood(dest_ip6, CONFIG["multihop_ihc"])

    def lookup_timeout(self, dest_ip6):
        # Lookup request message had been resolved
        if not dest_ip6 in self.lookup_req:
            return

        # If the hop count exceeds the hop limit, give up lookup 
        if 2*self.lookup_req[dest_ip6]["ttl"] > CONFIG["multihop_hl"]: 
            del self.lookup_req[dest_ip6]
            return

        # Multiply ttl by two and retry lookup_request flooding  
        self.flood(dest_ip6, 2*self.lookup_req[dest_ip6]["ttl"])

    def multihop_handle(self, data):
        if data[0] != ipop_ver:
             logging.error("ipop version mismatch: tincan:{0} controller:{1}"
                     "".format(data[0].encode("hex"), ipop_ver.encode("hex")))
        if data[1] == tincan_control: 
            msg = json.loads(data[2:])
            logging.debug("multihop control message recv {0}".format(msg))
            msg_type = msg.get("msg_type", None)
            if msg_type == "lookup_request":

                #If this message visit here before, just drop it
                for via in msg["via"][:-1]:
                    if self.ipop_state["_ip6"] == via:
                        return

                # found in peer, do lookup_reply
                for k, v in self.peers.iteritems():
                    if "ip6" in v and v["ip6"] == msg["target_ip6"]:
                        # IP is found in my peers,  
                        # send reply message back to previous sender
                        make_remote_call(sock=self.cc_sock,\
                          dest_addr=msg["via"][-2],\
                          dest_port=CONFIG["icc_port"], m_type=tincan_control,\
                          payload=None, msg_type="lookup_reply",\
                          target_ip6=msg["target_ip6"], via=msg["via"],\
                                         via_idx=-2)
                        return

                # not found in peer, add current node to via then flood 
                # lookup_request
                for k, v in self.peers.iteritems():
                    #Do not send lookup_request back to previous hop
                    logging.pktdump("k:{0}, v:{1}".format(k, v))
                    if "ip6" in v and msg["via"][-2] == v["ip6"]:
                        continue
                    # Flood lookup_request
                    if "ip6" in v and msg["ttl"] > 1:
                        make_remote_call(sock=self.cc_sock, dest_addr=v["ip6"],\
                          dest_port=CONFIG["icc_port"], m_type=tincan_control,\
                          payload=None, msg_type="lookup_request",\
                          ttl=msg["ttl"]-1, target_ip6=msg["target_ip6"],\
                          via=msg["via"] + [v["ip6"]])

            if msg_type == "lookup_reply":
                if CONFIG["multihop_sr"]:
                    if  ~msg["via_idx"]+1==len(msg["via"]):
                        # In source route mode, only source node updates route 
                        # information
                        self.update_farpeers(msg["target_ip6"],len(msg["via"]), 
                                             msg["via"])
                        if msg["target_ip6"] in self.lookup_req:
                            del self.lookup_req[msg["target_ip6"]]
                        return
                else:
                    # Non source route mode, route information is kept at each
                    # hop. Each node only keeps the next hop info
                    self.update_farpeers(msg["target_ip6"], len(msg["via"]),\
                                     msg["via"][msg["via_idx"]+1]) 

                # Send lookup_reply message back to the source
                make_remote_call(sock=self.cc_sock,\
                  dest_addr=msg["via"][msg["via_idx"]-1],\
                  dest_port=CONFIG["icc_port"], m_type=tincan_control,\
                  payload=None, msg_type="lookup_reply",\
                  target_ip6=msg["target_ip6"],\
                  via=msg["via"], via_idx=msg["via_idx"]-1)

            if msg_type == "route_error":
                if msg["index"] == 0:
                    del self.far_peers[msg["via"][-1]]
                else:
                    make_remote_call(sock=self.cc_sock,\
                      dest_addr=msg["via"][msg["index"]-1],\
                      dest_port=CONFIG["icc_port"], m_type=tincan_control,\
                      payload=None, msg_type="route_error",\
                      index=msg["index"]-1, via=msg["via"])\

        if data[1] == tincan_packet: 
            target_ip6=ip6_b2a(data[40:56])
            logging.pktdump("Multihop Packet Destined to {0}".format(\
                            target_ip6))
            if target_ip6 == self.ipop_state["_ip6"]:
                make_call(self.sock, payload=null_uid + null_uid + data[2:])
                return

            # The packet destined to its direct peers
            for k, v in self.peers.iteritems():
                if "ip6" in v and v["ip6"] == target_ip6:
                    make_remote_call(sock=self.cc_sock, dest_addr=target_ip6,\
                      dest_port=CONFIG["icc_port"], m_type=tincan_packet,\
                      payload=data[2:])
                    return

            # The packet is not in direct peers but have route information
            if ip6_b2a(data[40:56]) in self.far_peers: 
                make_remote_call(sock=self.cc_sock,\
                  dest_addr=self.far_peers[target_ip6]["via"],\
                  dest_port=CONFIG["icc_port"], m_type=tincan_packet,\
                  payload=data[2:])
                return
            logging.error("Unroutable packet. Oops this should not happen")

        if data[1] == tincan_sr6: 
            logging.pktdump("Multihop packet received", dump=data)
            hop_index = ord(data[2]) + 1
            hop_count = ord(data[3])
            if hop_index == hop_count:
                make_call(self.sock, payload=null_uid + null_uid +\
                  data[4+(hop_index)*16:])
                return
            packet = chr(hop_index)
            packet += data[3:]
            next_addr_offset = 4+(hop_index)*16
            next_hop_addr = data[next_addr_offset:next_addr_offset+16] 
            for k, v in self.peers.iteritems():
                if v["ip6"]==ip6_b2a(next_hop_addr) and v["status"]=="online":
                    make_remote_call(sock=self.cc_sock,\
                      dest_addr=ip6_b2a(next_hop_addr),\
                      dest_port=CONFIG["icc_port"], m_type=tincan_sr6,\
                      payload=packet)
                    return
            via = []
            for i in range(hop_count):
                via.append(ip6_b2a(data[4+i*16:4+16*i+16]))
            make_remote_call(sock=self.cc_sock, dest_addr=via[hop_index-2],\
              dest_port=CONFIG["icc_port"], m_type=tincan_control,\
              payload=None, msg_type="route_error", via=via, index=hop_index-2)
            logging.debug("Link lost send back route_error message to source{0}"
                          "".format(via[hop_index-2]))
                
    def report(self):
        data = json.dumps({ 
                "xmpp_host" : hashlib.sha1(CONFIG["xmpp_host"]).hexdigest(),\
                "uid": hashlib.sha1(self.uid).hexdigest(), "xmpp_username":\
                hashlib.sha1(CONFIG["xmpp_username"]).hexdigest(),\
                "time": str(datetime.datetime.now()),\
                "controller": self.vpn_type, "version": ord(ipop_ver)}) 

        try:
            url="http://" + CONFIG["stat_server"] + ":" +\
                str(CONFIG["stat_server_port"]) + "/api/submit"
            req = urllib2.Request(url=url, data=data)
            req.add_header("Content-Type", "application/json")
            res = urllib2.urlopen(req)
            logging.debug("Succesfully reported status to the stat-server({0})."
              ".\nHTTP response code:{1}, msg:{2}".format(url, res.getcode(),\
              res.read()))
            if res.getcode() != 200:
                raise
        except:
            logging.debug("Status report failed.")

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
    parser.add_argument("-s", help="configuration as json string "
                                   "(overrides configuration from file)",
                        dest="config_string", metavar="config_string")
    parser.add_argument("--pwdstdout", help="use stdout as password stream",
                        dest="pwdstdout", action="store_true")

    args = parser.parse_args()

    if args.config_file:
        # Load the config file
        with open(args.config_file) as f:
            loaded_config = json.load(f)
        CONFIG.update(loaded_config)
        
    if args.config_string:
        # Load the config string
        loaded_config = json.loads(args.config_string)
        CONFIG.update(loaded_config)        

    need_save = setup_config(CONFIG)
    if need_save and args.config_file and args.update_config:
        with open(args.config_file, "w") as f:
            json.dump(CONFIG, f, indent=4, sort_keys=True)

    if not ("xmpp_username" in CONFIG and "xmpp_host" in CONFIG):
        raise ValueError("At least 'xmpp_username' and 'xmpp_host' must be "
                         "specified in config file or string")
    keyring.set_keyring(keyring.backends.file.PlaintextKeyring())
    save_password = False
    if not args.update_config:
        if "xmpp_password" in CONFIG:
            # password is present in config file. No need to look
            # for other options store password in keyring
            temp = CONFIG["xmpp_password"]
            save_password = True
            # we need to store it in keyring because there is no way
            # to distinguish between environments where password is 
            # always present in config file and otherwise. So even though 
            # config with password is going to be used always, the keyring 
            # might prompt for password for keyring on first run
        else:
            # Try to retrieve password from keyring
            temp = keyring.get_password("ipop", CONFIG["xmpp_username"])
            # Try to request valid password from user
            while temp == None or temp == "":
                 prompt = "\nPassword for %s: " % CONFIG["xmpp_username"]
                 if args.pwdstdout:
                      temp = getpass.getpass(prompt, stream=sys.stdout)
                 else:
                      temp = getpass.getpass(prompt)
                 save_password = True
        if temp != None:
            CONFIG["xmpp_password"] = temp
    else:
        save_password = True
     
    if save_password:
        # Save password in keyring
        try:
            keyring.set_password("ipop", CONFIG["xmpp_username"],\
                                 CONFIG["xmpp_password"])
        except:
            logging.error("Unable to store password in the keyring")
            sys.exit(0)

    if "controller_logging" in CONFIG:
        level = getattr(logging, CONFIG["controller_logging"])
        logging.basicConfig(level=level)

    if args.ip_config:
        load_peer_ip_config(args.ip_config)

