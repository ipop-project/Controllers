#!/usr/bin/env python
import json
import socket
import random
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule


class TincanSender(ControllerModule):

    def __init__(self, sock_list, CFxHandle, paramDict, ModuleName):
        super(TincanSender, self).__init__(CFxHandle, paramDict, ModuleName)

        self.sock = sock_list[0]
        self.sock_svr = sock_list[1]

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        if cbt.action == 'DO_CREATE_LINK':
            uid = cbt.data.get('uid')
            fpr = cbt.data.get('fpr')
            nid = cbt.data.get('nid')
            sec = cbt.data.get('sec')
            cas = cbt.data.get('cas')
            self.do_create_link(self.sock, uid, fpr, nid, sec, cas)

        elif cbt.action == 'DO_TRIM_LINK':
            self.do_trim_link(self.sock, uid=cbt.data)

        elif cbt.action == 'DO_GET_STATE':
            query_uid = cbt.data
            self.do_get_state(self.sock, query_uid)


        elif cbt.action == 'DO_SET_REMOTE_IP':
            uid = cbt.data.get("uid")
            ip4 = cbt.data.get("ip4")
            self.do_set_remote_ip(self.sock,
                                  uid, ip4, self.gen_ip6(uid))

        elif cbt.action == 'ECHO_REPLY':
            m_type = cbt.data.get('m_type')
            dest_addr = cbt.data.get('dest_addr')
            dest_port = cbt.data.get('dest_port')
            self.make_remote_call(self.sock_svr, m_type=m_type,
                                  dest_addr=dest_addr, dest_port=dest_port,
                                  payload=None, type="echo_reply")

        elif cbt.action == 'DO_SEND_ICC_MSG':
            src_uid = cbt.data.get('src_uid')
            dst_uid = cbt.data.get('dst_uid')
            icc_type = cbt.data.get('icc_type')
            msg = cbt.data.get('msg')
            self.do_send_icc_msg(self.sock, src_uid, dst_uid, icc_type, msg)

        elif cbt.action == 'DO_INSERT_DATA_PACKET':
            self.send_packet(self.sock, ipoplib.hexstr2b(cbt.data))

        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def do_send_icc_msg(self, sock, src_uid, dst_uid, icc_type, msg):
        if socket.has_ipv6:
            dest = (self.CMConfig["localhost6"], self.CMConfig["svpn_port"])
        else:
            dest = (self.CMConfig["localhost"], self.CMConfig["svpn_port"])

        if icc_type == "control":
            return sock.sendto(ipoplib.ipop_ver + ipoplib.icc_control + ipoplib.uid_a2b(src_uid) + ipoplib.uid_a2b(dst_uid) + ipoplib.icc_mac_control + ipoplib.icc_ethernet_padding + bytes(json.dumps(msg).encode('utf-8')), dest)
        elif icc_type == "packet":
            return sock.sendto(ipoplib.ipop_ver + ipoplib.icc_packet + ipoplib.uid_a2b(src_uid) + ipoplib.uid_a2b(dst_uid) + ipoplib.icc_mac_packet + ipoplib.icc_ethernet_padding + bytes(json.dumps(msg).encode('utf-8')), dest)

    def do_create_link(self, sock, uid, fpr, overlay_id, sec,
                       cas, stun=None, turn=None):
        if stun is None:
            stun = random.choice(self.CMConfig["stun"])
        if turn is None:
            if self.CMConfig["turn"]:
                turn = random.choice(self.CMConfig["turn"])
            else:
                turn = {"server": "", "user": "", "pass": ""}
        return self.make_call(sock, m="create_link", uid=uid, fpr=fpr,
                              overlay_id=overlay_id, stun=stun,
                              turn=turn["server"],
                              turn_user=turn["user"],
                              turn_pass=turn["pass"],
                              sec=sec, cas=cas)

    def do_trim_link(self, sock, uid):
        return self.make_call(sock, m="trim_link", uid=uid)

    def do_get_state(self, sock, peer_uid="", stats=True):
        return self.make_call(sock, m="get_state", uid=peer_uid, stats=stats)

    def do_set_remote_ip(self, sock, uid, ip4, ip6):
        if self.CMConfig["switchmode"] == 1:
            return self.make_call(sock, m="set_remote_ip", uid=uid,
                                  ip4="127.0.0.1", ip6="::1/128")
        else:
            return self.make_call(sock, m="set_remote_ip", uid=uid, ip4=ip4,
                                  ip6=ip6)

    def make_call(self, sock, payload=None, **params):
        if socket.has_ipv6:
            dest = (self.CMConfig["localhost6"], self.CMConfig["svpn_port"])
        else:
            dest = (self.CMConfig["localhost"], self.CMConfig["svpn_port"])
        if payload is None:
            return sock.sendto(ipoplib.ipop_ver + ipoplib.tincan_control + bytes(json.dumps(params).encode('utf-8')), dest)
        else:
            return sock.sendto(bytes((ipoplib.ipop_ver + ipoplib.tincan_packet + payload).encode('utf-8')), dest)

    def gen_ip6(self, uid, ip6=None):
        if ip6 is None:
            ip6 = self.CMConfig["ip6_prefix"]
        for i in range(0, 16, 4):
            ip6 += ":" + uid[i:i+4]
        return ip6

    def make_remote_call(self, sock, dest_addr, dest_port, m_type, payload, **params):
        dest = (dest_addr, dest_port)
        if m_type == ipoplib.tincan_control:
            return sock.sendto(ipoplib.ipop_ver + m_type +
                               json.dumps(params), dest)
        else:
            return sock.sendto(ipoplib.ipop_ver + m_type +
                               payload, dest)

    def send_packet(self, sock, msg):
        if socket.has_ipv6:
            dest = (self.CMConfig["localhost6"], self.CMConfig["svpn_port"])
        else:
            dest = (self.CMConfig["localhost"], self.CMConfig["svpn_port"])
        return sock.sendto(ipoplib.ipop_ver + ipoplib.tincan_packet + msg, dest)

    def timer_method(self):
        pass

    def terminate(self):
        pass
