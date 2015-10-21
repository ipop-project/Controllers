import json
import socket
import random
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule


class TincanSender(ControllerModule):

    ipop_ver = "\x02"
    tincan_control = "\x01"
    tincan_packet = "\x02"

    def __init__(self, sock_list, CFxHandle, paramDict):

        super(TincanSender, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.sock = sock_list[0]
        self.sock_svr = sock_list[1]
        if self.CMConfig['icc']:
            self.sock_icc = sock_list[2]

    def initialize(self):

        logCBT = self.CFxHandle.createCBT(initiator='TincanSender',
                                          recipient='Logger',
                                          action='info',
                                          data="TincanSender Loaded")
        self.CFxHandle.submitCBT(logCBT)

    def processCBT(self, cbt):

        if(cbt.action == 'DO_CREATE_LINK'):

            uid = cbt.data.get('uid')
            fpr = cbt.data.get('fpr')
            nid = cbt.data.get('nid')
            sec = cbt.data.get('sec')
            cas = cbt.data.get('cas')
            self.do_create_link(self.sock, uid, fpr, nid, sec, cas)

        elif(cbt.action == 'DO_TRIM_LINK'):

            # cbt.data contains the UID of the peer
            self.do_trim_link(self.sock, cbt.data)

        elif(cbt.action == 'DO_GET_STATE'):

            self.do_get_state(self.sock)

        elif(cbt.action == 'DO_SEND_MSG'):

            method = cbt.data.get("method")
            overlay_id = cbt.data.get("overlay_id")
            uid = cbt.data.get("uid")
            data = cbt.data.get("data")
            self.do_send_msg(self.sock, method, overlay_id, uid, data)

        elif(cbt.action == 'DO_SET_REMOTE_IP'):

            uid = cbt.data.get("uid")
            ip4 = cbt.data.get("ip4")
            self.do_set_remote_ip(self.sock,
                                  uid, ip4, self.gen_ip6(uid))

        elif(cbt.action == 'ECHO_REPLY'):
            m_type = cbt.data.get('m_type')
            dest_addr = cbt.data.get('dest_addr')
            dest_port = cbt.data.get('dest_port')
            self.make_remote_call(self.sock_svr, m_type=m_type,
                                  dest_addr=dest_addr, dest_port=dest_port,
                                  payload=None, type="echo_reply")

        elif(cbt.action == 'DO_SEND_ICC_MSG'):
            msg = json.dumps(cbt.data.get('msg'))
            dest_addr = self.gen_ip6(cbt.data.get('dest_uid'))
            dest_port = self.CMConfig["icc_port"]
            self.sock_icc.sendto(self.ipop_ver + self.tincan_control + msg,
                                 (dest_addr, dest_port))

        elif(cbt.action == 'DO_INSERT_DATA_PACKET'):
            ipoplib.send_packet(self.sock, cbt.data.decode("hex"))

        else:
            logCBT = self.CFxHandle.createCBT(initiator='TincanSender',
                                              recipient='Logger',
                                              action='warning',
                                              data="TincanSender: Unrecognized"
                                              "CBT from " + cbt.initiator)
            self.CFxHandle.submitCBT(logCBT)

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

    def do_send_msg(self, sock, method, overlay_id, uid, data):
        return self.make_call(sock, m=method, overlay_id=overlay_id,
                              uid=uid, data=data)

    def do_set_remote_ip(self, sock, uid, ip4, ip6):
        if (self.CMConfig["switchmode"] == 1):
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
            return sock.sendto(self.ipop_ver + self.tincan_control +
                               json.dumps(params), dest)
        else:
            return sock.sendto(self.ipop_ver + self.tincan_packet +
                               payload, dest)

    def gen_ip6(self, uid, ip6=None):
        if ip6 is None:
            ip6 = self.CMConfig["ip6_prefix"]
        for i in range(0, 16, 4):
            ip6 += ":" + uid[i:i+4]
        return ip6

    def make_remote_call(self, sock, dest_addr, dest_port, m_type,
                         payload, **params):
        dest = (dest_addr, dest_port)
        if m_type == self.tincan_control:
            return sock.sendto(self.ipop_ver + m_type +
                               json.dumps(params), dest)
        else:
            return sock.sendto(self.ipop_ver + m_type +
                               payload, dest)

    def timer_method(self):
        pass

    def terminate(self):
        pass
