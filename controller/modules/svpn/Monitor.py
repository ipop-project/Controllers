#!/usr/bin/env python
import time
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule


class Monitor(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(Monitor, self).__init__(CFxHandle, paramDict, ModuleName)

        self.use_visualizer = False
        if 'use_central_visualizer' in self.CMConfig:
            self.use_visualizer = self.CMConfig["use_central_visualizer"]

        self.peerlist = set()
        self.peers = {}
        self.peers_ip4 = {}
        self.peers_ip6 = {}
        self.far_peers = {}
        self.conn_stat = {}
        self.ipop_state = None

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        # new CBTs request for services from other modules by issuing CBTs; if no
        # services are required, the CBT is processed here only
        if not self.checkMapping(cbt):
            if cbt.action == 'PEER_STATE':
                stateCBT = self.registerCBT('Watchdog', 'QUERY_IPOP_STATE')
                self.CBTMappings[cbt.uid] = [stateCBT.uid]
                self.pendingCBT[cbt.uid] = cbt

            elif cbt.action == 'QUERY_PEER_STATE':
                self.registerCBT(cbt.initiator, 'QUERY_PEER_STATE_RESP', \
                        self.peers.get(cbt.data), cbt.uid)

            elif cbt.action == 'QUERY_PEER_LIST':
                self.registerCBT(cbt.initiator, 'QUERY_PEER_LIST_RESP', \
                        self.peers, cbt.uid)


            elif cbt.action == 'QUERY_CONN_STAT':
                self.registerCBT(cbt.initiator, 'QUERY_CONN_STAT_RESP', \
                        self.conn_stat.get(cbt.data), cbt.uid)

            elif cbt.action == 'DELETE_CONN_STAT':
                uid = cbt.data
                self.conn_stat.pop(uid, None)

            elif cbt.action == 'STORE_CONN_STAT':
                try:
                    self.conn_stat[cbt.data['uid']] = cbt.data['status']
                except KeyError:
                    log = "invalid STORE_CONN_STAT configuration"
                    self.registerCBT('Logger', 'warning', log)

            elif cbt.action == 'QUERY_IDLE_PEER_STATE':
                self.registerCBT(cbt.initiator, 'QUERY_IDLE_PEER_STATE_RESP', \
                        self.idle_peers.get(cbt.data), cbt.uid)

            elif cbt.action == 'STORE_IDLE_PEER_STATE':
                # store state of a given idle peer
                try:
                # cbt.data contains a {'uid': <uid>, 'idle_peer_state': <peer_state>} mapping
                    self.idle_peers[cbt.data['uid']] = cbt.data['idle_peer_state']
                except KeyError:

                    log = "invalid STORE_IDLE_PEER_STATE configuration"
                    self.registerCBT('Logger', 'warning', log)

            else:
                log = '{0}: unrecognized CBT {1} received from {2}'\
                        .format(cbt.recipient, cbt.action, cbt.initiator)
                self.registerCBT('Logger', 'warning', log)

        # CBTs that required servicing by other modules are processed here
        else:
            # get the source CBT of this request
            sourceCBT_uid = self.checkMapping(cbt)
            self.pendingCBT[cbt.uid] = cbt

            # wait until all requested services are complete
            if self.allServicesCompleted(sourceCBT_uid):
                if self.pendingCBT[sourceCBT_uid].action == 'PEER_STATE':
                    for key in self.CBTMappings[sourceCBT_uid]:
                        if self.pendingCBT[key].action == 'QUERY_IPOP_STATE_RESP':
                            self.ipop_state = self.pendingCBT[key].data

                    # process the original CBT when all values have been received
                    msg = self.pendingCBT[sourceCBT_uid].data
                    msg_type = msg.get("type", None)

                    if msg_type == "peer_state":
                        uid = msg["uid"]
                        if msg["status"] == "online":
                            self.peers_ip4[msg["ip4"]] = msg
                            self.peers_ip6[msg["ip6"]] = msg
                        else:
                            if uid in self.peers and\
                              self.peers[uid]["status"] == "online":
                                del self.peers_ip4[self.peers[uid]["ip4"]]
                                del self.peers_ip6[self.peers[uid]["ip6"]]
                        self.peers[uid] = msg
                        self.trigger_conn_request(msg)
                        if msg["status"] == "offline" or ("stats" not in msg):
                            self.peers[msg["uid"]] = msg
                            self.trigger_conn_request(msg)

    def timer_method(self):
        if self.use_visualizer and self.ipop_state is not None:
            self.visual_debugger()

    # visual debugger
    #   send information to the central visualizer
    def visual_debugger(self):
        new_msg = {
            "type": "Monitor",
            "uid": self.ipop_state["_uid"],
            "ip4": self.ipop_state["_ip4"],
            "state": "connected",
            "links": list(self.peers.keys())
        }

        self.registerCBT('CentralVisualizer', 'SEND_INFO', new_msg)

    def trigger_conn_request(self, peer):
        if ("fpr" not in peer) and peer["xmpp_time"] < \
            self.CMConfig["trigger_con_wait_time"]:
            self.conn_stat[peer["uid"]] = "req_sent"

            cbtdata = {
                "method": "con_req",
                "overlay_id": 1,
                "uid": peer["uid"],
                "data": self.ipop_state["_fpr"]
            }

            self.registerCBT('TincanSender', 'DO_SEND_MSG', cbtdata)

    def terminate(self):
        pass
