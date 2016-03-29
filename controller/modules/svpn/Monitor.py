import time
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule


class Monitor(ControllerModule):

    def __init__(self, CFxHandle, paramDict):

        super(Monitor, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict

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

        logCBT = self.CFxHandle.createCBT(initiator='Monitor',
                                          recipient='Logger',
                                          action='info',
                                          data="Monitor Loaded")
        self.CFxHandle.submitCBT(logCBT)

    def processCBT(self, cbt):

        # In case of a fresh CBT, request the required services
        # from the other modules, by issuing CBTs. If no services
        # from other modules required, process the CBT here only

        if(not self.checkMapping(cbt)):

            if(cbt.action == 'PEER_STATE'):

                # Storing peer state requires ipop_state, which
                # is requested by sending a CBT to Watchdog
                stateCBT = self.CFxHandle.createCBT(initiator='Monitor',
                                                    recipient='Watchdog',
                                                    action='QUERY_IPOP_STATE',
                                                    data="")
                self.CFxHandle.submitCBT(stateCBT)

                # Maintain a mapping of the source CBT and issued CBTs
                self.CBTMappings[cbt.uid] = [stateCBT.uid]

                # Put this CBT in pendingCBT dict, since it hasn't been
                # processed yet
                self.pendingCBT[cbt.uid] = cbt

            elif(cbt.action == 'QUERY_PEER_STATE'):

                # Respond to a CM, requesting state of a particular peer

                peer_uid = cbt.data
                cbt.action = 'QUERY_PEER_STATE_RESP'
                cbt.initiator, cbt.recipient = cbt.recipient, cbt.initiator
                cbt.data = self.peers.get(peer_uid)
                self.CFxHandle.submitCBT(cbt)

            elif(cbt.action == 'QUERY_PEER_LIST'):

                # Respond to a CM requesting state of a particular peer

                cbt.action = 'QUERY_PEER_LIST_RESP'
                cbt.initiator, cbt.recipient = cbt.recipient, cbt.initiator
                cbt.data = self.peers
                self.CFxHandle.submitCBT(cbt)

            elif(cbt.action == 'QUERY_CONN_STAT'):

                # Respond to a CM requesting conn_stat of a particular peer
                uid = cbt.data
                cbt.action = 'QUERY_CONN_STAT_RESP'
                cbt.initiator, cbt.recipient = cbt.recipient, cbt.initiator
                cbt.data = self.conn_stat.get(uid)
                self.CFxHandle.submitCBT(cbt)

            elif(cbt.action == 'DELETE_CONN_STAT'):

                # Delete conn_stat of a given peer on request from another CM
                uid = cbt.data
                self.conn_stat.pop(uid, None)

            elif(cbt.action == 'STORE_CONN_STAT'):

                # Store conn_stat of a given peer
                try:
                    self.conn_stat[cbt.data['uid']] = cbt.data['status']
                except KeyError:
                    logCBT = self.CFxHandle.createCBT(initiator='Monitor',
                                                      recipient='Logger',
                                                      action='warning',
                                                      data="Invalid "
                                                      "STORE_CONN_STAT"
                                                      " Configuration"
                                                      " from " + cbt.initiator)
                    self.CFxHandle.submitCBT(logCBT)

            elif(cbt.action == 'QUERY_IDLE_PEER_STATE'):

                # Respond to a CM requesting idle peer state
                idle_peer_uid = cbt.data
                cbt.action = 'QUERY_IDLE_PEER_STATE_RESP'
                cbt.initiator, cbt.recipient = cbt.recipient, cbt.initiator
                cbt.data = self.idle_peers.get(idle_peer_uid)
                self.CFxHandle.submitCBT(cbt)

            elif(cbt.action == 'STORE_IDLE_PEER_STATE'):

                # Store state of a given idle peer
                try:
                    # cbt.data is a dict with uid and idle_peer_state keys
                    self.idle_peers[cbt.data['uid']] = \
                                    cbt.data['idle_peer_state']
                except KeyError:

                    logCBT = self.CFxHandle.createCBT(initiator='Monitor',
                                                      recipient='Logger',
                                                      action='warning',
                                                      data="Invalid "
                                                      "STORE_IDLE_PEER_STATE"
                                                      " Configuration")
                    self.CFxHandle.submitCBT(logCBT)

            else:
                logCBT = self.CFxHandle.createCBT(initiator='Monitor',
                                                  recipient='Logger',
                                                  action='error',
                                                  data="Monitor: Unrecognized "
                                                  "CBT from: " + cbt.initiator)
                self.CFxHandle.submitCBT(logCBT)

        # Case when one of the requested service CBT comes back
        else:

            # Get the source CBT of this response CBT
            sourceCBT_uid = self.checkMapping(cbt)
            self.pendingCBT[cbt.uid] = cbt

            # If all the other services of this sourceCBT are also completed,
            # process CBT here. Else wait for other CBTs to arrive
            if(self.allServicesCompleted(sourceCBT_uid)):
                if(self.pendingCBT[sourceCBT_uid].action ==
                        'PEER_STATE'):

                    # Retrieve values from response CBTs
                    for key in self.CBTMappings[sourceCBT_uid]:
                        if(self.pendingCBT[key].action ==
                           'QUERY_IPOP_STATE_RESP'):
                            self.ipop_state = self.pendingCBT[key].data

                    # Process the source CBT, once all the required variables
                    # are extracted from the response CBTs

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
                        if msg["status"] == "offline" or "stats" not in msg:
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
            "links": self.peers.keys()
        }

        TincanCBT = self.CFxHandle.createCBT(initiator='Monitor',
                                             recipient='CentralVisualizer',
                                             action='SEND_INFO',
                                             data=new_msg)
        self.CFxHandle.submitCBT(TincanCBT)

    def trigger_conn_request(self, peer):
        if "fpr" not in peer and peer["xmpp_time"] < \
                self.CMConfig["trigger_con_wait_time"]:
            self.conn_stat[peer["uid"]] = "req_sent"

            cbtData = {
                "method": "con_req",
                "overlay_id": 1,
                "uid": peer["uid"],
                "data": self.ipop_state["_fpr"]
            }

            TincanCBT = self.CFxHandle.createCBT(initiator='Monitor',
                                                 recipient='TincanSender',
                                                 action='DO_SEND_MSG',
                                                 data=cbtData)
            self.CFxHandle.submitCBT(TincanCBT)

    def terminate(self):
        pass
