#!/usr/bin/env python
import sys
import time
import math
import json
import random
from collections import defaultdict
import controller.framework.fxlib as fxlib
from controller.framework.ControllerModule import ControllerModule


class BaseTopologyManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):

        super(BaseTopologyManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.ipop_state = None
        self.interval_counter = 0
        self.cv_interval = 5
        self.use_visualizer = False
        # need this to query for peer state since it is no longer maintained 
        # by tincan.
        self.peer_uids = defaultdict(int)

        self.uid = ""
        self.ip4 = ""

        # peers (linked nodes)
        self.peers = {}

        # links:
        #   self.links["successor"] = { uid: None }
        #   self.links["chord"]     = { uid: {"log_uid": log_uid, "ttl": ttl} }
        #   self.links["on_demand"] = { uid: {"ttl": ttl, "rate": rate} }
        self.links = {
            "successor": {}, "chord": {}, "on_demand": {}
        }

        self.log_chords = []

        self.max_num_links = self.CMConfig["num_successors"] + \
                             self.CMConfig["num_chords"] + \
                             self.CMConfig["num_on_demand"] + \
                             self.CMConfig["num_inbound"]

        # discovered nodes
        #   self.discovered_nodes is the list of nodes used by the successors policy
        #   self.discovered_nodes_srv is the list of nodes obtained from peer_state
        #       notifications
        self.discovered_nodes = []
        self.discovered_nodes_srv = []

        # p2p overlay state
        self.p2p_state = "started"

        # address mapping
        self.uid_ip4_table = {}
        self.ip4_uid_table = {}

        # populate uid_ip4_table and ip4_uid_table with all UID and IPv4
        # mappings within the /16 subnet
        parts = self.CFxHandle.queryParam("ip4").split(".")
        ip_prefix = parts[0] + "." + parts[1] + "."
        for i in range(0, 255):
            for j in range(0, 255):
                ip4 = ip_prefix + str(i) + "." + str(j)
                uid = fxlib.gen_uid(ip4)
                self.uid_ip4_table[uid] = ip4
                self.ip4_uid_table[ip4] = uid

        if 'use_central_visualizer' in self.CMConfig:
            self.use_visualizer = self.CMConfig["use_central_visualizer"]
        if "interval_central_visualizer" in self.CMConfig:
            self.cv_interval = self.CMConfig["interval_central_visualizer"]

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    ############################################################################
    # send message functions                                                   #
    ############################################################################

    # send message (through XMPP service)
    #   - msg_type = message type attribute
    #   - uid      = UID of the destination node
    #   - msg      = message
    def send_msg_srv(self, msg_type, uid, msg):
        cbtdata = {"method": msg_type, "overlay_id": 0, "uid": uid, "data": msg} #TODO overlay_id
        self.registerCBT('XmppClient', 'DO_SEND_MSG', cbtdata)

    # send message (through ICC)
    #   - uid = UID of the destination peer (a tincan link must exist)
    #   - msg = message
    def send_msg_icc(self, uid, msg):
        if uid in self.peers:
            if "ip6" in self.peers[uid]:
                cbtdata = {
                    "icc_type": "control",
                    "src_uid": self.uid,
                    "dst_uid": uid,
                    "msg": msg
                }
                self.registerCBT('TincanSender', 'DO_SEND_ICC_MSG', cbtdata)

    ############################################################################
    # connectivity functions                                                   #
    ############################################################################

    # request connection
    #   send a connection request
    #   - con_type = {successor, chord, on_demand}
    #   - uid      = UID of the target node
    def request_connection(self, con_type, uid):

        # send connection request
        data = {
            "fpr": self.ipop_state["_fpr"],
            "con_type": con_type
        }
        try:
            self.send_msg_srv("con_req", uid, json.dumps(data))
        except:
            self.registerCBT('Logger', 'info', "Exception in send_msg_srv con_req") 

        log = "sent con_req ({0}): {1}".format(con_type, uid)
        self.registerCBT('Logger', 'info', log)

    # respond connection
    #   create connection and return a connection acknowledgement and response
    #   - uid  = UID of the target node
    #   - data = information necessary to establish a link
    def respond_connection(self, con_type, uid, data):
        self.create_connection(uid, data)

        # send con_ack message
        data = {
            "fpr": self.ipop_state["_fpr"],
            "con_type": con_type
        }

        self.send_msg_srv("con_ack", uid, json.dumps(data))
        log = "sent con_ack to {0}".format(uid)
        self.registerCBT('Logger','info', log)

    # create connection
    #   establish a tincan link
    #   - uid  = UID of the target node
    #   - data = information necessary to establish a link
    def create_connection(self, uid, data):

        # FIXME check_collision was removed
        fpr_len = len(self.ipop_state["_fpr"])
        fpr = data[:fpr_len]
        nid = 0 # need this to make tincan fwd con_resp to controller.
        sec = self.CMConfig["sec"]
        cas = data[fpr_len + 1:]
        ip4 = self.uid_ip4_table[uid]

        con_dict = {'uid': uid, 'fpr': data, 'nid': nid, 'sec': sec, 'cas': cas}
        self.registerCBT('LinkManager', 'CREATE_LINK', con_dict)
        # Add uid to list for whom connection has been attempted.
        self.peer_uids[uid] = 1
        cbtdata = {"uid": uid, "ip4": ip4}
        self.registerCBT('TincanSender', 'DO_SET_REMOTE_IP', cbtdata)

    def linked(self, uid):
        if uid in self.peers:
            if self.peers[uid]["con_status"] == "online":
                return True
        return False

    # remove connection
    #   remove a link by peer UID
    #   - uid = UID of the peer
    def remove_connection(self, uid):
        if uid in self.peers:
            self.registerCBT('LinkManager', 'TRIM_LINK', uid)
            del self.peers[uid]

            for con_type in ["successor", "chord", "on_demand"]:
                if uid in self.links[con_type].keys():
                    del self.links[con_type][uid]

            log = "removed connection: {0}".format(uid)
            self.registerCBT('Logger', 'info', log)

    # clean connections
    #   remove peers with expired time-to-live attributes
    def clean_connections(self):
        # time-to-live attribute indicative of an offline link
        for uid in list(self.peers.keys()):
            if time.time() > self.peers[uid]["ttl"]:
                self.remove_connection(uid)

        # periodically call policy for link removal
        self.clean_chord()
        self.clean_on_demand()

    ############################################################################
    # add/remove link functions                                                #
    ############################################################################

    # add outbound link
    def add_outbound_link(self, con_type, uid, attributes):

        # add peer to link type
        self.links[con_type][uid] = attributes

        # peer is not in the peers list
        if uid not in self.peers.keys():

            # add peer to peers list
            self.peers[uid] = {
                "uid": uid,
                "ttl": time.time() + self.CMConfig["ttl_link_initial"],
                "con_status": "sent_con_req"
            }

            # connection request
            try:
                self.request_connection(con_type, uid)
            except:
                self.registerCBT('Logger', 'info', "Exception in request_connection") 

    # add inbound link
    def add_inbound_link(self, con_type, uid, fpr):

        
        # recvd con_req and sender is in peers_list - uncommon case

        if (uid in self.peers.keys()):
            log_msg = "AIL: Recvd con_req for peer in list from {0} status {1}".format(uid,self.peers[uid]["con_status"])
            self.registerCBT('Logger','info',log_msg)

            # if node has received con_req, re-respond (in case it was lost)
            if (self.peers[uid]["con_status"] == "recv_con_req"):
                log_msg = "AIL: Resending respond_connection to {0}".format(uid)
                self.registerCBT('Logger','info',log_msg)
                self.respond_connection(con_type, uid, fpr)
                return

            # else if node has sent con_request concurrently
            elif (self.peers[uid]["con_status"] == "sent_con_req"):
                # peer with smaller UID sends a response 
                if (self.uid < uid):
                    log_msg = "AIL: SmallerUID respond_connection to {0}".format(uid)
                    self.registerCBT('Logger','info',log_msg)
                    self.peers[uid] = {
                        "uid": uid,
                        "ttl": time.time() + self.CMConfig["ttl_link_initial"],
                        "con_status": "conc_sent_response"
                    }
                    self.respond_connection(con_type, uid, fpr)
                # peer with larger UID ignores 
                else:
                    log_msg = "AIL: LargerUID ignores from {0}".format(uid)
                    self.registerCBT('Logger','info',log_msg)
                    self.peers[uid] = {
                        "uid": uid,
                        "ttl": time.time() + self.CMConfig["ttl_link_initial"],
                        "con_status": "conc_no_response"
                    }
                return

            # if node was in any other state:
            # replied or ignored a concurrent send request:
            #    conc_no_response, conc_sent_response
            # or if status is online or offline, 
            # remove link and wait to try again
            else:
                log_msg = "AIL: Giving up, remove_connection from {0}".format(uid)
                self.registerCBT('Logger','info',log_msg)
                self.remove_connection(uid)

        # recvd con_req and sender is not in peers list - common case
        else:
            # add peer to peers list and set status as having received and
            # responded to con_req
            log_msg = "AIL: Recvd con_req for peer not in list {0}".format(uid)
            self.registerCBT('Logger','info',log_msg)
            self.peers[uid] = {
                "uid": uid,
                "ttl": time.time() + self.CMConfig["ttl_link_initial"],
                "con_status": "recv_con_req"
            }

            # connection response
            self.respond_connection(con_type, uid, fpr)
            

    # remove link
    def remove_link(self, con_type, uid):

        # remove peer from link type
        if uid in self.links[con_type].keys():
            del self.links[con_type][uid]

        # this peer does not have any outbound links
        if uid not in (list(self.links["successor"].keys()) + \
                       list(self.links["chord"].keys()) + \
                       list(self.links["on_demand"].keys())):

            # remove connection
            self.remove_connection(uid)

    ############################################################################
    # packet forwarding policy                                                 #
    ############################################################################

    # closer function
    #   tests if uid is successively closer to uid_B than uid_A
    def closer(self, uid_A, uid, uid_B):
        if (uid_A < uid_B) and ((uid_A < uid) and (uid <= uid_B)):
            return True  #0---A===B---N
        elif (uid_A > uid_B) and ((uid_A < uid) or (uid <= uid_B)):
            return True  #0===B---A===N
        return False

    # forward packet
    #   forward a packet across ICC
    #   - fwd_type = {
    #       exact   = intended specifically to the destination node,
    #       closest = intended to the node closest to the designated node
    #     }
    #   - dst_uid  = UID of the destination or designated node
    #   - msg      = message in transit
    #   returns true if this packet is intended for the calling node
    def forward_msg(self, fwd_type, dst_uid, msg):

        # find peer that is successively closest to and less-than-or-equal-to
        # the designated UID
        nxt_uid = self.uid
        for peer in self.peers.keys():
            if self.linked(peer):
                if self.closer(nxt_uid, peer, dst_uid):
                    nxt_uid = peer

        # packet is intended specifically to the destination node
        if fwd_type == "exact":

            # this is the destination uid
            if self.uid == dst_uid:
                return True

            # this is the closest node but not the destination; drop packet
            elif self.uid == nxt_uid:
                return False

        # packet is intended to the node closest to the designated node
        elif fwd_type == "closest":

            # this is the destination uid or the node closest to it
            if self.uid == nxt_uid:
                return True

        # there is a closer node; forward packet to the next node
        self.send_msg_icc(nxt_uid, msg)
        return False

    ############################################################################
    # successors policy                                                        #
    ############################################################################
    # [1] A discovers nodes in the network
    #     A requests to link to the closest successive node B as A's successor
    # [2] B accepts A's link request, with A as B's inbound link
    #     B responds to link to A
    # [3] A and B are connected
    # [*] the link is terminated when A discovers and links to closer successive
    #     nodes, or the link disconnects
    # [*] A periodically advertises its peer list to its peers to help them
    #     discover nodes

    def add_successors(self):
        # sort nodes into rotary, unique list with respect to this UID
        nodes = sorted(set(list(self.links["successor"].keys()) + self.discovered_nodes))
        if self.uid in nodes:
            nodes.remove(self.uid)
        if max([self.uid] + nodes) != self.uid:
            while nodes[0] < self.uid:
                nodes.append(nodes.pop(0))

        # link to the closest <num_successors> nodes (if not already linked)
        for node in nodes[0:min(len(nodes), self.CMConfig["num_successors"])]:
            if node not in self.links["successor"].keys():
                try:
                    self.add_outbound_link("successor", node, None)
                except:
                   self.registerCBT('Logger', 'info', "Exception in add_outbound_link") 

        # reset list of discovered nodes
        del self.discovered_nodes[:]

    def remove_successors(self):

        # sort nodes into rotary, unique list with respect to this UID
        successors = sorted(self.links["successor"].keys())
        if max([self.uid] + successors) != self.uid:
            while successors[0] < self.uid:
                successors.append(successors.pop(0))

        # remove all linked successors not within the closest <num_successors> linked nodes
        # remove all unlinked successors not within the closest <num_successors> nodes
        num_linked_successors = 0
        for successor in successors:
            if self.linked(successor):
                num_linked_successors += 1

                # remove excess successors
                if num_linked_successors > self.CMConfig["num_successors"]:
                    self.remove_link("successor", successor)

    def advertise(self):
        # create list of linked peers
        peer_list = []
        for peer in self.peers.keys():
            if self.linked(peer):
                peer_list.append(peer)

        # send peer list advertisement to all peers
        new_msg = {
            "msg_type": "advertise",
            "src_uid": self.uid,
            "peer_list": peer_list
        }

        for peer in self.peers.keys():
            if self.linked(peer):
                self.send_msg_icc(peer, new_msg)

    ############################################################################
    # chords policy                                                            #
    ############################################################################
    # [1] A forwards a headless find_chord message approximated by a designated UID
    # [2] B discovers that it is the closest node to the designated UID
    #     B responds with a found_chord message to A
    # [3] A requests to link to B as A's chord
    # [4] B accepts A's link request, with A as B's inbound link
    #     B responds to link to A
    # [5] A and B are connected
    # [*] the link is terminated when the chord time-to-live attribute expires and
    #     a better chord was found or the link disconnects

    def find_chords(self):

        # find chords closest to the approximate logarithmic nodes 
        if len(self.log_chords) == 0:
            for i in reversed(range(self.CMConfig["num_chords"])):
                log_num = (int(self.uid, 16) + int(math.pow(2, 160-1-i))) % int(math.pow(2, 160))
                log_uid = "{0:040x}".format(log_num)
                self.log_chords.append(log_uid)

        # determine list of designated UIDs
        log_chords = self.log_chords
        for chord in self.links["chord"].values():
            if chord["log_uid"] in log_chords:
                log_chords.remove(chord["log_uid"])

        # forward find_chord messages to the nodes closest to the designated UID
        for log_uid in log_chords:

            # forward find_chord message
            new_msg = {
                "msg_type": "find_chord",
                "src_uid": self.uid,
                "dst_uid": log_uid,
                "log_uid": log_uid
            }

            self.forward_msg("closest", log_uid, new_msg)

    def add_chord(self, uid, log_uid):

        # if a chord associated with log_uid already exists, check if the found
        # chord is the same chord:
        # if they are the same then the chord is already the best one available
        # otherwise, remove the chord and link to the found chord
        for chord in list(self.links["chord"].keys()):
            if self.links["chord"][chord]["log_uid"] == log_uid:
                if chord == uid:
                    return
                else:
                    self.remove_link("chord", chord)

        # add chord link
        attributes = {
            "log_uid": log_uid,
            "ttl": time.time() + self.CMConfig["ttl_chord"]
        }

        self.add_outbound_link("chord", uid, attributes)

    def clean_chord(self):

        if not self.links["chord"].keys():
            return

        # find chord with the oldest time-to-live attribute
        uid = min(self.links["chord"].keys(), key=lambda u: (self.links["chord"][u]["ttl"]))

        # time-to-live attribute has expired: determine if a better chord exists
        if time.time() > self.links["chord"][uid]["ttl"]:

            # forward find_chord message
            new_msg = {
                "msg_type": "find_chord",
                "src_uid": self.uid,
                "dst_uid": self.links["chord"][uid]["log_uid"],
                "log_uid": self.links["chord"][uid]["log_uid"]
            }

            self.forward_msg("closest", self.links["chord"][uid]["log_uid"], new_msg)

            # extend time-to-live attribute
            self.links["chord"][uid]["ttl"] = time.time() + self.CMConfig["ttl_chord"]

    ############################################################################
    # on-demand links policy                                                   #
    ############################################################################
    # [1] A is forwarding packets to B
    #     A immediately requests to link to B, with B as A's on-demand link
    # [2] B accepts A's link request, with A as B's inbound link
    # [3] A and B are connected
    #     B responds to link to A
    # [*] the link is terminated when the transfer rate is below some threshold
    #     until the on-demand time-to-live attribute expires or the link
    #     disconnections

    def add_on_demand(self, uid):

        if len(self.links["on_demand"].keys()) < self.CMConfig["num_on_demand"]:

            if uid not in self.links["on_demand"].keys():

                # add on-demand link
                attributes = {
                    "ttl": time.time() + self.CMConfig["ttl_on_demand"],
                    "rate": 0
                }
                self.add_outbound_link("on_demand", uid, attributes)

    def clean_on_demand(self):
        for uid in list(self.links["on_demand"].keys()):

            # rate exceeds threshold: increase time-to-live attribute
            if self.links["on_demand"][uid]["rate"] >= self.CMConfig["threshold_on_demand"]:
                self.links["on_demand"][uid]["ttl"] = time.time() + self.CMConfig["ttl_on_demand"]

            # rate is below theshold and the time-to-live attribute expired: remove link
            elif time.time() > self.links["on_demand"][uid]["ttl"]:
                self.remove_link("on_demand", uid)

    ############################################################################
    # inbound links policy                                                     #
    ############################################################################

    def add_inbound(self, con_type, uid, fpr):

        if con_type == "successor":
            self.add_inbound_link(con_type, uid, fpr)

        elif con_type in ["chord", "on_demand"]:
            if len(self.peers.keys()) < self.max_num_links:
                self.add_inbound_link(con_type, uid, fpr)

    ############################################################################
    # service notifications                                                    #
    ############################################################################

    def processCBT(self, cbt):

        # tincan control messages
        if cbt.action == "TINCAN_CONTROL":
            msg = cbt.data
            msg_type = msg.get("type", None)

            # update local state
            if msg_type == "local_state":
                self.ipop_state = msg
                self.uid = msg["_uid"]
                self.ip4 = msg["_ip4"]

            # update peer list
            elif msg_type == "peer_state":

                if msg["uid"] in self.peers:
                    # preserve ttl and con_status attributes
                    ttl = self.peers[msg["uid"]]["ttl"]
                    con_status = self.peers[msg["uid"]]["con_status"]

                    # update ttl attribute
                    if "online" == msg["status"]:
                        ttl = time.time() + self.CMConfig["ttl_link_pulse"]

                    # update peer state
                    self.peers[msg["uid"]] = msg
                    self.peers[msg["uid"]]["ttl"] = ttl
                    self.peers[msg["uid"]]["con_status"] = con_status

                    if msg["uid"] in self.links["on_demand"].keys():
                        if "stats" in msg:
                            self.links["on_demand"][msg["uid"]]["rate"] = msg["stats"][0]["sent_bytes_second"]

            # handle connection status
            elif msg_type == "con_stat":

                if msg["uid"] in self.peers:
                    self.peers[msg["uid"]]["con_status"] = msg["data"]

            # handle connection response
            elif msg_type == "con_resp":
                fpr_len = len(self.ipop_state["_fpr"])
                my_fpr = msg["data"][:fpr_len]
                my_cas = msg["data"][fpr_len + 1:]
                data = msg["data"]
                target_uid = msg["uid"]
                self.send_msg_srv("con_resp",target_uid,data)
                log = "recv con_resp from Tincan for {0}".format(msg["uid"])
                self.registerCBT('Logger', 'info', log)           
                              
                '''self.create_connection(msg["uid"], msg["data"])

                log = "recv con_resp: {0}".format(msg["uid"])
                self.registerCBT('Logger', 'info', log)'''

            
                
        # handle CBT's from XmppClient
        elif cbt.action == "XMPP_MSG":
            msg = cbt.data
            msg_type = msg.get("type", None)
            
            # handle connection request
            if msg_type == "con_req":
                msg["data"] = json.loads(msg["data"])
                log = "recv con_req ({0}): {1}".format(msg["data"]["con_type"], msg["uid"])
                self.registerCBT('Logger', 'info', log)
                self.add_inbound(msg["data"]["con_type"], msg["uid"],
                                    msg["data"]["fpr"])

            # handle connection acknowledgement
            elif msg_type == "con_ack":
                msg["data"] = json.loads(msg["data"])
                log = "recv con_ack ({0}): {1}".format(msg["data"]["con_type"], msg["uid"])
                self.registerCBT('Logger', 'info', log)
                self.create_connection(msg["uid"], msg["data"]["fpr"])
                
            # handle ping message
            elif msg_type == "ping":
                # add source node to the list of discovered nodes
                self.discovered_nodes.append(msg["uid"])
                # reply with a ping response message
                self.send_msg_srv("ping_resp", msg["uid"], self.uid)
                log = "recv ping: {0}".format(msg["uid"])
                self.registerCBT('Logger', 'info', log)

            # handle ping response
            elif msg_type == "ping_resp":
                # add source node to the list of discovered nodes
                self.discovered_nodes.append(msg["uid"])
                log = "recv ping_resp: {0}".format(msg["uid"])
                self.registerCBT('Logger', 'info', log)
                
            # handle peer_con_resp sent by peer   
            elif msg_type == "peer_con_resp":
                self.create_connection(msg["uid"], msg["data"])
                log = "recv con_resp: {0}".format(msg["uid"])
                self.registerCBT('Logger', 'info', log)
                
            # Handle xmpp advertisements   
            elif msg_type == "xmpp_advertisement":
                self.discovered_nodes_srv.append(msg["data"])
                self.discovered_nodes_srv = list(set(self.discovered_nodes_srv))
                log = "recv xmpp_advt: {0}".format(msg["uid"])
                self.registerCBT('Logger', 'info', log)
                
        # handle and forward tincan data packets
        elif cbt.action == "TINCAN_PACKET":

            data = cbt.data

            # ignore packets when not connected to the overlay
            if self.p2p_state != "connected":
                return

            # extract the source uid and destination uid
            # XXX src_uid and dst_uid should be obtained from the header, but
            # sometimes the dst_uid is the null uid
            # FIXME sometimes an irrelevant ip4 address obtained
            src_ip4 = '.'.join(str(int(i, 16)) for i in [data[132:140][i:i+2] for i in range(0, 8, 2)])
            dst_ip4 = '.'.join(str(int(i, 16)) for i in [data[140:148][i:i+2] for i in range(0, 8, 2)])

            try:
                src_uid = self.ip4_uid_table[src_ip4]
                dst_uid = self.ip4_uid_table[dst_ip4]
            except KeyError: # FIXME
                log = "recv illegal tincan_packet: src={0} dst={1}".format(src_ip4, dst_ip4)
                self.registerCBT('Logger', 'error', log)
                return

            # send forwarded message
            new_msg = {
                "msg_type": "forward",
                "src_uid": src_uid,
                "dst_uid": dst_uid,
                "packet": data
            }

            self.forward_msg("exact", dst_uid, new_msg)

            log = "sent tincan_packet (exact): {0}".format(dst_uid)
            self.registerCBT('Logger', 'info', log)

            # add on-demand link
            self.add_on_demand(dst_uid)

        # inter-controller communication (ICC) messages
        elif cbt.action == "ICC_CONTROL":
            msg = cbt.data
            msg_type = msg.get("msg_type", None)

            # advertisement of nearby nodes
            if msg_type == "advertise":
                self.discovered_nodes = list(set(self.discovered_nodes + msg["peer_list"]))

                log = "recv advertisement: {0}".format(msg["src_uid"])
                self.registerCBT('Logger', 'info', log)

            # handle forward packet
            elif msg_type == "forward":

                if self.forward_msg("exact", msg["dst_uid"], msg):
                    self.registerCBT('TincanSender', 'DO_INSERT_DATA_PACKET', msg["packet"])

                    log = "recv tincan_packet: {0}".format(msg["src_uid"])
                    self.registerCBT('Logger', 'info', log)

            # handle find chord
            elif msg_type == "find_chord":

                if self.forward_msg("closest", msg["dst_uid"], msg):

                    # forward found_chord message
                    new_msg = {
                        "msg_type": "found_chord",
                        "src_uid": self.uid,
                        "dst_uid": msg["src_uid"],
                        "log_uid": msg["log_uid"]
                    }

                    self.forward_msg("exact", msg["src_uid"], new_msg)

            # handle found chord
            elif msg_type == "found_chord":

                if self.forward_msg("closest", msg["dst_uid"], msg):

                    self.add_chord(msg["src_uid"], msg["log_uid"])

        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    ############################################################################
    # manage topology                                                          #
    ############################################################################

    def manage_topology(self):

        # obtain local state
        if self.p2p_state == "started":
            if not self.ipop_state:
                self.registerCBT('Logger', 'info', "p2p state: started")
                return
            else:
                self.p2p_state = "searching"
                log = "identified local state: {0}".format(self.ipop_state["_uid"])
                self.registerCBT('Logger', 'info', log)

        # discover nodes (from XMPP)
        if self.p2p_state == "searching":
            if not self.discovered_nodes_srv:
                self.registerCBT('Logger', 'info', "p2p state: searching")
                return
            else:
                self.p2p_state = "connecting"
                self.discovered_nodes = self.discovered_nodes_srv

        # connecting to the peer-to-peer network
        if self.p2p_state == "connecting":

            # if there are no discovered nodes, ping nodes
            if not self.peers and not self.discovered_nodes:
                self.ping()
                return

            log = "discovered nodes: {0}".format(self.discovered_nodes)
            self.registerCBT('Logger', 'info', log)

            # trim offline connections
            self.clean_connections()

            # attempt to bootstrap
            try:
                self.add_successors()
            except:
                self.registerCBT('Logger', 'info', "Exception in add_succesors")

            # wait until connected
            for peer in self.peers.keys():
                if self.linked(peer):
                    self.p2p_state = "connected"
                    break

        # connecting or connected to the IPOP peer-to-peer network; manage local topology
        if self.p2p_state == "connected":

            # trim offline connections
            self.clean_connections()

            # manage successors
            self.add_successors()
            self.remove_successors()

            # manage chords
            self.find_chords()

            # create advertisements
            self.advertise()

            if not self.peers:
                self.p2p_state = "connecting"
                self.registerCBT('Logger', 'info', "p2p state: DISCONNECTED")
            else:
                self.registerCBT('Logger', 'info', "p2p state: CONNECTED")

    def timer_method(self):
    
        try:
            self.interval_counter += 1

            # every <interval_management> seconds
            if self.interval_counter % self.CMConfig["interval_management"] == 0:

                # manage topology
                try:
                    self.manage_topology()
                except:
                    self.registerCBT('Logger', 'info', "Exception in MT BTM timer")

                # update local state and update the list of discovered nodes (from XMPP)
                self.registerCBT('TincanSender', 'DO_GET_STATE', '')
                for uid in self.peer_uids.keys():
                    self.registerCBT('TincanSender', 'DO_GET_STATE', uid)

            # every <interval_ping> seconds
            if self.interval_counter % self.CMConfig["interval_ping"] == 0:

                # ping to repair potential network partitions
                try:
                    self.ping()
                except:
                    self.registerCBT('Logger', 'info', "Exception in PING BTM timer")

            # every <interval_central_visualizer> seconds
            if self.use_visualizer and self.interval_counter % self.cv_interval == 0:
                # send information to central visualizer
                self.visual_debugger()
        except:
            self.registerCBT('Logger', 'info', "Exception in BTM timer")

    def ping(self):

        # send up to <num_pings> ping messages to random nodes to test if the
        # node is available
        rand_list = random.sample(range(0, len(self.discovered_nodes_srv)), 
                min(len(self.discovered_nodes_srv), self.CMConfig["num_pings"]))

        for i in rand_list:
            self.send_msg_srv("ping", self.discovered_nodes_srv[i], self.uid)

        # reset list of discovered nodes (from XMPP)
        del self.discovered_nodes_srv[:]

    def terminate(self):
        pass

    # visual debugger
    #   send information to the central visualizer
    def visual_debugger(self):

        # list only connected links
        new_msg = {
            "type": "BaseTopologyManager",
            "uid": self.uid,
            "ip4": self.ip4,
            "state": self.p2p_state,
            "links": {
                "successor": [], "chord": [], "on_demand": []
            }
        }

        for con_type in ["successor", "chord", "on_demand"]:
            for peer in self.links[con_type].keys():
                if self.linked(peer):
                    new_msg["links"][con_type].append(peer)

        self.registerCBT('CentralVisualizer', 'SEND_INFO', new_msg)
