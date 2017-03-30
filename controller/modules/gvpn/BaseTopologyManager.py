from controller.framework.ControllerModule import ControllerModule
from controller.framework.CFx import CFX
import time,math,json,random,stun
from threading import Lock

global btmlock
btmlock = Lock()


class BaseTopologyManager(ControllerModule,CFX):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(BaseTopologyManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.interval_counter = 0
        self.cv_interval = 5
        self.ipop_interface_details={}
        self.sendcount = ""
        self.receivecount = ""

        self.max_num_links = self.CMConfig["NumberOfSuccessors"] + \
                             self.CMConfig["NumberOfChords"] + \
                             self.CMConfig["NumberOfOnDemand"] + \
                             self.CMConfig["NumberOfInbound"]
        
        self.maxretries = self.CMConfig["MaxConnRetry"]

        self.tincanparams = self.CFxHandle.queryParam("Tincan","Vnets")
        for k in range(len(self.tincanparams)):
            interface_name= self.tincanparams[k]["TapName"]
            self.ipop_interface_details[interface_name]                         = {}
            interface_details = self.ipop_interface_details[interface_name]
            interface_details["p2p_state"]            = "started"
            interface_details["discovered_nodes"]     = []
            interface_details["discovered_nodes_srv"] = []     #TO DO Remove this once dev complete
            interface_details["online_peer_uid"]      = []
            interface_details["cas"]                  = ""
            interface_details["mac"]                  = ""
            interface_details["peers"]                = {}
            interface_details["ipop_state"]           = None
            interface_details["ip_uid_table"]         = {}
            interface_details["uid_mac_table"]        = {}
            interface_details["mac_uid_table"]        = {}
            interface_details["xmpp_client_code"]     = self.tincanparams[k]["XMPPModuleName"]
        self.tincanparams = None


    def initialize(self):
        for interface_name in self.ipop_interface_details.keys():
            self.registerCBT(self.ipop_interface_details[interface_name]["xmpp_client_code"],"GetXMPPPeer","")
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def send_msg_srv(self, msg_type, uid, msg, interface_name):
        cbtdata = {"method": msg_type, "overlay_id": 0, "uid": uid, "data": msg, "interface_name": interface_name}
        self.registerCBT(self.ipop_interface_details[interface_name]["xmpp_client_code"], 'DO_SEND_MSG', cbtdata)

    # send message (through ICC)
    #   - uid = UID of the destination peer (a tincan link must exist)
    #   - msg = message
    def send_msg_icc(self, uid, msg, interface_name):
        if uid in self.ipop_interface_details[interface_name]["online_peer_uid"]:
            cbtdata = {
                        "src_uid": self.ipop_interface_details[interface_name]["ipop_state"]["_uid"],
                        "dst_uid": uid,
                        "dst_mac": self.ipop_interface_details[interface_name]["peers"][uid]["mac"],
                        "msg": msg,
                        "interface_name": interface_name
            }
            self.registerCBT("Logger","debug","ICC Message overlay" + str(cbtdata))
            self.registerCBT('TincanSender', 'DO_SEND_ICC_MSG', cbtdata)
        else:
            self.registerCBT("Logger", "warning", "Trying to send ICC message to Offline Peer {0}. Message:: {1}".format(uid,msg))

    def linked(self, uid, interface_name):
        peers = self.ipop_interface_details[interface_name]["peers"]
        if uid in peers.keys():  # if uid in self.peers:
            attributes = peers[uid].keys()
            if "con_status" in attributes:
                if peers[uid]["con_status"] == "online":
                    return True
            if "status" in attributes:
                if peers[uid]["status"] == "online":
                    return True
        return False

############################################################################
        # add/remove link functions                                                #
############################################################################

    # add outbound link
    def add_outbound_link(self, con_type, uid, attributes, interface_name):
        interface_details = self.ipop_interface_details[interface_name]
        # add peer to link type

        if uid < interface_details["ipop_state"]["_uid"]:
            self.registerCBT('Logger', 'info',"Dropping connection request to Node with SmallerUID. {0}".format(uid))
            return
        #self.registerCBT('Logger', 'debug', "peer::" + str(interface_details["peers"]))

        # Connection Request Message
        ttl = time.time() + self.CMConfig["InitialLinkTTL"]
        msg = {
                "con_type": con_type,
                "peer_uid": uid,
                "interface_name": interface_name,
                "ip4": interface_details["ipop_state"]["_ip4"],
                "fpr": interface_details["ipop_state"]["_fpr"],
                "mac": interface_details["mac"],
                "ttl": ttl
        }

        # peer is not in the peers list
        if uid not in interface_details["peers"].keys():
            # add peer to peers list

            interface_details["peers"][uid] = {
                 "uid": uid,
                 "ttl": ttl,
                 "con_status": "sent_con_req",
                 "con_type"  : [con_type],
                 "mac": ""
            }
            # Send connect message to ConnectionManager
            self.registerCBT("ConnectionManager","request_connection",msg)
        elif interface_details["peers"][uid]["con_status"] not in ["offline","online"]:
            interface_details["peers"][uid]["ttl"] = ttl
            self.registerCBT("ConnectionManager", "request_connection", msg)

############################################################################
        # inbound links policy                                                     #
############################################################################

    def add_inbound(self, con_type, uid, data, interface_name):
        if con_type == "successor":
            self.add_inbound_link(con_type, uid, data, interface_name)
        elif con_type in ["chord", "on_demand"]:
            if len(self.ipop_interface_details[interface_name]["peers"].keys()) < self.max_num_links:
                self.add_inbound_link(con_type, uid, data, interface_name)

    # add inbound link
    def add_inbound_link(self, con_type, uid, data, interface_name):
        # recvd con_req and sender is in peers_list - uncommon case
        peer = self.ipop_interface_details[interface_name]["peers"]
        response_msg = {
            "con_type" : con_type,
            "uid"      : uid,
            "interface_name" : interface_name,
            "fpr"      : self.ipop_interface_details[interface_name]["ipop_state"]["_fpr"],
            "cas"      : data["cas"],
            "ip4"      : self.ipop_interface_details[interface_name]["ipop_state"]["_ip4"],
            #"ip6"      : self.ipop_interface_details[interface_name]["ipop_state"]["_ip6"],
            "mac"      : self.ipop_interface_details[interface_name]["mac"],
            "peer_mac" : data["peer_mac"]
        }

        remove_link_msg = {
            "uid"  : uid,
            "interface_name" : interface_name
        }

        if (uid in peer.keys()):
            log_msg = "AIL: Recvd con_req for peer in list from {0} status {1}".format(uid, peer[uid][
                            "con_status"])
            self.registerCBT('Logger', 'info', log_msg)
            ttl = time.time() + self.CMConfig["InitialLinkTTL"]
            # if node has received con_req, re-respond (in case it was lost)
            if (peer[uid]["con_status"] == "recv_con_req"):
                log_msg = "AIL: Resending respond_connection to {0}".format(uid)
                self.registerCBT('Logger', 'info', log_msg)
                #self.respond_connection(con_type, uid, fpr, interface_name)
                response_msg["ttl"] = ttl
                self.registerCBT("ConnectionManager","respond_connection",response_msg)
                # else if node has sent con_request concurrently
            elif (peer[uid]["con_status"] == "sent_con_req"):
                # peer with Bigger UID sends a response
                if (self.ipop_interface_details[interface_name]["ipop_state"]["_uid"] > uid):
                    log_msg = "AIL: LargerUID respond_connection to {0}".format(uid)
                    self.registerCBT('Logger', 'info', log_msg)

                    peer[uid] = {
                                    "uid": uid,
                                    "ttl": ttl,
                                    "con_status": "conc_sent_response",
                                    "mac": data["peer_mac"]
                    }
                    #self.respond_connection(con_type, uid, fpr, interface_name)
                    response_msg["ttl"] = ttl
                    self.registerCBT("ConnectionManager", "respond_connection", response_msg)
                # peer with larger UID ignores
                else:
                    log_msg = "AIL: SmallerUID ignores from {0}".format(uid)
                    self.registerCBT('Logger', 'info', log_msg)
                    peer[uid] = {
                                    "uid": uid,
                                    "ttl": ttl,
                                    "con_status": "conc_no_response",
                                    "mac": data["peer_mac"]
                    }
                    return
            elif peer[uid]["con_status"] == "offline":
                if "connretrycount" not in peer[uid].keys():
                        peer[uid]["connretrycount"] = 0
                        response_msg["ttl"] = ttl
                        self.registerCBT("ConnectionManager", "respond_connection", response_msg)
                else:
                    if peer[uid]["connretrycount"] < self.maxretries:
                        peer[uid]["connretrycount"] += 1
                        peer[uid] = {
                            "uid": uid,
                            "ttl": ttl,
                            "con_status": "conc_sent_response",
                            "mac": data["peer_mac"]
                        }
                        log_msg = "AIL: Resending respond_connection to {0}".format(uid)
                        self.registerCBT('Logger', 'info', log_msg)
                        response_msg["ttl"] = ttl
                        self.registerCBT("ConnectionManager", "respond_connection", response_msg)
                    else:
                        peer[uid]["connretrycount"] = 0
                        log_msg = "AIL: Giving up after max conn retries, remove_connection from {0}".format(
                                        uid)
                        self.registerCBT('Logger', 'warning', log_msg)
                        #self.remove_connection(uid, interface_name)
                        if uid in self.ipop_interface_details[interface_name]["peers"]:
                            self.ipop_interface_details[interface_name]["peers"].pop(uid)
                            self.registerCBT("ConnectionManager","remove_connection",remove_link_msg)
            # if node was in any other state:
            # replied or ignored a concurrent send request:
            #    conc_no_response, conc_sent_response
            # or if status is online or offline,
            # remove link and wait to try again
            else:
                if peer[uid]["con_status"] in ["conc_sent_response","conc_no_response"]:
                    log_msg = "AIL: Giving up, remove_connection from {0}".format(uid)
                    self.registerCBT('Logger', 'info', log_msg)
                    #self.remove_connection(uid, interface_name)
                    if uid in self.ipop_interface_details[interface_name]["peers"]:
                        self.ipop_interface_details[interface_name]["peers"].pop(uid)
                        self.registerCBT("ConnectionManager", "remove_connection", remove_link_msg)
            # recvd con_req and sender is not in peers list - common case
        else:
            # add peer to peers list and set status as having received and
            # responded to con_req
            log_msg = "AIL: Recvd con_req for peer not in list {0}".format(uid)
            self.registerCBT('Logger', 'info', log_msg)
            if (self.ipop_interface_details[interface_name]["ipop_state"]["_uid"] > uid):

                ttl = time.time() + self.CMConfig["InitialLinkTTL"]
                peer[uid] = {
                                "uid": uid,
                                "ttl": ttl,
                                "con_status": "recv_con_req",
                                "mac": data["peer_mac"]
                }
                # connection response
                #self.respond_connection(con_type, uid, fpr, interface_name)
                response_msg["ttl"] = ttl
                self.registerCBT("ConnectionManager", "respond_connection", response_msg)

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

    def add_successors(self, interface_name):
        # sort nodes into rotary, unique list with respect to this UID
        interface_details = self.ipop_interface_details[interface_name]
        uid = interface_details["ipop_state"]["_uid"]

        #nodes = sorted(
                #set(list(interface_details["links"]["successor"].keys()) + interface_details["discovered_nodes"]))
        nodes = list(sorted(interface_details["discovered_nodes"]))

        if uid in nodes:
            nodes.remove(uid)
        if max([uid] + nodes) != uid:
            while nodes[0] < uid:
                nodes.append(nodes.pop(0))
	requested_nodes = []
        self.registerCBT('Logger', 'info', "Peer Nodes:" + str(interface_details["peers"]))
        # link to the closest <num_successors> nodes (if not already linked)
        for node in nodes[0:min(len(nodes), self.CMConfig["NumberOfSuccessors"])]:
            #if node not in interface_details["online_peer_uid"]:
            if self.linked(node,interface_name) == False:
                self.add_outbound_link("successor", node, None, interface_name)

        # establishing link from the smallest UID node in the network to the biggest UID in the network
        if min([uid] + nodes) == uid and len(nodes)>1:
            for node in list(reversed(nodes))[0:self.CMConfig["NumberOfSuccessors"]]:
                #if node not in interface_details["online_peer_uid"]:
                if self.linked(node, interface_name) == False and node not in requested_nodes:
                    self.add_outbound_link("successor", node, None, interface_name)

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

    def add_on_demand(self, uid, interface_name):
        #if len(self.ipop_interface_details[interface_name]["links"]["on_demand"].keys()) < \
            #self.CMConfig["num_on_demand"]:
            #if uid not in self.ipop_interface_details[interface_name]["links"]["on_demand"].keys():
        peerlist = list(self.ipop_interface_details[interface_name]["peers"].keys())
        if uid not in peerlist:
            # add on-demand link
            attributes = {
                        "ttl": time.time() + self.CMConfig["OnDemandLinkTTL"],
                        "rate": 0
            }
            self.add_outbound_link("on_demand", uid, attributes, interface_name)



############################################################################
        # packet forwarding policy                                                 #
############################################################################

    # closer function
    #   tests if uid is successively closer to uid_B than uid_A
    def closer(self, uid_A, uid, uid_B):
            if (uid_A < uid_B) and ((uid_A < uid) and (uid <= uid_B)):
                return True  # 0---A===B---N
            elif (uid_A > uid_B) and ((uid_A < uid) or (uid <= uid_B)):
                return True  # 0===B---A===N
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
    def forward_msg(self, fwd_type, dst_uid, msg, interface_name):
        # find peer that is successively closest to and less-than-or-equal-to the designated UID
        interface_details = self.ipop_interface_details[interface_name]
        uid = interface_details["ipop_state"]["_uid"]
        nxt_uid = uid
        for peer in sorted(interface_details["online_peer_uid"]):
            if self.linked(peer, interface_name):
                if peer == dst_uid:
                    nxt_uid = peer
                    break
                if self.closer(uid, peer, dst_uid):
                    nxt_uid = peer

            # packet is intended specifically to the destination node
        if fwd_type == "exact":
            # this is the destination uid
            if dst_uid == uid:  # if self.uid == dst_uid:
                 #self.send_msg_icc(nxt_uid, msg, interface_name)
                 return True

                # this is the closest node but not the destination; drop packet
            elif nxt_uid == uid:  # elif self.uid == nxt_uid:
                # check if atleast one online peer exists
                if len(interface_details["online_peer_uid"])>0:
                    nxt_uid = max(interface_details["online_peer_uid"])
                else:
                    return False

        # packet is intended to the node closest to the designated node
        elif fwd_type == "closest":
            #print("closest",nxt_uid, uid)  # this is the destination uid or the node closest to it
            if nxt_uid == uid:  # if self.uid == nxt_uid:
                #self.send_msg_icc(nxt_uid, msg, interface_name)
                return True

            # there is a closer node; forward packet to the next node
        self.send_msg_icc(nxt_uid, msg, interface_name)
        return False


    def getnearestnode(self,remoteuid,interface_name):
        interface_details = self.ipop_interface_details[interface_name]
        uid = interface_details["ipop_state"]["_uid"]
        nxt_uid = uid

        peerlist = sorted(list(interface_details["peers"].keys()))
        for peer in peerlist:
            if self.linked(peer, interface_name):
                if self.closer(uid, peer, remoteuid):
                    nxt_uid = peer
        if nxt_uid == uid:
            nxt_uid = max(peerlist)
        return nxt_uid


    def processCBT(self, cbt):
        msg = cbt.data
        if cbt.action == "peer_list":
            interface_name = msg.get("interface_name")
            xmpp_peer_list = msg.get("peer_list")

            # Update discovered_nodes_srv till state is not connected
            if self.ipop_interface_details[interface_name]["p2p_state"] =="searching":
                self.ipop_interface_details[interface_name]["discovered_nodes_srv"] = xmpp_peer_list
            else:
                if len(xmpp_peer_list)>0:
                    self.ipop_interface_details[interface_name]["discovered_nodes"]+=xmpp_peer_list
                    self.ipop_interface_details[interface_name]["discovered_nodes"]= \
                        list(set(self.ipop_interface_details[interface_name]["discovered_nodes"]))
                else:
                    self.ipop_interface_details[interface_name]["discovered_nodes_srv"] = []
                    self.ipop_interface_details[interface_name]["discovered_nodes"] = []
        elif cbt.action == "forward_msg":
            msg = cbt.data
            self.forward_msg(msg["fwd_type"],msg["dst_uid"],msg["data"],msg["interface_name"])

        elif cbt.action == "UpdateConnectionDetails":
            interface_name = msg.get("interface_name")
            msg_type = msg["msg_type"]
            uid = msg.get("uid")
            mac = msg.get("mac")
            if msg_type == "add_peer":
                self.ipop_interface_details[interface_name]["peers"][uid]={
                            "uid": uid,
                            "mac": mac,
                            "ttl": time.time()+self.CMConfig["InitialLinkTTL"],
                            "con_status": "offline"
                }
                if uid not in self.ipop_interface_details[interface_name]["online_peer_uid"]:
                    self.ipop_interface_details[interface_name]["online_peer_uid"].append(uid)
                self.ipop_interface_details[interface_name]["uid_mac_table"][uid] = [mac]
                self.ipop_interface_details[interface_name]["mac_uid_table"][mac] = uid
                self.registerCBT('Logger', 'debug', "inside add peer **********")
                #self.ipop_interface_details[interface_name]["peer_uids"][uid] = 1
            elif msg_type == "remove_peer":
                self.registerCBT('Logger', 'debug',"inside remove peer************")
                if uid in list(self.ipop_interface_details[interface_name]["peers"].keys()):
                    del  self.ipop_interface_details[interface_name]["peers"][uid]
                if uid in self.ipop_interface_details[interface_name]["online_peer_uid"]:
                    self.ipop_interface_details[interface_name]["online_peer_uid"].remove(msg["uid"])
                if uid in list(self.ipop_interface_details[interface_name]["uid_mac_table"].keys()):
                    maclist =  list(self.ipop_interface_details[interface_name]["uid_mac_table"][uid])
                    for mac in maclist:
                        del self.ipop_interface_details[interface_name]["mac_uid_table"][mac]
                    del self.ipop_interface_details[interface_name]["uid_mac_table"][uid]

            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator,cbt.data)
                self.registerCBT('Logger', 'warning', log)

        elif cbt.action == "XMPP_MSG":
            msg = cbt.data
            msg_type = msg.get("type", None)
            interface_name = msg["interface_name"]
            interface_details = self.ipop_interface_details[interface_name]

            # handle connection request
            if msg_type == "con_req":
                msg["data"] = json.loads(msg["data"])
                uid  = msg["uid"]
                log = "recv con_req ({0}): {1}".format(msg["data"]["con_type"], uid)
                self.registerCBT('Logger', 'debug', log)
                if uid < interface_details["ipop_state"]["_uid"]:
                    self.registerCBT('TincanSender', 'DO_GET_CAS', msg)

            # handle connection acknowledgement
            elif msg_type == "con_ack":
                msg["data"] = json.loads(msg["data"])
                log = "recv con_ack ({0}): {1}".format(msg["data"]["con_type"], msg["uid"])
                self.registerCBT('Logger', 'debug', log)

            # handle ping message
            elif msg_type == "ping":
                # add source node to the list of discovered nodes
                interface_details["discovered_nodes"].append(msg["uid"])
                interface_details["discovered_nodes"] = list(set(interface_details["discovered_nodes"]))

                if interface_details["p2p_state"] == "searching":
                    interface_details["discovered_nodes_srv"] = interface_details["discovered_nodes"]

                # reply with a ping response message
                self.send_msg_srv("ping_resp", msg["uid"], interface_details["ipop_state"]["_uid"],interface_name)
                log = "recv ping: {0}".format(msg["uid"])
                self.registerCBT('Logger', 'debug', log)

            # handle ping response
            elif msg_type == "ping_resp":
                # add source node to the list of discovered nodes
                interface_details["discovered_nodes"].append(msg["uid"])
                interface_details["discovered_nodes"] = list(set(interface_details["discovered_nodes"]))

                if interface_details["p2p_state"] == "searching":
                    interface_details["discovered_nodes_srv"] = interface_details["discovered_nodes"]
                log = "recv ping_resp from {0}".format(msg["uid"])
                self.registerCBT('Logger', 'debug', log)

            # Remove Offline peer node
            elif msg_type == "offline_peer":
                if msg["uid"] in interface_details["discovered_nodes"]:
                    interface_details["discovered_nodes"].remove(msg["uid"])
                if msg["uid"] in interface_details["discovered_nodes_srv"]:
                    interface_details["discovered_nodes_srv"].remove(msg["uid"])
                log = "removed peer from discovered node list {0}".format(msg["uid"])
                self.registerCBT('Logger', 'debug', log)

            # handle peer_con_resp sent by peer
            elif msg_type == "peer_con_resp":
                log = "recv con_resp: {0}".format(msg["uid"])
                self.registerCBT('Logger', 'debug', log)

            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator,cbt.data)
                self.registerCBT('Logger', 'warning', log)

        elif cbt.action == "TINCAN_CONTROL":
            msg = cbt.data
            msg_type = msg.get("type", None)
            interface_name  = msg["interface_name"]
            interface_details = self.ipop_interface_details[interface_name]
            # update local state
            if msg_type == "local_state":
                interface_details["ipop_state"] = msg
                interface_details["mac"] = msg["mac"]
                interface_details["mac_uid_table"][msg["mac"]] = msg["_uid"]
                if msg["_uid"] not in interface_details["uid_mac_table"].keys():
                    interface_details["uid_mac_table"][msg["_uid"]] = [msg["mac"]]
                self.registerCBT("Multicast","getlocalmacaddress",{"interface_name":interface_name, "localmac": msg["mac"]})
                self.registerCBT("Logger","info","Local Node Info UID:{0} MAC:{1} IP4: {2}".format(msg["_uid"],\
                    msg["mac"],msg["_ip4"]))
            # update peer list
            elif msg_type == "peer_state":
                uid = msg["uid"]
                interface_details["mac_uid_table"][msg["mac"]] = uid

                # Creating an entry for the peer in the UID_MAC_Table
                if uid not in interface_details["uid_mac_table"].keys():
                    if "unknown" != msg["status"]:
                        interface_details["uid_mac_table"][msg["uid"]] = [msg["mac"]]

                if uid in interface_details["peers"]:
                    # preserve ttl and con_status attributes
                    ttl = interface_details["peers"][uid]["ttl"]
                    connretry = 0
                    if "connretrycount" in interface_details["peers"][uid].keys():
                        connretry = interface_details["peers"][uid]["connretrycount"]
                    # update ttl attribute
                    if "online" == msg["status"]:
                        ttl = time.time() + self.CMConfig["LinkPulse"]
                        if uid not in interface_details["online_peer_uid"]:
                            interface_details["online_peer_uid"].append(uid)
                    elif "unknown" == msg["status"]:
                        del interface_details["peers"][uid]
                        if uid in interface_details["online_peer_uid"]:
                                interface_details["online_peer_uid"].remove(msg["uid"])
                        if uid in list(interface_details["uid_mac_table"].keys()):
                            mac_list = list(interface_details["uid_mac_table"][uid])
                            for mac in mac_list:
                                del interface_details["mac_uid_table"][mac]
                            del interface_details["uid_mac_table"][uid]
                        return
                    else:
                        if msg["uid"] in interface_details["online_peer_uid"]:
                            interface_details["online_peer_uid"].remove(uid)

                    # update peer state
                    interface_details["peers"][uid].update(msg)
                    interface_details["peers"][uid]["ttl"]          = ttl
                    interface_details["peers"][uid]["con_status"]   = msg["status"]
                    interface_details["peers"][uid]["connretrycount"] = connretry

                    update_link_attributes ={
                        "uid": uid,
                        "ttl" : ttl,
                        "stats": msg["stats"],
                        "interface_name" : interface_name,
                        "status" : msg["status"],
                        "mac": msg["mac"]
                    }

                    self.registerCBT("ConnectionManager", "update_link_attr",update_link_attributes)
                    #if msg["uid"] in interface_details["links"]["on_demand"].keys():
                        #if "stats" in msg:
                            #interface_details["links"]["on_demand"][msg["uid"]]["rate"] = msg["stats"][0]["sent_bytes_second"]
            elif msg_type == "con_ack":
                self.registerCBT('Logger', 'debug',
                                             "Received CAS from Tincan for UID {0}".format(msg["uid"]))
                interface_details["cas"] = msg["data"]["cas"]
                self.add_inbound(msg["data"]["con_type"], msg["uid"], msg["data"], interface_name)
                #interface_details["peer_uids"][msg["uid"]] = 1

            elif msg_type == "UpdateMACUIDIp":
                location    = msg.get("location")
                uid         = msg["uid"]
                localuid    = interface_details["ipop_state"]["_uid"]

                #check whether an entry exists for UID
                if uid not in list(interface_details["uid_mac_table"].keys()):
                    interface_details["uid_mac_table"][uid] = []

                self.registerCBT('Logger', 'info', 'UpdateMACUIDMessage:::' + str(msg))

                if uid not in interface_details["online_peer_uid"] and uid !=localuid:
                    nextuid = self.getnearestnode(uid,interface_name)
                    nextnodemac = interface_details["peers"][nextuid]["mac"]

                    '''
                    for destmac in list(msg["mac_ip_table"].keys()):
                        self.registerCBT('Logger', 'info', 'MAC_UID Table:::' + str(interface_details["mac_uid_table"]))

                        if destmac not in list(interface_details["mac_uid_table"].keys()):
                            message = {
                                "interface_name": interface_name,
                                "sourcemac": nextnodemac,
                                "destmac": [destmac]
                            }
                            self.registerCBT("TincanSender", "DO_INSERT_ROUTING_RULES", message)
                        else:
                            olduid = interface_details["mac_uid_table"][destmac]
                            if olduid != uid:
                                message = {
                                    "interface_name": interface_name,
                                    "sourcemac": nextnodemac,
                                    "destmac": [destmac]
                                }
                                self.registerCBT("TincanSender", "DO_INSERT_ROUTING_RULES", message)
                    '''

                for mac, ip in msg["mac_ip_table"].items():
                    if mac not in interface_details["uid_mac_table"][uid]:
                        interface_details["uid_mac_table"][uid].append(mac)
                    interface_details["ip_uid_table"].update({ip: uid})
                    interface_details["mac_uid_table"].update({mac: uid})


            elif msg_type == "GetOnlinePeerList":
                self.registerCBT('Logger', 'debug', 'Control inside BaseTopology Manager peerlist code')
                interface_name = cbt.data["interface_name"]
                interface_details = self.ipop_interface_details[interface_name]

                cbtdt = {'peerlist'       : interface_details["online_peer_uid"],
                         'uid'            : interface_details["ipop_state"]["_uid"],
                         'mac'            : interface_details["mac"],
                         'interface_name' : interface_name
                         }

                self.registerCBT('BroadCastForwarder', 'peer_list', cbtdt)

            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator,cbt.data)
                self.registerCBT('Logger', 'warning', log)

        elif cbt.action == "ICC_CONTROL":
            msg = cbt.data
            msg_type = msg.get("msg_type", None)
            interface_name = msg["interface_name"]
            # advertisement of nearby nodes
            if msg_type == "advertise":
                self.ipop_interface_details[interface_name]["discovered_nodes"] \
                            = list(set(self.ipop_interface_details[interface_name]["discovered_nodes"] + msg["peer_list"]))
                localuid = self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]
                if localuid in self.ipop_interface_details[interface_name]["discovered_nodes"]:
                    self.ipop_interface_details[interface_name]["discovered_nodes"].remove(localuid)

                log = "recv advertisement: {0}".format(msg["src_uid"])
                self.registerCBT('Logger', 'info', log)

                # handle forward packet
            elif msg_type == "forward":
                dst_uid = msg["dst_uid"]
                if dst_uid != self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]:
                    self.forward_msg("exact", msg["dst_uid"], msg, interface_name)
                else:
                    msg["interface_name"] = interface_name
                    if "datagram" in msg.keys():
                        data = msg.pop("datagram")
                        msg["dataframe"] = data
                        self.registerCBT('TincanSender', 'DO_INSERT_DATA_PACKET', msg)

            # handle find chord
            elif msg_type == "find_chord":
                if self.forward_msg("closest", msg["dst_uid"], msg, interface_name):
                    # Check whether the current node UID is bigger than the Chord UID
                    if msg["src_uid"] > self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]:
                        self.add_outbound_link("chord",msg["src_uid"],None,interface_name)
                    else:
                        # forward found_chord message
                        new_msg = {
                                "msg_type": "found_chord",
                                "src_uid": self.ipop_interface_details[interface_name]["ipop_state"]["_uid"],
                                "dst_uid": msg["src_uid"],
                                "log_uid": msg["log_uid"]
                        }

                        self.forward_msg("exact", msg["src_uid"], new_msg, interface_name)

            # handle found chord
            elif msg_type == "found_chord":

                if self.forward_msg("exact", msg["dst_uid"], msg, interface_name):
                    if msg["src_uid"] > self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]:
                        self.add_outbound_link("chord", msg["src_uid"],None, interface_name)

            elif msg_type == "add_on_demand":
                self.add_on_demand(msg["uid"],msg["interface_name"])

            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator,cbt.data)
                self.registerCBT('Logger', 'warning', log)

        elif cbt.action == "get_visualizer_data":
            for interface_name in self.ipop_interface_details.keys():
                local_uid = self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]
                local_ip  = self.ipop_interface_details[interface_name]["ipop_state"]["_ip4"]
                unmanaged_node_list = []
                for ip,uid in self.ipop_interface_details[interface_name]["ip_uid_table"].items():
                    if ip!=local_ip and uid == local_uid:
                        unmanaged_node_list.append(ip)
                new_msg = {
                    "interface_name": interface_name,
                    "uid": local_uid,
                    "ip4": local_ip,
                    "GeoIP": self.getGeoIP(self.ipop_interface_details[interface_name]["cas"]),
                    "mac": self.ipop_interface_details[interface_name]["mac"],
                    "state": self.ipop_interface_details[interface_name]["p2p_state"],
                    "macuidmapping": self.ipop_interface_details[interface_name]["uid_mac_table"],
                    "unmanagednodelist": unmanaged_node_list,
                    "sendcount": "",
                    "receivecount": "",
                }
                self.registerCBT("OverlayVisualizer","topology_details",new_msg)

            # handle and forward tincan data packets
        elif cbt.action == "TINCAN_PACKET":
            reqdata = cbt.data
            interface_name = reqdata["interface_name"]
            data = reqdata["dataframe"]
            interface_details = self.ipop_interface_details[interface_name]
            m_type = reqdata["m_type"]
            # ignore packets when not connected to the overlay
            if interface_details["p2p_state"] != "connected":
                return

            if m_type=="ARP":
                maclen = int(data[36:38], 16)
                iplen = int(data[38:40], 16)
                srcmacindex = 44 + 2 * maclen
                srcmac = data[44:srcmacindex]
                srcipindex = srcmacindex + 2 * iplen
                srcip = '.'.join(
                    str(int(i, 16)) for i in [data[srcmacindex:srcipindex][i:i + 2] for i in range(0, 8, 2)])
                destmacindex = srcipindex + 2 * maclen
                destmac = data[srcipindex:destmacindex]
                destipindex = destmacindex + 2 * iplen
                dst_ip = '.'.join(
                    str(int(i, 16)) for i in [data[destmacindex:destipindex][i:i + 2] for i in range(0, 8, 2)])
            else:
                src_ip = '.'.join(str(int(i, 16)) for i in [data[52:60][i:i + 2] for i in range(0, 8, 2)])
                dst_ip = '.'.join(str(int(i, 16)) for i in [data[60:68][i:i + 2] for i in range(0, 8, 2)])
                destmac, srcmac = data[0:12], data[12:24]

            ip4_uid_table = interface_details["ip_uid_table"]
            if dst_ip in list(ip4_uid_table.keys()):
                dst_uid = ip4_uid_table[dst_ip]
            elif destmac in interface_details["mac_uid_table"].keys():
                dst_uid = interface_details["mac_uid_table"][destmac]
            elif destmac == "FFFFFFFFFFFF":
                datapacket = {
                        "dataframe": data,
                        "interface_name": interface_name,
                        "type": "local"
                }
                self.registerCBT("BroadCastForwarder","BroadcastPkt",datapacket)
                return
            else:
                log = "recv illegal tincan_packet: src={0} dst={1}".format(srcmac, destmac)
                self.registerCBT('Logger', 'info', log)
                return

            # Message routing to one of the local node attached to this UID
            if dst_uid == interface_details["ipop_state"]["_uid"]:
                network_inject_message = {
                    "dataframe": data,
                    "interface_name": interface_name
                }
                self.registerCBT("TincanSender","DO_INSERT_DATA_PACKET",network_inject_message)
                return

            # send forwarded message
            new_msg = {
                "msg_type": "forward",
                "src_uid": interface_details["ipop_state"]["_uid"],
                "dst_uid": dst_uid,
                "datagram": data
            }

            self.forward_msg("exact", dst_uid, new_msg, interface_name)

            log = "sent tincan_packet (exact): {0}. Message: {1}".format(dst_uid,data)
            self.registerCBT('Logger', 'info', log)

            # add on-demand link
            #self.add_on_demand(dst_uid, interface_name)

        else:
            log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
            self.registerCBT('Logger', 'warning', log)

############################################################################
                 # manage topology  #
############################################################################

    def manage_topology(self, interface_name):
            log = "Inside Manager Topology"
            self.registerCBT('Logger', 'info', log)

            interface_details = self.ipop_interface_details[interface_name]
            self.registerCBT(interface_details["xmpp_client_code"], "GetXMPPPeer", "")

            if interface_details["p2p_state"] == "started":
                if not interface_details["ipop_state"]:
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: started")
                    return
                else:
                    interface_details["p2p_state"] = "searching"
                    log = "identified local state: {0}".format(interface_details["ipop_state"]["_uid"])
                    self.registerCBT('Logger', 'info', log)

            # discover nodes (from XMPP)
            if interface_details["p2p_state"] == "searching":
                if not interface_details["discovered_nodes_srv"]:
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: searching")
                    return
                else:
                    interface_details["p2p_state"] = "connecting"
                    interface_details["discovered_nodes"] = list(set(interface_details["discovered_nodes_srv"]))

            # connecting to the peer-to-peer network
            if interface_details["p2p_state"] == "connecting":
                # if there are no discovered nodes, ping nodes
                #if not interface_details["peers"] and not interface_details["discovered_nodes"]:
                    #self.ping(interface_name)
                    #return

                log = "discovered nodes: {0}".format(interface_details["discovered_nodes"])
                self.registerCBT('Logger', 'info', log)
                self.registerCBT('Logger', 'info', interface_name + " p2p state: connecting")
                # trim offline connections
                #self.clean_connections(interface_name)
                self.registerCBT('ConnectionManager', 'clean_connection', {"interface_name": interface_name})

                # attempt to bootstrap
                try:
                    self.add_successors(interface_name)
                except:
                    self.registerCBT('Logger', 'error', "Exception in add_successors")

                # wait until connected
                for peer in interface_details['peers'].keys():
                    if self.linked(peer, interface_name):
                        interface_details["p2p_state"] = "connected"
                        break

            # connecting or connected to the IPOP peer-to-peer network; manage local topology
            if interface_details["p2p_state"] == "connected":

                # trim offline connections
                #self.clean_connections(interface_name)

                self.registerCBT("ConnectionManager","clean_connection",{"interface_name":interface_name})

                # manage successors
                self.add_successors(interface_name)
                #self.remove_successors(interface_name)

                req_msg = {
                    "interface_name": interface_name,
                    "uid": self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]
                }
                self.registerCBT("ConnectionManager", "remove_successor", req_msg)

                # manage chords
                #self.find_chords(interface_name)
                self.registerCBT("ConnectionManager", "find_chord", req_msg)

                # create advertisements
                self.advertise(interface_name)

                self.registerCBT('Logger', 'info', "Online peers::" + str(interface_details["online_peer_uid"]))


                if not interface_details["online_peer_uid"]: #if not interface_details["peers"]:
                    interface_details["p2p_state"] = "connecting"
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: DISCONNECTED")
                else:
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: CONNECTED")

    def ping(self,interface_name=""):

        # send up to <num_pings> ping messages to random nodes to test if the
        # node is available
        if interface_name == "":
            interface_list = list(self.ipop_interface_details.keys())
            for i_name in interface_list:
                rand_list = random.sample(
                    range(0, len(self.ipop_interface_details[i_name]["discovered_nodes_srv"])),
                    min(len(self.ipop_interface_details[i_name]["discovered_nodes_srv"]),
                        self.CMConfig["NumberOfPingsToPeer"]))

                for i in rand_list:
                    self.send_msg_srv("ping", self.ipop_interface_details[i_name]["discovered_nodes_srv"][i],
                                      self.ipop_interface_details[i_name]["ipop_state"]["_uid"],interface_name=i_name)

                # reset list of discovered nodes (from XMPP)
                #self.ipop_interface_details[i_name]["discovered_nodes_srv"] = []
        else:
            rand_list = random.sample(range(0, len(self.ipop_interface_details[interface_name]["discovered_nodes_srv"])),
                    min(len(self.ipop_interface_details[interface_name]["discovered_nodes_srv"]), self.CMConfig["NumberOfPingsToPeer"]))

            for i in rand_list:
                self.send_msg_srv("ping", self.ipop_interface_details[interface_name]["discovered_nodes_srv"][i],
                                  self.ipop_interface_details[interface_name]["ipop_state"]["_uid"],interface_name)

            # reset list of discovered nodes (from XMPP)
            #self.ipop_interface_details[interface_name]["discovered_nodes_srv"]=[]

    def advertise(self,interface_name):
        # create list of linked peers
        peer_list = []
        for peer in self.ipop_interface_details[interface_name]["peers"].keys():
            if self.linked(peer,interface_name):
                peer_list.append(peer)

        # send peer list advertisement to all peers
        new_msg = {
            "msg_type": "advertise",
            "src_uid": self.ipop_interface_details[interface_name]["ipop_state"]["_uid"],
            "peer_list": peer_list
        }

        for peer in (self.ipop_interface_details[interface_name]["peers"]).keys():
            if self.linked(peer,interface_name):
                self.send_msg_icc(peer, new_msg,interface_name)


    def timer_method(self):
        try:
            self.interval_counter += 1
            # every <interval_management> seconds
            if self.interval_counter % self.CMConfig["TopologyRefreshInterval"] == 0:
                for interface_name in self.ipop_interface_details.keys():
                    # manage topology
                    try:
                        self.manage_topology(interface_name)
                    except Exception as error:
                        self.registerCBT('Logger', 'error', "Exception in MT BTM timer: " + str(error))


                    # update local state and update the list of discovered nodes (from XMPP)
                    if self.ipop_interface_details[interface_name]["p2p_state"] == "searching":
                        msg = {"interface_name": interface_name, "MAC": ""}
                        #msg = {"interface_name": interface_name, "uid": ""}
                        self.registerCBT('TincanSender', 'DO_GET_STATE', msg)

                    peer_list = list(self.ipop_interface_details[interface_name]["peers"].keys())

                    for uid in peer_list:
                        #message  = {"interface_name": interface_name, "uid": uid}
                        if self.ipop_interface_details[interface_name]["peers"][uid]["con_status"] != "sent_con_req":
                            message = {
                                "interface_name": interface_name,
                                "MAC": self.ipop_interface_details[interface_name]["peers"][uid]["mac"],
                                "uid": uid}
                            self.registerCBT('TincanSender', 'DO_GET_STATE', message)

                    # self.registerCBT('TincanSender', 'DO_ECHO', '')

            # every <interval_ping> seconds
            if self.interval_counter % self.CMConfig["PeerPingInterval"] == 0:
                # ping to repair potential network partitions
                try:
                    self.ping()
                except Exception as error_msg:
                    self.registerCBT('Logger', 'error', "Exception in PING BTM timer:" + str(error_msg))

        except Exception as err:
            self.registerCBT('Logger', 'error', "Exception in BTM timer:" + str(err))

    def getGeoIP(self,cas):
        stun_details =self.CFxHandle.queryParam("VirtualNetworkInitializer","Stun")[0].split(":")
        nat_type, external_ip, external_port = stun.get_ip_info(stun_host=stun_details[0], stun_port=int(stun_details[1]))
        return external_ip
        return " "

    def terminate(self):
        pass
