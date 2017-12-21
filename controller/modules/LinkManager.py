# ipop-project
# Copyright 2016, University of Florida
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from controller.framework.ControllerModule import ControllerModule
import time
import json
import threading

class LinkManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(LinkManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.link_details = {}
        self.peers_lck = threading.Lock()
        # Member data to hold value for p2plink retries (value entered in config file)
        self.maxretries = self.CMConfig["MaxConnRetry"]

    def initialize(self):
        # Query UID and Tap Interface from TincanInterface
        tincanparams = self.CFxHandle.queryParam("TincanInterface", "Vnets")
        # Iterate across the virtual networks to get UID and TAPName
        for k in range(len(tincanparams)):
            interface_name = tincanparams[k]["TapName"]
            self.link_details[interface_name] = {}
            self.link_details[interface_name]["xmpp_client_code"] = tincanparams[k]["XMPPModuleName"]
            self.link_details[interface_name]["uid"] = tincanparams[k]["uid"]
            # Attribute to store Peer2Peer link details
            self.link_details[interface_name]["peers"] = {}
            self.link_details[interface_name]["ipop_state"] = {}
            # Attribute to store p2p link with online as link status
            self.link_details[interface_name]["online_peer_uid"] = []
        # Iterate across Table to send Local Get State request to Tincan
        for interface_name in self.link_details.keys():
            msg = {"interface_name": interface_name, "MAC": ""}
            self.registerCBT('TincanInterface', 'DO_GET_STATE', msg)
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    # Forward cbt over XMPP
    def forward_cbt(self,interface_name,peer_uid,payload):
        cbtdata = {"uid": peer_uid, "data": payload, "interface_name":interface_name}
        self.registerCBT(self.link_details[interface_name]["xmpp_client_code"], "SIG_FORWARD_CBT",cbtdata)

    # send message (through ICC)
    #   - uid = UID of the destination peer (a tincan link must exist)
    #   - msg = message
    def send_msg_icc(self, uid, msg, interface_name):
        # Check whether the UID exits in Peer Table
        if uid in self.link_details[interface_name]["peers"].keys():
            cbtdata = {
                    "src_uid": self.link_details[interface_name]["ipop_state"]["_uid"],
                    "dst_uid": uid,
                    "dst_mac": self.link_details[interface_name]["peers"][uid]["mac"],
                    "msg": msg,
                    "interface_name": interface_name
            }
            self.registerCBT('TincanInterface', 'DO_SEND_ICC_MSG', cbtdata)

############################################################################
# p2plink functions #
############################################################################

    # Request CAS Details from Peer UID
    def request_cas(self, uid, interface_name):
        link_data = self.link_details[interface_name]
        self.registerCBT('Logger', 'debug', "Peer Table::" + str(link_data["peers"]))
        # Set the initial Time To Live for p2plink(time within which its status has to change Online)
        ttl = time.time() + self.CMConfig["InitialLinkTTL"]
        '''
        if uid < self.link_details[interface_name]["ipop_state"]["_uid"]:
            self.registerCBT('Logger', 'info', "Dropping connection to smaller UID node")
            return
        '''
        # Check whether the request is for a new p2plink to the Peer
        if uid not in link_data["peers"].keys():
            # add peer to peers list
            link_data["peers"][uid] = {
                "uid": uid,
                "ttl": ttl,
                "status": "sent_link_req",
                "mac": ""
            }
        # check whether the p2plink request is already in progress but not in
        # connected state then allow p2plink creation to proceed
        elif link_data["peers"][uid]["status"] not in ["online", "offline"]:
            link_data["peers"][uid]["ttl"] = ttl
        else:
            return
        # Connection Request Details for Peer
        msg = {
            "peer_uid": uid,
            "interface_name": interface_name,
            "ip4": link_data["ipop_state"]["ip4"],
            "fpr": link_data["ipop_state"]["fpr"],
            "mac": link_data["mac"],
            "ttl": ttl
        }


        # Send the message via XMPP server to Peer node
        payload = dict(sender_uid=self.link_details[interface_name]["ipop_state"]["_uid"], dest_module="LinkManager",
                       action='RETRIEVE_CAS_FROM_TINCAN',core_data=json.dumps(msg))

        self.forward_cbt(interface_name, uid, payload)
        self.registerCBT('Logger', 'info', "Requested CAS details for peer UID:{0}".format(uid))

    # Remove p2plink specified by input UID
    def remove_p2plink(self, uid, interface_name):
        peer_details = self.link_details[interface_name]["peers"]
        # Check whether the request for an existing p2plink for UID if NO drop the request
        if uid in peer_details.keys():
            # Check whether the p2plink state is either Online or Offline
            if peer_details[uid]["status"] in ["online", "offline"]:
                if "mac" in list(peer_details[uid].keys()):
                    mac = peer_details[uid]["mac"]
                    if mac is not None and mac != "":
                        msg = {"interface_name": interface_name, "uid": uid, "MAC": mac}
                        self.registerCBT('TincanInterface', 'DO_TRIM_LINK', msg)
            del peer_details[uid]
            self.registerCBT('Logger', 'info', "Removed Connection to Peer UID: {0}".format(uid))

    #  remove peers with expired time-to-live attributes
    def clean_p2plinks(self, interface_name):
        # time-to-live attribute indicative of an offline link
        links = self.link_details[interface_name]
        # for uid in list(self.ipop_interface_details[interface_name]["peers"].keys()):
        for uid in links["peers"].keys():
            # check whether the time to link has expired
            if time.time() > links["peers"][uid]["ttl"]:
                log = "Time to Live expired going to remove peer: {0}".format(uid)
                self.registerCBT('Logger', 'info', log)
                self.remove_p2plink(uid, interface_name)

    # Get CAS details from Tincan
    def send_casdetails(self, uid, data, interface_name):
            peer = self.link_details[interface_name]["peers"]
            # Get CAS Response Message to Peer
            response_msg = {
                "uid": uid,
                "interface_name": interface_name,
                "fpr": self.link_details[interface_name]["ipop_state"]["fpr"],
                "cas": data["cas"],
                "ip4": self.link_details[interface_name]["ipop_state"]["ip4"],
                "mac": self.link_details[interface_name]["mac"],
                "peer_mac": data["peer_mac"]
            }

            # If CAS is requested for Peer which is already present in the Table
            if uid in peer.keys():
                log_msg = "Received CAS from Tincan for peer {0} in list.".format(uid)
                self.registerCBT('Logger', 'info', log_msg)
                # Setting Time To Live for the peer2peer link
                ttl = time.time() + self.CMConfig["InitialLinkTTL"]
                # if node has received CAS details, re-respond (in case it was lost)
                if peer[uid]["status"] == "recv_cas_details":
                    log_msg = "Resending CAS details to peer UID: {0}".format(uid)
                    self.registerCBT('Logger', 'info', log_msg)
                    response_msg["ttl"] = ttl
                    payload = dict(sender_uid=self.link_details[interface_name]["ipop_state"]["_uid"],
                                   dest_module="LinkManager",
                                   action='CREATE_P2PLINK', core_data=json.dumps(response_msg))
                    self.forward_cbt(interface_name, uid, payload)
                # else if node has sent p2plinkrequest concurrently
                elif peer[uid]["status"] == "sent_link_req":
                    # peer with Bigger UID sends a response
                    # if (self.link_details[interface_name]["ipop_state"]["_uid"] > uid):
                    self.registerCBT('Logger', 'info', "Sending CAS details to peer UID:{0}".format(uid))
                    peer[uid] = {
                        "uid": uid,
                        "ttl": ttl,
                        "status": "sent_casdetails",
                        "mac": data["peer_mac"]
                    }
                    response_msg["ttl"] = ttl
                    payload = dict(sender_uid=self.link_details[interface_name]["ipop_state"]["_uid"],
                                   dest_module="LinkManager",
                                   action='CREATE_P2PLINK', core_data=json.dumps(response_msg))
                    self.forward_cbt(interface_name, uid, payload)
                elif peer[uid]["status"] == "offline":
                    # If the CAS has been requested for a peer UID but it is inprogress
                    if "linkretrycount" not in peer[uid].keys():
                        peer[uid]["linkretrycount"] = 1
                        response_msg["ttl"] = ttl
                        payload = dict(sender_uid=self.link_details[interface_name]["ipop_state"]["_uid"],
                                       dest_module="LinkManager",
                                       action='CREATE_P2PLINK', core_data=json.dumps(response_msg))
                        self.forward_cbt(interface_name, uid, payload)
                    else:
                        # Check whether the peer2peer link retry has exceeded the max count
                        if peer[uid]["linkretrycount"] < self.maxretries:
                            peer[uid]["linkretrycount"] += 1
                            # Updating Connection Manager Table
                            peer[uid] = {
                                "uid": uid,
                                "ttl": ttl,
                                "status": "sent_response",
                                "mac": data["peer_mac"]
                            }
                            self.registerCBT('Logger', 'info', "Sending CAS details to peer UID:{0}".format(uid))
                            response_msg["ttl"] = ttl
                            payload = dict(src_uid=self.link_details[interface_name]["ipop_state"]["_uid"],
                                           dest_module="LinkManager",
                                           action='CREATE_P2PLINK', core_data=json.dumps(response_msg))
                            self.forward_cbt(interface_name, uid, payload)
                        else:
                            peer[uid]["linkretrycount"] = 0
                            log_msg = "Giving up after max retries, removing peer {0}".format(uid)
                            self.registerCBT('Logger', 'warning', log_msg)
                            # Remove the link as retry has exceeded max value
                            self.remove_p2plink(uid, interface_name)
                            # Send CAS details for fresh p2plink
                            # self.send_msg_srv("sent_peer_casdetails", uid, json.dumps(response_msg), interface_name)
                    # if node was in any other state replied or ignored a concurrent
                    # send request [conc_no_response, conc_sent_response]
                    # or if status is online or offline, remove link and wait to try again
                else:
                    if peer[uid]["status"] in ["sent_casdetails", "no_response", "recv_cas_details"]:
                        self.registerCBT('Logger', 'info', "Giving up, remove peer {0}".format(uid))
                        self.remove_p2plink(uid, interface_name)
            else:
                # add peer to peers list and set status as having received and
                # responded to p2plink request with CAS details
                log_msg = "Received CAS from Tincan for peer {0} in list.".format(uid)
                self.registerCBT('Logger', 'info', log_msg)
                # if self.link_details[interface_name]["ipop_state"]["_uid"] > uid:
                ttl = time.time() + self.CMConfig["InitialLinkTTL"]
                peer[uid] = {
                    "uid": uid,
                    "ttl": ttl,
                    "status": "recv_cas_details",
                    "mac": data["peer_mac"]
                }
                response_msg["ttl"] = ttl
                payload = dict(sender_uid=self.link_details[interface_name]["ipop_state"]["_uid"],
                               dest_module="LinkManager",
                               action='CREATE_P2PLINK', core_data=json.dumps(response_msg))
                self.forward_cbt(interface_name, uid, payload)

    # Create Peer2Peer Link via Tincan
    def create_p2plink(self, uid, interface_name, msg):
        peer_mac = msg["data"]["mac"]

        # Create an entry in Conn Manager Table for Peer if does not exists
        if uid not in self.link_details[interface_name]["peers"].keys():
            self.link_details[interface_name]["peers"][uid] = {}
        # Update Time To Live for the Link
        self.link_details[interface_name]["peers"][uid]["ttl"] = time.time() + self.CMConfig[
            "InitialLinkTTL"]
        self.link_details[interface_name]["peers"][uid]["mac"] = peer_mac
        self.registerCBT('Logger', 'info', "Received CAS from Peer ({0})".format(uid))
        # Send the Create Connection request to Tincan Interface
        self.registerCBT('TincanInterface', 'DO_CREATE_LINK', msg)

    # Advertise all Online Peer to each node in the network
    def advertise_p2plinks(self, interface_name):
        # create list of linked peers
        peer_list = self.link_details[interface_name]["online_peer_uid"]
        new_msg = {
            "msg_type": "advertise",
            "src_uid": self.link_details[interface_name]["ipop_state"]["_uid"],
            "peer_list": peer_list
        }
        # send peer list advertisement to all peers
        for peer in peer_list:
            self.send_msg_icc(peer, new_msg, interface_name)

    def processCBT(self, cbt):
        try:
            self.peers_lck.acquire()
            if cbt.action == "REMOVE_LINK":
                self.remove_p2plink(cbt.data.get("uid"), cbt.data.get("interface_name"))
            elif cbt.action == "CREATE_LINK":
                self.request_cas(cbt.data.get("uid"), cbt.data.get("interface_name"))
            elif cbt.action == "RETRIEVE_CAS_FROM_TINCAN":
                msg = cbt.data
                uid = msg["uid"]
                msg["data"] = json.loads(msg["data"])
                self.registerCBT('Logger', 'debug', "Received peer {0} req to retrieve CAS details.".format(uid))
                self.registerCBT('TincanInterface', 'DO_GET_CAS', msg)
                # Request Peer CAS details for two way connection
                if uid not in self.link_details[msg["interface_name"]]["peers"].keys():
                    self.request_cas(uid, msg["interface_name"])
            elif cbt.action == "CREATE_P2PLINK":
                msg = cbt.data
                msg["data"] = json.loads(msg["data"])
                self.create_p2plink(msg["uid"], msg.get("interface_name"), msg)
            elif cbt.action == "SEND_CAS_DETAILS_TO_PEER":
                msg = cbt.data
                interface_name = msg["interface_name"]
                self.send_casdetails(msg["uid"], msg["data"], interface_name)
            elif cbt.action == "SEND_ICC_MSG":
                msg = cbt.data
                self.send_msg_icc(msg.get("dst_uid"), msg.get("msg"), msg.get("interface_name"))
            elif cbt.action == "GET_NODE_MAC_ADDRESS":
                interface_name = cbt.data.get("interface_name")
                if "mac" in self.link_details[interface_name].keys():
                    self.registerCBT(cbt.initiator, "NODE_MAC_ADDRESS",
                                     {"interface_name": interface_name, "localmac": self.link_details[interface_name]["mac"]})
                else:
                    self.registerCBT(cbt.initiator, "NODE_MAC_ADDRESS",
                                     {"interface_name": interface_name, "localmac": ""})
            elif cbt.action == "GET_LINK_DETAILS":
                interface_name = cbt.data["interface_name"]
                message = {}
                # Send Link details like Time to Live, Status and Peer MAC to initiator
                for peeruid, linkdetails in list(self.link_details[interface_name]["peers"].items()):
                    message.update({peeruid: {
                        "ttl": linkdetails["ttl"],
                        "status": linkdetails["status"],
                        "mac": linkdetails["mac"]
                    }})
                self.registerCBT(cbt.initiator, "RETRIEVE_LINK_DETAILS", {"interface_name": interface_name,
                                                                              "data": message})
            elif cbt.action == "TINCAN_RESPONSE":
                msg = cbt.data
                msg_type = msg.get("type", None)
                interface_name = msg["interface_name"]
                interface_details = self.link_details[interface_name]
                # update local state
                if msg_type == "local_state":
                    interface_details["ipop_state"] = msg
                    interface_details["mac"] = msg["mac"]
                    self.registerCBT("Logger", "info","LM Local Node Info UID:{0} MAC:{1} IP4: {2}" \
                      .format(msg["_uid"], msg["mac"], msg["ip4"]))
                    # update peer list
                elif msg_type == "peer_state":
                    uid = msg["uid"]
                    data = cbt.data
                    # check whether UID exits in LinkManager Tables
                    if uid in interface_details["peers"]:
                        # check whether TTL exits if not initialize it to current timestamp
                        if "ttl" not in interface_details["peers"][uid]:
                            interface_details["peers"][uid]["ttl"] = time.time()
                        # Variable to store TTL
                        ttl = interface_details["peers"][uid]["ttl"]

                        # Check whether the p2plink is online if yes extend its Time To Live
                        if "online" == data["status"]:
                            ttl = time.time() + self.CMConfig["LinkPulse"]
                            # If the p2plink has just turned Online added into the Online Peer List
                            if uid not in self.link_details[interface_name]["online_peer_uid"]:
                                self.link_details[interface_name]["online_peer_uid"].append(uid)
                        # Connection has been removed from Tincan clear Connection Manager Table
                        elif "unknown" == data["status"]:
                            del self.link_details[interface_name]["peers"][uid]
                            if uid in self.link_details[interface_name]["online_peer_uid"]:
                                self.link_details[interface_name]["online_peer_uid"].remove(uid)
                            return
                        else:
                            if uid in self.link_details[interface_name]["online_peer_uid"]:
                                self.link_details[interface_name]["online_peer_uid"].remove(uid)
                        # update peer state within BTM Tables
                        interface_details["peers"][uid].update(msg)
                        interface_details["peers"][uid]["ttl"] = ttl
                else:
                    log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                        .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                    self.registerCBT('Logger', 'warning', log)
            elif cbt.action == "GET_ONLINE_PEERLIST":
                interface_name = cbt.data["interface_name"]
                if "_uid" in self.link_details[interface_name]["ipop_state"].keys():
                    cbtdt = {'peerlist': self.link_details[interface_name]["online_peer_uid"],
                             'uid': self.link_details[interface_name]["ipop_state"]["_uid"],
                             'mac': self.link_details[interface_name]["mac"],
                             'interface_name': interface_name
                    }
                    # Send the Online PeerList to the Initiator of CBT
                    self.registerCBT(cbt.initiator, 'ONLINE_PEERLIST', cbtdt)
            else:
                log = 'Unrecognized CBT message {0} received from {1}.Data: {3}' \
                        .format(cbt.action, cbt.initiator, cbt.data)
                self.registerCBT('Logger', 'warning', log)
            self.peers_lck.release()
        except Exception as err:
            self.peers_lck.release()
            raise

    def timer_method(self):
        try:
            # Iterate across various virtual networks
            self.peers_lck.acquire()
            for interface_name in self.link_details.keys():
                self.registerCBT("Logger","debug","Peer Nodes:: {0}".format(self.link_details[interface_name]["peers"]))
                # Iterate over the Peer Table
                for peeruid in self.link_details[interface_name]["peers"].keys():
                    # Check whether the Peer MAC address has been obtained via XMPP
                    if self.link_details[interface_name]["peers"][peeruid]["mac"] != "":
                        message = {
                            "interface_name": interface_name,
                            "MAC": self.link_details[interface_name]["peers"][peeruid]["mac"],
                            "uid": peeruid
                        }
                        # Get P2P Link state
                        self.registerCBT('TincanInterface', 'DO_GET_STATE', message)
                        # Get P2P Link stats
                        self.registerCBT('TincanInterface', 'DO_QUERY_LINK_STATS', message)
                # Check whether Local Node details have been obtained from Tincan, if not issue local 
                # state message to Tincan
                if "_uid" not in self.link_details[interface_name]["ipop_state"].keys():
                    msg = {"interface_name": interface_name, "MAC": ""}
                    self.registerCBT('TincanInterface', 'DO_GET_STATE', msg)
                else:
                    self.clean_p2plinks(interface_name)
                    self.advertise_p2plinks(interface_name)
            self.peers_lck.release()
        except Exception as err:
            self.peers_lck.release()
            self.registerCBT('Logger', 'error', "Exception caught in LinkManager timer thread.\
                             Error: {0}".format(str(err)))

    def terminate(self):
        pass
