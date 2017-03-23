#!/usr/bin/env python
import sys
import json, ssl
import time
from controller.framework.ControllerModule import ControllerModule
import sleekxmpp
from collections import defaultdict
from sleekxmpp.xmlstream.stanzabase import ElementBase, ET, JID
from sleekxmpp.xmlstream import register_stanza_plugin
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.stanza.message import Message

py_ver = sys.version_info[0]

if py_ver == 3:
    import _thread as thread
else:
    import thread
log_level = "info"


# set up a new custom message stanza
class Ipop_Msg(ElementBase):
    namespace = 'Conn_setup'
    name = 'Ipop'
    plugin_attrib = 'Ipop'
    interfaces = set(('setup', 'payload', 'uid'))
    subinterfaces = interfaces


class XmppClient(ControllerModule, sleekxmpp.ClientXMPP):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        ControllerModule.__init__(self, CFxHandle, paramDict, ModuleName)
        # keeps track of last recvd advertisement and if node is active on XMPP.
        self.xmpp_peers = defaultdict(lambda: [0, False])
        # need to maintain uid<->jid mapping to route xmpp messages.
        self.uid_jid = {}
        # FullJID,Knows my UID,num(Correct advts recvd)
        self.jid_uid = defaultdict(lambda: ['', False, 1])
        self.xmpp_username = self.CMConfig.get("Username")
        self.xmpp_passwd = self.CMConfig.get("Password","None")
        self.xmpp_host = self.CMConfig.get("AddressHost")
        self.xmpp_port = self.CMConfig.get("Port")
        self.vpn_type = self.CFxHandle.queryParam("CFx","Model")
        self.interface_name  = self.CMConfig.get("TapName")
        self.uid = ""
        self.update_peerlist = False
        # time of last recvd xmpp advt.
        self.last_sent_advt = 0
        # keeps track of if xmpp advt recvd in interval
        self.xmpp_advt_recvd = True
        # Initial ADVT Delay
        self.INITIAL_ADVT_DELAY = 5
        # interval between sending advertisements
        self.advt_delay = self.INITIAL_ADVT_DELAY
        # Maximum delay between advertisements is 10 minutes
        self.MAX_ADVT_DELAY = 600
        # initialize the base Xmpp client class, handle login/authentication.
        if self.CMConfig.get("AuthenticationMethod") == "x509" and \
                (self.xmpp_username != None \
                         or self.xmpp_passwd != None):
            raise RuntimeError(
                "x509 Authentication Error: Username/Password in IPOP configuration file.")

        use_tls = True
        if self.CMConfig.get("AuthenticationMethod") == "x509":
            sleekxmpp.ClientXMPP.__init__(self, self.xmpp_host, self.xmpp_passwd, sasl_mech='EXTERNAL')
            self.ssl_version = ssl.PROTOCOL_TLSv1
            self.ca_certs = self.CMConfig.get("TrustStore")
            self.certfile = self.CMConfig.get("CertDirectory") + self.CMConfig.get("CertFile")
            self.keyfile = self.CMConfig.get("CertDirectory") + self.CMConfig.get("Keyfile")
        else:
            sleekxmpp.ClientXMPP.__init__(self, self.xmpp_username, self.xmpp_passwd, sasl_mech='PLAIN')
            if self.CMConfig.get("AcceptUntrustedServer") == True:
                self['feature_mechanisms'].unencrypted_plain = True
                use_tls = False
            else:
                self.ca_certs = self.CMConfig.get("TrustStore")

        # register a new plugin stanza and handler for it,
        # whenever a matching message will be received on
        # the xmpp stream , registered handler will be called.
        register_stanza_plugin(Message, Ipop_Msg)
        self.registerHandler(
            Callback('Ipop',StanzaPath('message/Ipop'),self.MsgListener))

        # Register event handler for session start
        self.add_event_handler("session_start", self.start)

        self.add_event_handler("roster_update", self.deletepeerjid)

        # populate uid_ip4_table and ip4_uid_table with all UID and IPv4
        # mappings within the /16 subnet

        if (self.vpn_type == "GroupVPN"):
            ipop_interfaces = self.CFxHandle.queryParam("Tincan","Vnets")
            for interface_details in ipop_interfaces:
                if interface_details["TapName"] == self.interface_name:
                    self.uid = interface_details["uid"]
        elif (self.vpn_type == "SocialVPN"):
            self.registerCBT('Watchdog', 'QUERY_IPOP_STATE')
        # Start xmpp handling thread
        self.xmpp_handler()

    # Triggered at start of XMPP session
    def start(self, event):
        try:
            self.get_roster()
            self.send_presence()
        except Exception as err:
            self.log("Exception in XMPPClient:".format(err), severity="error")
        # Add handler for incoming presence messages.
        self.add_event_handler("presence_available", self.handle_presence)
        self.add_event_handler("presence_unavailable", self.removepeerjid)

    # will need to handle presence, to keep track of who is online.
    def handle_presence(self, presence):
        presence_sender = presence['from']
        if self.xmpp_peers[presence_sender][1] == False:
            self.xmpp_peers[presence_sender] = [time.time(), True]
            self.log("presence received from {0}".format(presence_sender), severity=log_level)

    # Call Remove connection once the Peer has been deleted from the friend list(Roster)
    def deletepeerjid(self,message):
        try:
            self.log("XMPP server Message::"+str(message))
            for nodejid,data in message["roster"]["items"].items():
                if data["subscription"] == "remove":
                    for ele in self.jid_uid.keys():
                        tempjid = JID(ele)
                        jid = str(tempjid.user)+"@"+str(tempjid.domain)
                        if jid.find(str(nodejid)) !=-1:
                            node_uid = self.jid_uid[ele][0]
                            del self.jid_uid[ele]
                            del self.xmpp_peers[ele]
                            if node_uid in self.uid_jid.keys():
                                del self.uid_jid[node_uid]
                                self.update_peerlist = True
                                self.registerCBT("Logger","info","{0} has been deleted from the roster.".format(node_uid))
                                self.registerCBT("ConnectionManager","remove_connection",\
                                                 {"interface_name":self.interface_name,"uid":node_uid})
                                msg = {
                                    "uid": node_uid,
                                    "type": "offline_peer",
                                    "interface_name": self.interface_name
                                }
                                self.registerCBT("BaseTopologyManager", "XMPP_MSG", msg)

        except Exception as err:
            self.log("Exception in deletepeerjid method.{0}".format(err),severity="error")

    # Remove the Offline Peer from the internal dictionary
    def removepeerjid(self,message):
        peerjid = message["from"]
        self.log("Peer JID {0} offline".format(peerjid))
        if peerjid in self.xmpp_peers.keys():
            del self.xmpp_peers[peerjid]

        if peerjid in self.jid_uid.keys():
            uid = self.jid_uid[peerjid][0]
            del self.jid_uid[peerjid]
            if uid in self.uid_jid.keys():
                del self.uid_jid[uid]
                self.update_peerlist = True
                self.log("Removed Peer JID: {0} UID: {1} from the JID-UID and UID-JID Table".format(peerjid,uid))
                msg = {
                    "uid" : uid,
                    "type": "offline_peer",
                    "interface_name":self.interface_name
                }
                self.registerCBT("BaseTopologyManager","XMPP_MSG",msg)



    # This handler method listens for the matched messages on tehj xmpp stream,
    # extracts the setup and payload and takes suitable action depending on the
    # them.
    def MsgListener(self, msg):
        if self.uid == "":
            self.log("UID not yet received- Not Ready.")
            return
        # extract setup and content
        setup = str(msg['Ipop']['setup'])
        payload = str(msg['Ipop']['payload'])
        msg_type, target_uid, target_jid = setup.split("#")
        sender_jid = msg['from']

        if (msg_type == "regular_msg"):
            self.log("Recvd mesage from {0}".format(msg['from']), severity=log_level)
            self.log("Msg is {0}".format(payload), severity="debug")
        elif (msg_type == "xmpp_advertisement"):
            # peer_uid - uid of the node that sent the advt
            # target_uid - what it percieves as my uid

            try:
                peer_uid, target_uid = payload.split("#")
                if peer_uid != self.uid:
                    if peer_uid not in self.uid_jid.keys():
                        self.update_peerlist= True
                    # update last known advt reception time in xmpp_peers
                    self.xmpp_peers[sender_jid][0] = time.time()
                    self.uid_jid[peer_uid] = sender_jid
                    self.jid_uid[msg['from']][0] = peer_uid
                    # sender knows my uid, so I will not send an advert to him
                    if target_uid == self.uid:
                        self.jid_uid[msg['from']][1] = True
                        # recvd correct advertisement
                        self.jid_uid[msg['from']][2] += 1
                    else:
                        self.jid_uid[msg['from']][1] = False
                    msg = {}
                    msg["uid"] = peer_uid
                    msg["data"] = peer_uid
                    msg["type"] = "xmpp_advertisement"
                    msg["interface_name"] = self.interface_name
                    #if (self.vpn_type == "GroupVPN"):
                    #    self.registerCBT('BaseTopologyManager', 'XMPP_MSG', msg)
                    #elif (self.vpn_type == "SocialVPN"):
                    if (self.vpn_type == "SocialVPN"):
                        self.registerCBT('Watchdog', 'XMPP_MSG', msg)
                    # refresh xmpp advt recvd flag
                    self.xmpp_advt_recvd = True
                    self.log("recvd xmpp_advt from {0}".format(msg["uid"]), severity=log_level)
            except:
                self.log("advt_payload: {0}".format(payload), severity="error")

        # compare uid's here , if target uid does not match with mine do nothing.
        # have to avoid loop messages.
        if target_uid == self.uid:
            sender_uid, recvd_data = payload.split("#")
            # If I recvd XMPP msg from this peer, I should record his UID-JID & JID-UID
            if sender_uid not in self.uid_jid.keys():
                self.update_peerlist = True
            self.uid_jid[sender_uid] = sender_jid
            if (msg_type == "con_req"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "con_req"
                msg["interface_name"] = self.interface_name
                # send this CBT to BaseTopology Manager
                self.registerCBT('BaseTopologyManager', 'XMPP_MSG', msg)
                self.log("recvd con_req from {0}".format(msg["uid"]), severity=log_level)

            elif (msg_type == "con_resp"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "peer_con_resp"
                msg["interface_name"] = self.interface_name
                self.registerCBT('BaseTopologyManager', 'XMPP_MSG', msg)
                self.log("recvd con_resp from {0}".format(msg["uid"]), severity=log_level)

            elif (msg_type == "con_ack"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "con_ack"
                msg["interface_name"] = self.interface_name
                self.registerCBT('ConnectionManager', 'create_connection', msg)
                self.log("recvd con_ack from {0}".format(msg["uid"]), severity=log_level)

            elif (msg_type == "ping_resp"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "ping_resp"
                msg["interface_name"] = self.interface_name
                self.registerCBT('BaseTopologyManager', 'XMPP_MSG', msg)
                self.log("recvd ping_resp from {0}".format(msg["uid"]), severity=log_level)

            elif (msg_type == "ping"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "ping"
                msg["interface_name"] = self.interface_name
                self.registerCBT('BaseTopologyManager', 'XMPP_MSG', msg)
                self.log("recvd ping from {0}".format(msg["uid"]), severity=log_level)

    def sendMsg(self, peer_jid, setup_load=None, msg_payload=None):
        if (setup_load == None):
            setup_load = "regular_msg" + "#" + "None" + "#" + peer_jid.full
        else:
            setup_load = setup_load + "#" + peer_jid.full

        if py_ver != 3:
            setup_load = unicode(setup_load)

        if (msg_payload == None):
            content_load = "Hello there this is {0}".format(self.xmpp_username)
        else:
            content_load = msg_payload

        msg = self.Message()
        msg['to'] = peer_jid.bare
        msg['type'] = 'chat'
        msg['Ipop']['setup'] = setup_load
        msg['Ipop']['payload'] = content_load
        msg.send()
        self.log("Sent a message to  {0}".format(peer_jid), severity=log_level)

    def xmpp_handler(self):
        try:
            if (self.connect(address=(self.xmpp_host, self.xmpp_port))):
                thread.start_new_thread(self.process, ())
                self.log("Started XMPP handling", severity="debug")

        except:
            self.log("Unable to start XMPP handling thread-Check Internet connectivity/credentials.", severity='error')

    def log(self, msg, severity='info'):
        self.registerCBT('Logger', severity, msg)

    def initialize(self):
        self.log("{0} module Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        if (cbt.action == 'QUERY_IPOP_STATE_RESP'):
            if cbt.data != None:
                self.uid = cbt.data["_uid"]
                self.log("UID {0} received from Watchdog".format(self.uid))
        if (cbt.action == "DO_SEND_MSG"):
            if self.uid == "":
                self.log("UID not yet received- Not Ready.")
                return
            method = cbt.data.get("method")
            peer_uid = cbt.data.get("uid")
            interface_index = cbt.data.get("interface_index")
            try:
                peer_jid = self.uid_jid[peer_uid]
            except:
                log_msg = "UID-JID mapping for UID: {0} not present.\
                            msg: {1} will not be sent.".format(peer_uid, method)
                self.log(log_msg)
                return
            data = cbt.data.get("data")
            if (method == "con_req"):
                setup_load = "con_req" + "#" + peer_uid
                msg_payload = self.uid + "#" + data
                self.sendMsg(peer_jid, setup_load, msg_payload)
                self.log("sent con_req to {0}".format(self.uid_jid[peer_uid]), severity=log_level)
            elif (method == "con_resp"):
                setup_load = "con_resp" + "#" + peer_uid
                msg_payload = self.uid + "#" + data
                self.sendMsg(peer_jid, setup_load, msg_payload)
                self.log("sent con_resp to {0}".format(self.uid_jid[peer_uid]), severity=log_level)
            elif (method == "con_ack"):
                setup_load = "con_ack" + "#" + peer_uid
                msg_payload = self.uid+ "#" + data
                self.sendMsg(peer_jid, setup_load, msg_payload)
                self.log("sent con_ack to {0}".format(self.uid_jid[peer_uid]), severity=log_level)
            elif (method == "ping_resp"):
                setup_load = "ping_resp" + "#" + peer_uid
                msg_payload = self.uid + "#" + data
                self.sendMsg(peer_jid, setup_load, msg_payload)
                self.log("sent ping_resp to {0}".format(self.uid_jid[peer_uid]), severity=log_level)
            elif (method == "ping"):
                setup_load = "ping" + "#" + peer_uid
                msg_payload = self.uid + "#" + data
                self.sendMsg(peer_jid, setup_load, msg_payload)
                self.log("sent ping to {0}".format(self.uid_jid[peer_uid]), severity=log_level)
        elif cbt.action == "GetXMPPPeer":
            if self.update_peerlist == True:

                msg = {
                    "interface_name": self.interface_name,
                    "peer_list"     : list(self.uid_jid.keys())
                }
                pendingcbt = self.retrievePendingCBT(cbt)
                if pendingcbt!= None:
                    self.registerCBT(pendingcbt.initiator, "peer_list", msg)
                else:
                    self.registerCBT(cbt.initiator,"peer_list",msg)
                self.log("XMPP Peer List:::" + str(msg),severity="debug")
                self.update_peerlist = False
            else:
                self.insertPendingCBT(cbt)


    def sendXmppAdvt(self, override=False):
        if self.uid != "":
            for peer in self.xmpp_peers.keys():
                send_advt = False
                # True indicates that peer node does not knows my UID.
                # If I have recvd more than 10 correct advertisements from peer
                # reply back, may be my reply was lost.
                if (self.jid_uid[peer][1] == True and self.jid_uid[peer][2] % 10 == 0):
                    send_advt = True
                    self.jid_uid[peer][2] = 1
                elif (self.jid_uid[peer][1] == True and override != True):
                    # Do not send an advt
                    send_advt = False
                else:
                    # If here, peer does not knows my UID
                    send_advt = True

                if (send_advt == True):
                    setup_load = "xmpp_advertisement" + "#" + "None"
                    msg_load = str(self.uid) + "#" + str(self.jid_uid[peer][0])
                    self.sendMsg(peer, setup_load, msg_load)
                    self.log("sent xmpp_advt to {0}".format(peer), severity=log_level)

    def timer_method(self):
        if (self.uid == "" and self.vpn_type == "SocialVPN"):
            self.log("UID not yet received- Not Ready.")
            self.registerCBT('Watchdog', 'QUERY_IPOP_STATE')
            return
        try:
            if (time.time() - self.last_sent_advt > self.advt_delay):
                # see if I recvd a advertisement in this time period
                # if yes than XMPP link is open
                if (self.xmpp_advt_recvd == True):
                    self.sendXmppAdvt()
                    # update xmpp tracking parameters.
                    self.last_sent_advt = time.time()
                    self.xmpp_advt_recvd = False
                    self.advt_delay = self.INITIAL_ADVT_DELAY
                # Have not heard from anyone in a while, Handles XMPP disconnection
                # do not want to overwhelm with queued messages.
                elif (self.advt_delay < self.MAX_ADVT_DELAY):
                    self.advt_delay = 2 * self.advt_delay
                    self.log("Delaying the XMPP advt timer \
                            to {0} seconds".format(self.advt_delay))
                else:
                    # send the advertisement anyway, after MaxDelay.
                    self.sendXmppAdvt(override=True)
                    # update xmpp tracking parameters.
                    self.last_sent_advt = time.time()
                    self.xmpp_advt_recvd = False
        except:
            self.log("Exception in XmppClient timer", severity="error")

    def terminate(self):
        pass


