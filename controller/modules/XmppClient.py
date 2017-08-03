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

import sys
import ssl
import time
from controller.framework.ControllerModule import ControllerModule
from collections import defaultdict

try:
    import sleekxmpp
    from sleekxmpp.xmlstream.stanzabase import ElementBase, JID
    from sleekxmpp.xmlstream import register_stanza_plugin
    from sleekxmpp.xmlstream.handler.callback import Callback
    from sleekxmpp.xmlstream.matcher import StanzaPath
    from sleekxmpp.stanza.message import Message
except:
    raise ImportError("Sleekxmpp Module not installed")

py_ver = sys.version_info[0]
if py_ver == 3:
    import _thread as thread
else:
    import thread
log_level = "info"


class XmppClient(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        ControllerModule.__init__(self, CFxHandle, paramDict, ModuleName)
        self.ipop_xmpp_details = {}
        self.keyring_installed = False
        # Check whether the KeyRing Module has been installed
        try:
            import keyring
            self.keyring_installed = True
        except:
            self.registerCBT("Logger", "warning", "Key-ring module not installed.")

    # Triggered at start of XMPP session
    def start(self, event):
        try:
            for xmpp_detail in list(self.ipop_xmpp_details.values()):
                # Check whether Callback functions are configured for XMPP server messages
                if xmpp_detail["callbackinit"] is False:
                    xmpp_detail["callbackinit"] = True
                    xmpp_detail["XMPPObj"].get_roster()  # Obtains the friends list for the user
                    xmpp_detail["XMPPObj"].send_presence()  # Sends presence message when the XMPP user is online
                    # Event to capture all online peer nodes as seen by the XMPP server
                    xmpp_detail["XMPPObj"].add_event_handler("presence_available", self.handle_presence)
                    # Event to capture all offline peer nodes as seen by the XMPP server
                    xmpp_detail["XMPPObj"].add_event_handler("presence_unavailable", self.offline_xmpp_peers)
                    # Register IPOP message with the server
                    register_stanza_plugin(Message, IpopMsg)
                    xmpp_detail["XMPPObj"].registerHandler(Callback('Ipop', StanzaPath('message/Ipop'), self.xmppmessagelistener))
        except Exception as err:
            self.log("Exception in XMPPClient:{0} Event:{1}".format(err, event), severity="error")

    # Callback Function to keep track of Online XMPP Peers
    def handle_presence(self, presence):
        try:
            presence_sender = presence['from']
            presence_receiver_jid = JID(presence['to'])
            presence_receiver = str(presence_receiver_jid.user)+"@"+str(presence_receiver_jid.domain)
            for xmpp_details in self.ipop_xmpp_details.values():
                # Check whether the Receiver JID belongs to the XMPP Object of the particular virtual network,
                # If YES update JID-UID table
                if presence_receiver == xmpp_details["username"] and presence_sender != xmpp_details["XMPPObj"].boundjid\
                        .full:
                    xmpp_details["jid_uid"][presence_sender][1] = time.time()
                    self.log("Presence received from {0}".format(presence_sender), severity=log_level)
        except Exception as err:
            self.log("Exception caught in XMPPClient handle_presence method : {0}".format(err), severity="error")

    # Callback Function to update XMPP Roster information (Revocation)
    def updateroster(self, message):
        try:
            presence_receiver_jid = JID(message['to'])
            presence_receiver = str(presence_receiver_jid.user) + "@" + str(presence_receiver_jid.domain)
            interface_name, xmppobj = "", None
            # Iterate across the XMPPClient module table to find the TapInterface for the XMPP server message
            for tapName, xmpp_details in list(self.ipop_xmpp_details.items()):
                if presence_receiver == xmpp_details["username"]:
                    xmppobj = xmpp_details
                    interface_name = tapName
                    break
            # check whether Server Message has a matching key in the XMPP Table if not stop processing
            if xmppobj is None:
                return
            # iterate across the roster details to find unsubscribe JIDs
            for nodejid, data in message["roster"]["items"].items():
                if data["subscription"] == "remove":
                    for ele in list(xmppobj["jid_uid"].keys()):
                        tempjid = JID(ele)
                        jid = str(tempjid.user)+"@"+str(tempjid.domain)
                        # Check whether the JID of the unsubscribed user exists in the JID-UID Table,
                        # If Yes remove it from JID-UID, UID-JID Table and sent a trigger message to Topology Manager
                        # to remove it from its discovered node list
                        if jid.find(str(nodejid)) != -1:
                            node_uid = xmppobj[ele][0]
                            del xmppobj["jid_uid"][ele]
                            # Check whether UID exists in the UID-JID Table, If YES remove it
                            if node_uid in xmppobj["uid_jid"].keys():
                                del xmppobj["uid_jid"][node_uid]
                                self.registerCBT("Logger", "info", "{0} has been deleted from the roster.".
                                                 format(node_uid))
                                msg = {
                                    "uid": node_uid,
                                    "type": "offline_peer",
                                    "interface_name": interface_name
                                }
                                self.registerCBT("BaseTopologyManager", "XMPP_MSG", msg)
                            # Remove the peer from XMPP Online Peerlist if it exists
                            if node_uid in xmppobj["online_xmpp_peers"]:
                                xmppobj["online_xmpp_peers"].remove(node_uid)
        except Exception as err:
            self.log("Exception in deletepeerjid method.{0}".format(err), severity="error")

    # Callback function to keep track of Peer Offline events sent by the XMPP server
    def offline_xmpp_peers(self, message):
        try:
            peerjid = message["from"]
            self.log("Peer JID {0} offline".format(peerjid), severity="info")
            presence_receiver_jid = JID(message['to'])
            presence_receiver = str(presence_receiver_jid.user) + "@" + str(presence_receiver_jid.domain)
            interface_name, xmppobj = "", None
            # Iterate across the XMPPClient module table to find the TapInterface for the XMPP server message
            for tapName, xmpp_details in list(self.ipop_xmpp_details.items()):
                if presence_receiver == xmpp_details["username"]:
                    xmppobj = xmpp_details
                    interface_name = tapName
                    break
            # check whether Server Message has a matching key in the XMPP Table if not stop processing
            if xmppobj is None:
                return
            # Check whether the JID of the offline peer exists in the JID-UID Table, If YES remove it from the
            # JID-UID, UID-JID and sent a trigger message to Topology Manager
            # to remove it from its discovered node list
            if peerjid in xmppobj["jid_uid"].keys():
                uid = xmppobj["jid_uid"][peerjid][0]
                del xmppobj["jid_uid"][peerjid]
                # Check whether UID exists in the UID-JID Table, If YES remove it
                if uid in xmppobj["uid_jid"].keys():
                    del xmppobj["uid_jid"][uid]
                    self.log("Removed Peer JID: {0} UID: {1} from the JID-UID and UID-JID Table".format(peerjid, uid),
                             severity="info")
                    msg = {
                        "uid": uid,
                        "type": "offline_peer",
                        "interface_name": interface_name
                    }
                    self.registerCBT("BaseTopologyManager", "XMPP_MSG", msg)
        except Exception as err:
            self.log("Exception in remove peerjid method. Error::{0}".format(err), severity="error")

    # This handler method listens for the matched messages on tehj xmpp stream,
    # extracts the setup and payload and takes suitable action depending on the
    # them.
    def xmppmessagelistener(self, msg):
        presence_receiver_jid = JID(msg['to'])
        sender_jid = msg['from']
        presence_receiver = str(presence_receiver_jid.user) + "@" + str(presence_receiver_jid.domain)
        interface_name, xmppobj = "", None
        # Iterate across the XMPPClient module table to find the TapInterface for the XMPP server message
        for tapName, xmpp_details in list(self.ipop_xmpp_details.items()):
            if presence_receiver == xmpp_details["username"]:
                xmppobj = xmpp_details
                interface_name = tapName
                break
        # check whether Server Message has a matching key in the XMPP Table if not stop processing
        if xmppobj is None:
            return
        # Check whether Node UID obtained from CFX
        if xmppobj["uid"] == "":
            self.log("UID not received from Tincan. Please check Tincan logs.", severity="warning")
            return
        # Check whether the message was initiated by the node itself if YES discard it
        if sender_jid == xmppobj["XMPPObj"].boundjid.full:
            return
        # extract setup and content
        setup = str(msg['Ipop']['setup'])
        payload = str(msg['Ipop']['payload'])
        msg_type, target_uid, target_jid = setup.split("#")

        if msg_type == "regular_msg":
            self.log("Received regular mesage from {0}".format(msg['from']), severity=log_level)
            self.log("Msg is {0}".format(payload), severity="debug")
        elif msg_type == "xmpp_advertisement":
            # peer_uid - uid of the node that sent the advt
            # target_uid - what it percieves as my uid
            try:
                peer_uid, target_uid = payload.split("#")
                # Check that the source of xmpp message is not the current node, if NOT, update the JID-UID Table's
                # current time, UID and message count attributes
                if peer_uid != xmppobj["uid"]:
                    xmppobj["uid_jid"][peer_uid] = sender_jid
                    xmppobj["jid_uid"][sender_jid][0] = peer_uid
                    xmppobj["jid_uid"][sender_jid][2] += 1
                    xmppobj["jid_uid"][sender_jid][1] = time.time()
                    self.log("XMPP Message: Received advertisement from peer {0}".format(peer_uid),
                                 severity=log_level)
            except Exception as error:
                self.log("Exception caught while processing advt_payload: {0}. Error: {1}".format(payload, str(error)),
                         severity="error")

        # compare uid's here , if target uid does not match with mine do nothing.
        # have to avoid loop messages.
        if target_uid == xmppobj["uid"]:
            sender_uid, recvd_data = payload.split("#")
            # If I recvd XMPP msg from this peer, I should record his UID-JID & JID-UID
            if sender_uid not in xmppobj["uid_jid"].keys():
                self.ipop_xmpp_details[interface_name]["update_xmpppeerlist_flag"] = True
                self.ipop_xmpp_details[interface_name]["online_xmpp_peers"].append(sender_uid)
                self.ipop_xmpp_details[interface_name]["online_xmpp_peers"] = \
                    list(set(self.ipop_xmpp_details[interface_name]["online_xmpp_peers"]))
                self.sendxmpppeerlist(interface_name)
            xmppobj["uid_jid"][sender_uid] = sender_jid
            msg = {}
            if msg_type == "get_casdetails":
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["interface_name"] = interface_name
                # send this CBT to BaseTopology Manager
                self.registerCBT('LinkManager', 'RETRIEVE_CAS_FROM_TINCAN', msg)
                self.log("XMPP Message: Received CAS request from peer {0}".format(msg["uid"]), severity=log_level)
            elif msg_type == "sent_casdetails":
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["interface_name"] = interface_name
                self.registerCBT('LinkManager', 'CREATE_P2PLINK', msg)
                self.log("Received CAS details from peer {0}".format(msg["uid"]), severity=log_level)

    # Send message to Peer JID via XMPP server
    def sendxmppmsg(self, peer_jid, xmppobj, setup_load=None, msg_payload=None):
        if setup_load is None:
            setup_load = "regular_msg" + "#" + "None" + "#" + peer_jid.full
        else:
            setup_load = setup_load + "#" + peer_jid.full

        if py_ver != 3:
            setup_load = unicode(setup_load)

        if msg_payload is None:
            content_load = "Hello there this is {0}".format(xmppobj.username)
        else:
            content_load = msg_payload
        msg = xmppobj.Message()
        msg['to'] = peer_jid
        msg["from"] = xmppobj.boundjid.full
        msg['type'] = 'chat'
        msg['Ipop']['setup'] = setup_load
        msg['Ipop']['payload'] = content_load
        msg.send()
        self.log("Sent XMPP message to {0}".format(peer_jid), severity=log_level)

    def xmpp_handler(self, xmpp_details, xmppobj):
        try:
            if xmppobj.connect(address=(xmpp_details["AddressHost"], xmpp_details["Port"])):
                thread.start_new_thread(xmppobj.process, ())
                self.log("Started XMPP handling", severity="debug")
        except Exception as err:
            self.log("Unable to start XMPP handling thread-Check Internet connectivity/credentials."+str(err),
                     severity='error')

    def log(self, msg, severity='info'):
        self.registerCBT('Logger', severity, msg)

    def initialize(self):
      xmpp_details = self.CMConfig.get("XmppDetails")
      xmpp_password = None
      # Iterate over the XMPP credentials for different virtual networks configured in ipop-config.json
      for i, xmpp_ele in enumerate(xmpp_details):
          xmpp_ele = dict(xmpp_ele)
          interface_name = xmpp_ele['TapName']
          self.ipop_xmpp_details[interface_name] = {}
          # Check whether the authentication mechanism is certifcate if YES then config file should not contain
          # Password based authentication parameters
          if xmpp_ele.get("AuthenticationMethod") == "x509" and (xmpp_ele.get("Username", None) is not None
                                                                  or xmpp_ele.get("Password", None) is not None):
              raise RuntimeError("x509 Authentication Error: Username/Password in IPOP configuration file.")

          # Check the Authentication Method configured for the particular virtual network interface
          if xmpp_ele.get("AuthenticationMethod") == "x509":
              xmppobj = sleekxmpp.ClientXMPP(None, None, sasl_mech='EXTERNAL')
              xmppobj.ssl_version = ssl.PROTOCOL_TLSv1
              xmppobj.ca_certs = xmpp_ele["TrustStore"]
              xmppobj.certfile = xmpp_ele["CertDirectory"] + xmpp_ele["CertFile"]
              xmppobj.keyfile = xmpp_ele["CertDirectory"] + xmpp_ele["Keyfile"]
              xmppobj.use_tls = True
          else:
              # Authentication method is Password based hence check whether XMPP username has been provided in the
              # ipop configuration file
              if xmpp_ele.get("Username", None) is None:
                  raise RuntimeError("Authentication Error: Username not provided in IPOP configuration file.")
              # Check whether the keyring module is installed if yes extract the Password
              if self.keyring_installed is True:
                  xmpp_password = keyring.get_password("ipop", xmpp_ele['Username'])
              # Check whether the Password exists either in Config file or inside Keyring
              if xmpp_ele.get("Password", None) is None and xmpp_password is None:
                  print("Authentication Error: Password not provided for XMPP "
                                                      "Username:{0}".format(xmpp_ele['Username']))
                  # Prompt user to enter password
                  print("Enter Password: ",)
                  if py_ver == 3:
                      xmpp_password = str(input())
                  else:
                      xmpp_password = str(raw_input())

                  xmpp_ele['Password'] = xmpp_password   # Store the userinput in the internal table
                  if self.keyring_installed is True:
                      try:
                          # Store the password inside the Keyring
                          keyring.set_password("ipop", xmpp_ele['Username'], xmpp_password)
                      except Exception as error:
                          self.registerCBT("Logger", "error", "unable to store password in keyring.Error: {0}"
                                            .format(error))
              xmppobj = sleekxmpp.ClientXMPP(xmpp_ele['Username'], xmpp_ele['Password'], sasl_mech='PLAIN')
              # Check whether Server SSL Authenication required
              if xmpp_ele.get("AcceptUntrustedServer") is True:
                  xmppobj.register_plugin("feature_mechanisms", pconfig={'unencrypted_plain': True})
                  xmppobj.use_tls = False
              else:
                  xmppobj.ca_certs = xmpp_ele["TrustStore"]

          # Register event handler for session start and Roster Update in case a user gets unfriended
          xmppobj.add_event_handler("session_start", self.start)
          xmppobj.add_event_handler("roster_update", self.updateroster)

          self.ipop_xmpp_details[interface_name]["XMPPObj"] = xmppobj     # Store the Sleekxmpp object in the Table
          # Store XMPP UserName (required to extract TapInterface from the XMPP server message)
          self.ipop_xmpp_details[interface_name]["username"] = xmpp_ele["Username"]
          # Store Online Peers as seen by the XMPP Server
          self.ipop_xmpp_details[interface_name]["online_xmpp_peers"] = []
          # Flag to indicate there is change in the Online Peer List
          self.ipop_xmpp_details[interface_name]["update_xmpppeerlist_flag"] = False
          # Store the JIDs seen by the node
          # self.ipop_xmpp_details[interface_name]["xmpp_peers"] = {}
          # Table to store Peer UID their corresponding JID (Needed while sending XMPP Message)
          self.ipop_xmpp_details[interface_name]["uid_jid"] = {}
          # Flag to check whether the XMPP Callback for various functionalities have been set
          self.ipop_xmpp_details[interface_name]["callbackinit"] = False
          # Stores the XMPP message limit after which the advrt delay is increased for Peer Nodes
          self.ipop_xmpp_details[interface_name]["MessagePerIntervalDelay"] = \
              xmpp_ele.get("MessagePerIntervalDelay", self.CMConfig.get("MessagePerIntervalDelay"))
          # Initial interval between sending advertisements from ipop config file, else load from fxlib.py
          self.ipop_xmpp_details[interface_name]["initialadvrtdelay"] = \
              xmpp_ele.get("InitialAdvertismentDelay", self.CMConfig.get("InitialAdvertismentDelay"))
          # Table to store Peer JID as key, Peer UID, xmpp advrt delay and xmpp advrt received as value
          self.ipop_xmpp_details[interface_name]["jid_uid"] = defaultdict(lambda: ['', 0, 0,
                                          self.ipop_xmpp_details[interface_name]["initialadvrtdelay"]])
          # Steady state interval between sending advertisements from ipop config file, else load from fxlib.py
          self.ipop_xmpp_details[interface_name]["advrtdelay"] = \
              xmpp_ele.get("XmppAdvrtDelay", self.CMConfig.get("XmppAdvrtDelay"))
          # Maximum delay between advertisements from ipop config file else, load from fxlib.py
          self.ipop_xmpp_details[interface_name]["maxadvrtdelay"] = \
              xmpp_ele.get("MaxAdvertismentDelay", self.CMConfig.get("MaxAdvertismentDelay"))
          # Query VirtualNetwork Interface details from TincanInterface module
          ipop_interfaces = self.CFxHandle.queryParam("TincanInterface", "Vnets")
          # Iterate over the entire VirtualNetwork Interface List
          for interface_details in ipop_interfaces:
              # Check whether the TapName given in the XMPPClient and TincanInterface module if yes load UID
              if interface_details["TapName"] == interface_name:
                  self.ipop_xmpp_details[interface_name]["uid"] = interface_details["uid"]
          # Connect to the XMPP server
          self.xmpp_handler(xmpp_ele, xmppobj)
      self.log("{0} module Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        message = cbt.data
        interface_name = message.get("interface_name")
        # CBT to send messages over XMPP protocol
        if cbt.action == "DO_SEND_MSG":
            if self.ipop_xmpp_details[interface_name]["uid"] == "":
                self.log("UID not received from Tincan. Please check Tincan logs.", severity="error")
                return
            method = message.get("method")
            peer_uid = message.get("uid")
            node_uid = self.ipop_xmpp_details[interface_name]["uid"]
            if peer_uid in self.ipop_xmpp_details[interface_name]["uid_jid"].keys():
                peer_jid = self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]
            else:
                log_msg = "UID-JID mapping for UID: {0} not present.\
                            msg: {1} will not be sent.".format(peer_uid, method)
                self.log(log_msg)
                return
            data = message.get("data")

            if method == "get_peer_casdetails":
                setup_load = "get_casdetails" + "#" + peer_uid
                msg_payload = node_uid + "#" + data
                self.log("Sent GET_CAS_Request sent to peer {0}".format(
                    self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]), severity=log_level)
                self.sendxmppmsg(peer_jid, self.ipop_xmpp_details[interface_name]["XMPPObj"],
                                 setup_load, msg_payload)
            elif method == "sent_peer_casdetails":
                setup_load = "sent_casdetails" + "#" + peer_uid
                msg_payload = node_uid + "#" + data
                self.log("XMPP Message: CAS Response sent to peer {0}".format(
                    self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]), severity=log_level)
                self.sendxmppmsg(peer_jid, self.ipop_xmpp_details[interface_name]["XMPPObj"],
                                 setup_load, msg_payload)
            else:
                log = '{0}: unrecognized method received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                self.registerCBT('Logger', 'warning', log)
        # CBT for extracting peer nodes seen by the XMPP server
        elif cbt.action == "GET_XMPP_PEERLIST":
            # check whether there has been a change to the Online PeerList if YES send the initator the latest list,
            # If NO out the CBT into the Pending list
            if self.ipop_xmpp_details[interface_name]["update_xmpppeerlist_flag"] is True and \
                len(self.ipop_xmpp_details[interface_name]["online_xmpp_peers"]) > 0:
                msg = {
                    "interface_name": interface_name,
                    "peer_list": self.ipop_xmpp_details[interface_name]["online_xmpp_peers"]
                }
                retrieveCBTList = self.retrievePendingCBT(str(cbt.initiator) + " " + str(cbt.action))
                # Check if there are any pending GetXMPPPeerList CBT
                if retrieveCBTList is None:
                    self.registerCBT(cbt.initiator, "UPDATE_XMPP_PEERLIST", msg)
                else:
                    for cbtele in retrieveCBTList:
                        self.registerCBT(cbtele.initiator, "UPDATE_XMPP_PEERLIST", msg)
                self.ipop_xmpp_details[interface_name]["update_xmpppeerlist_flag"] = False
            else:
                self.insertPendingCBT(cbt)
        else:
            log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
            self.registerCBT('Logger', 'warning', log)

    # Function to send XMPP advrt to Peer
    def sendxmppadvrt(self, interface_name, peer):
        # Condition to prevent the node from sending message to itself
        if self.ipop_xmpp_details[interface_name]["uid"] != "" and peer != self.ipop_xmpp_details[interface_name]\
            ["XMPPObj"].boundjid.full:
                # Increase the XMPP backoff delay once the number of messages received by the node from peer has
                # exceeded or reached the permissible value
                if self.ipop_xmpp_details[interface_name]["jid_uid"][peer][2] % \
                        self.ipop_xmpp_details[interface_name]["MessagePerIntervalDelay"] == 0:
                    # Increase the XMPP Backoff till the Max backoff limit
                    if self.ipop_xmpp_details[interface_name]["jid_uid"][peer][3] < self.ipop_xmpp_details\
                        [interface_name]["maxadvrtdelay"]:
                        self.ipop_xmpp_details[interface_name]["jid_uid"][peer][3] += self.ipop_xmpp_details\
                        [interface_name]["advrtdelay"]
                        self.ipop_xmpp_details[interface_name]["jid_uid"][peer][2] = 1
                setup_load = "xmpp_advertisement" + "#" + "None"
                # Advertisment message format <NODE_UID>#<PEER_NODE_UID>
                # Note:: Initially PEER_NODE_UID is an empty string
                msg_load = str(self.ipop_xmpp_details[interface_name]["uid"]) + "#" + str(self.ipop_xmpp_details\
                                [interface_name]["jid_uid"][peer][0])
                self.sendxmppmsg(peer, self.ipop_xmpp_details[interface_name]["XMPPObj"], setup_load, msg_load)

    # Extract pending GetXMPPPeerList and send the Online Peerlist
    def sendxmpppeerlist(self, interface_name):
        retrieveCBTList = self.retrievePendingCBT("GET_XMPP_PEERLIST")
        msg = {
            "interface_name": interface_name,
            "peer_list": self.ipop_xmpp_details[interface_name]["online_xmpp_peers"]
        }
        # Check if there are any pending GET_XMPP_PEERLIST CBT
        if retrieveCBTList is not None:
            # Send the latest XMPP_PEERLIST to all the initators
            for cbt in retrieveCBTList:
                self.registerCBT(cbt.initiator, "UPDATE_XMPP_PEERLIST", msg)
            # Reset the Update Flag
            self.ipop_xmpp_details[interface_name]["update_xmpppeerlist_flag"] = False

    def timer_method(self):
        try:
            for interface_name in self.ipop_xmpp_details.keys():
                xmpp_details = self.ipop_xmpp_details[interface_name]
                updatepeerflag = False
                for peerjid in xmpp_details["jid_uid"].keys():
                    # check whether Peer has sent advertisement with its UID, if not dont add it to online xmpp peerlist
                    xmpp_msg_delay = time.time() - xmpp_details["jid_uid"][peerjid][1]
                    if xmpp_msg_delay > self.ipop_xmpp_details[interface_name]["jid_uid"][peerjid][3] and \
                            xmpp_msg_delay < self.ipop_xmpp_details[interface_name]["maxadvrtdelay"]:
                        self.sendxmppadvrt(interface_name=interface_name, peer=peerjid)
                        # check whether peer uid is exists in online_xmpp_peer list
                        if xmpp_details["jid_uid"][peerjid][0] not in xmpp_details["online_xmpp_peers"] \
                                        and xmpp_details["jid_uid"][peerjid][0] not in ['', xmpp_details["uid"]]:
                            xmpp_details["online_xmpp_peers"].append(xmpp_details["jid_uid"][peerjid][0])
                            updatepeerflag = True
                # Check for a change in XMPP Peerlist
                if updatepeerflag is True:
                    self.ipop_xmpp_details[interface_name]["update_xmpppeerlist_flag"] = True
                    self.sendxmpppeerlist(interface_name)
        except Exception as error:
            self.log("Exception in XmppClient timer.{0}".format(error), severity="error")

    def terminate(self):
        pass


# set up a new custom message stanza
class IpopMsg(ElementBase):
    namespace = "Conn_setup"
    name = 'Ipop'
    plugin_attrib = 'Ipop'
    interfaces = set(('setup', 'payload', 'uid', 'TapName'))