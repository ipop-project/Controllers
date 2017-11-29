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
import json
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
    from queue import Queue
    import _thread as thread
else:
    from Queue import Queue
    import thread
log_level = "info"


class XmppClient(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        ControllerModule.__init__(self, CFxHandle, paramDict, ModuleName)
        self.presence_publisher = None
        self.ipop_xmpp_details = {}
        self.keyring_installed = False
        #self.pending_CBTQ = {}

    # Triggered at start of XMPP session
    def start(self, event):
        try:
            for xmpp_detail in list(self.ipop_xmpp_details.values()):
                # Check whether Callback functions are configured for XMPP server messages
                if xmpp_detail["callbackinit"] is False:
                    xmpp_detail["callbackinit"] = True
                    xmpp_detail["XMPPObj"].get_roster()  # Obtains the friends list for the user
                    xmpp_detail["XMPPObj"].send_presence(pstatus="uid_is#"+xmpp_detail["uid"])  # Sends presence message when the XMPP user is online
                    # Event to capture all online peer nodes as seen by the XMPP server
                    xmpp_detail["XMPPObj"].add_event_handler("presence_available", self.handle_presence)
                    # Register IPOP message with the server
                    register_stanza_plugin(Message, IpopMsg)
                    xmpp_detail["XMPPObj"].registerHandler(Callback('Ipop', StanzaPath('message/Ipop'), self.xmppmessagelistener))
        except Exception as err:
            self.log("Exception in XMPPClient:{0} Event:{1}".format(err, event), severity="error")

    # Callback Function to keep track of Online XMPP Peers
    def handle_presence(self, presence):
        #try:
            presence_sender = presence['from']
            presence_receiver_jid = JID(presence['to'])
            presence_receiver = str(presence_receiver_jid.user)+"@"+str(presence_receiver_jid.domain)
            status = presence['status']
            for interface, xmpp_details in self.ipop_xmpp_details.items():
                # Check whether the Receiver JID belongs to the XMPP Object of the particular virtual network,
                # If YES update JID-UID table
                if presence_receiver == xmpp_details["username"] and presence_sender != xmpp_details["XMPPObj"].boundjid\
                        .full:
                    if (status != "" and "#" in status):
                        p_type, uid = status.split('#')
                        if (p_type=="uid_is"):
                            self.presence_publisher.PostUpdate(dict(uid_notification=uid, interface_name=interface))
                            self.log("UID {0} received from {1}".format(uid,presence_sender), severity=log_level)
                        elif (p_type == "uid?"):
                                if (xmpp_details["uid"]==uid):
                                    xmpp_details["XMPPObj"].send_presence(pstatus="jid_uid#" + xmpp_details["uid"]+"#"+
                                                                          xmpp_details["XMPPObj"].boundjid.full)
                                    setup_load = "UID_MATCH" + "#" + "None" + "#" + str(presence_sender)
                                    msg_load = xmpp_details["XMPPObj"].boundjid.full+"#"+xmpp_details["uid"]
                                    self.sendxmppmsg(presence_sender, xmpp_details["XMPPObj"],
                                                     setup_load, msg_load)
                                    self.log("UID {0} matched, replied to {1}".format(uid, presence_sender),
                                             "debug")
                                else:
                                    self.log("UID {0} request received from {1}".format(uid, str(presence_sender)), "debug")
        #except Exception as err:
            #self.log("Exception caught in XmppClient handle_presence method : {0}".format(err), severity="error")

    def arp_uid(self,xmppobj , who_has_uid):
        xmppobj.send_presence(pstatus="uid?#"+who_has_uid)

    # This handler method listens for the matched messages on the xmpp stream,
    # extracts the setup and payload and takes suitable action depending on the
    # them.
    def xmppmessagelistener(self, msg):
        receiver_jid = JID(msg['to'])
        sender_jid = msg['from']
        receiver = str(receiver_jid.user) + "@" + str(receiver_jid.domain)
        interface_name, xmppobj = "", None
        self.log("RECEIVED MESSAGE MSG {} ".format(msg), "debug")
        # Iterate across the XMPPClient module table to find the TapInterface for the XMPP server message
        for tapName, xmpp_details in list(self.ipop_xmpp_details.items()):
            if receiver == xmpp_details["username"]:
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
        setup = msg['Ipop']['setup']
        payload = msg['Ipop']['payload']
        msg_type, target_uid, target_jid = setup.split("#")
        self.log("RECEIVED MESSAGE setup {} payload {}".format(setup,payload),"debug")

        if msg_type == "regular_msg":
            self.log("Received regular mesage from {0}".format(msg['from']), severity=log_level)
            self.log("Msg is {0}".format(payload), severity="debug")

        elif msg_type == "UID_MATCH":
            # This type does not contains target uid
            match_jid,matched_uid = payload.split("#")
            self.log("UID match received from JID {} for UID {}".format(match_jid,matched_uid),"debug")
            # complete all pending CBTs
            #CBTQ = self.pending_CBTQ[matched_uid]
            CBTQ = self.ipop_xmpp_details[interface_name]["pending_CBTQ"][matched_uid]
            self.log("Type CBTQ {} ".format(type(CBTQ)), "debug")
            self.log("CBTQ {} ".format(CBTQ),"debug")
            while not CBTQ.empty():
                cbt = CBTQ.get()
                self.log("cbt {}".format(cbt),"debug")
                setup_load = "FORWARDED_CBT" + "#" + "None" + "#" + match_jid
                msg_payload = json.dumps(cbt)
                self.sendxmppmsg(match_jid,xmppobj["XMPPObj"],setup_load,msg_payload)
            self.log("sent out pending cbt for UID {}".format(matched_uid), "debug")
            return

        elif msg_type == "FORWARDED_CBT":
            self.log("____________________HERE-6__________________", "debug")
            # does not contains target uid
            payload = json.loads(payload)
            self.log("payload {}".format(payload), "debug")
            self.log("____________________HERE-7__________________", "debug")
            dest_module = payload["dest_module"]
            self.log("____________________HERE-8__________________", "debug")
            src_uid = payload["sender_uid"]
            action = payload["action"]
            cbtdata = dict(uid=src_uid,data=payload['core_data'],interface_name = interface_name)
            self.log("cbtdata {}".format(cbtdata),"debug")
            self.registerCBT(dest_module, action, cbtdata, _tag=src_uid)
            self.log("Received forwarded CBT from {}".format(sender_jid),"debug")
            return


    # Send message to Peer JID via XMPP server
    def sendxmppmsg(self, peer_jid, xmppobj, setup_load=None, msg_payload=None):
        if setup_load is None:
            setup_load = "regular_msg" + "#" + "None" + "#" + peer_jid.full

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
        try:
            import keyring
            self.keyring_installed = True
        except:
            self.registerCBT("Logger", "info", "Key-ring module not installed.")
        xmpp_details = self.CMConfig.get("XmppDetails")
        self.presence_publisher = self.CFxHandle.PublishSubscription("PEER_PRESENCE_NOTIFICATION")
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

                    xmpp_ele['Password'] = xmpp_password     # Store the userinput in the internal table
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

            self.ipop_xmpp_details[interface_name]["XMPPObj"] = xmppobj     # Store the Sleekxmpp object in the Table
            self.ipop_xmpp_details[interface_name]["pending_CBTQ"] = {} # map for holding pending cbt data to be forwarded
            # Store XMPP UserName (required to extract TapInterface from the XMPP server message)
            self.ipop_xmpp_details[interface_name]["username"] = xmpp_ele["Username"]
            # Flag to check whether the XMPP Callback for various functionalities have been set
            self.ipop_xmpp_details[interface_name]["callbackinit"] = False
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
        if self.ipop_xmpp_details[interface_name]["uid"] == "":
            self.log("UID not received from Tincan. Please check Tincan logs.", severity="error")
            return
        # CBT to send messages over XMPP protocol
        if cbt.action == "FORWARD_CBT":
            peer_uid = message.get("uid")
            node_uid = self.ipop_xmpp_details[interface_name]["uid"]
            data = message.get("data")
            #CBTQ =  self.pending_CBTQ
            CBTQ = self.ipop_xmpp_details[interface_name]["pending_CBTQ"]
            if peer_uid in CBTQ.keys():
                CBTQ[peer_uid].put(data)
            else:
                CBTQ[peer_uid]= Queue(maxsize=0)
                CBTQ[peer_uid].put(data)
            self.arp_uid(self.ipop_xmpp_details[interface_name]["XMPPObj"], peer_uid)
            self.log("CBT for UID {} put into pending Queue".format(peer_uid), severity="debug")



    def timer_method(self):
       pass

    def terminate(self):
        pass


# set up a new custom message stanza
class IpopMsg(ElementBase):
    namespace = "Conn_setup"
    name = 'Ipop'
    plugin_attrib = 'Ipop'
    interfaces = set(('setup', 'payload', 'uid', 'TapName'))