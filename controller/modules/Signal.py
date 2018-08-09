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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import ssl
import time
import threading
from queue import Queue
try:
    import simplejson as json
except ImportError:
    import json
import sleekxmpp
from sleekxmpp.xmlstream.stanzabase import ElementBase, JID
from sleekxmpp.xmlstream import register_stanza_plugin
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.stanza.message import Message
from controller.framework.ControllerModule import ControllerModule


class IpopSignal(ElementBase):
    """Representation of SIGNAL's custom message stanza"""
    name = "ipop"
    namespace = "signal"
    plugin_attrib = "ipop"
    interfaces = set(("type", "payload"))


class JidCache:
    def __init__(self, cmod, expiry):
        self.lck = threading.Lock()
        self.cache = {}
        self.sig = cmod
        self.expiry = expiry

    #def _log(self, msg, severity="LOG_INFO"):
    #    self.cm_mod._log(msg, severity)

    def add_entry(self, node_id, jid):
        self.lck.acquire()
        self.cache[node_id] = (jid, time.time())
        self.lck.release()

    def scavenge(self,):
        self.lck.acquire()
        curr_time = time.time()
        keys_to_be_deleted = \
            [key for key, value in self.cache.items() if curr_time - value[1] >= 120]
        for key in keys_to_be_deleted:
            del self.cache[key]
            self.sig.sig_log("Deleted entry from JID cache {0}".format(key))
        self.lck.release()

    def lookup(self, node_id):
        jid = None
        self.lck.acquire()
        ent = self.cache.get(node_id)
        if ent:
            jid = ent[0]
        self.lck.release()
        return jid


class XmppTransport(sleekxmpp.ClientXMPP):
    def __init__(self, jid, password, sasl_mech):
        sleekxmpp.ClientXMPP.__init__(self, jid, password, sasl_mech=sasl_mech)
        self.overlay_id = None
        self.overlay_descr = None
        self._sig = None
        self.node_id = None
        self.presence_publisher = None
        self.jid_cache = None
        self.cbts = None
        self._cbt_to_action_tag = {}  # maps remote action tags to cbt tags
        self.host = None
        self.port = None

    @staticmethod
    def factory(overlay_id, overlay_descr, cm_mod, presence_publisher, jid_cache, cbts):
        try:
            keyring_installed = False
            import keyring
            keyring_installed = True
        except ImportError as err:
            cm_mod.sig_log("No key-ring found", "LOG_INFO")
        host = overlay_descr["HostAddress"]
        port = overlay_descr["Port"]
        user = overlay_descr.get("Username", None)
        pswd = overlay_descr.get("Password", None)
        auth_method = overlay_descr.get("AuthenticationMethod", "Password")
        if auth_method == "x509" and (user is not None or pswd is not None):
            er_log = "x509 Authentication is enbabled but credentials " \
                "exists in IPOP configuration file; x509 will be used."
            cm_mod.sig_log(er_log, "LOG_WARNING")
        if auth_method == "x509":
            transport = XmppTransport(None, None, sasl_mech="EXTERNAL")
            transport.ssl_version = ssl.PROTOCOL_TLSv1
            transport.ca_certs = overlay_descr["TrustStore"]
            transport.certfile = overlay_descr["CertDirectory"] + overlay_descr["CertFile"]
            transport.keyfile = overlay_descr["CertDirectory"] + overlay_descr["Keyfile"]
            transport.use_tls = True
        elif auth_method == "PASSWORD":
            if user is None:
                raise RuntimeError("No username is provided in IPOP configuration file.")
            if pswd is None and keyring_installed is True:
                pswd = keyring.get_password("ipop", overlay_descr["Username"])
            if pswd is None:
                print("{0} XMPP Password: ".format(user))
                pswd = str(input())
                if keyring_installed is True:
                    try:
                        keyring.set_password("ipop", user, pswd)
                    except Exception as err:
                        cm_mod.sig_log("Failed to store password in keyring. {0}".format(str(err)),
                                       "LOG_ERROR")
            transport = XmppTransport(user, pswd, sasl_mech="PLAIN")
            del pswd
        else:
            raise RuntimeError("Invalid authentication method specified in configuration: {0}"
                               .format(auth_method))
        transport.host = host
        transport.port = port
        transport.overlay_id = overlay_id
        transport._sig = cm_mod
        transport.node_id = cm_mod._cm_config["NodeId"]
        transport.presence_publisher = presence_publisher
        transport.jid_cache = jid_cache
        transport.cbts = cbts
        # Server SSL Authenication required by default
        if overlay_descr.get("AcceptUntrustedServer", False) is True:
            transport.register_plugin("feature_mechanisms", pconfig={"unencrypted_plain": True})
            transport.use_tls = False
        else:
            transport.ca_certs = overlay_descr["TrustStore"]
        # event handler for session start and roster update
        transport.add_event_handler("session_start", transport.start_event_handler)
        return transport

    #def _log(self, msg, severity="LOG_INFO"):
    #    self.cm_mod.sig_log(msg, severity)

    def start_event_handler(self, event):
        """Registers custom event handlers at the start of XMPP session"""
        self._sig.sig_log("XMPP Signalling started for overlay: {0}".format(self.overlay_id))
        try:
            # Notification of peer signon
            self.add_event_handler("presence_available",
                                   self.presence_event_handler)
            # Register IPOP message with the server
            register_stanza_plugin(Message, IpopSignal)
            self.registerHandler(
                Callback("ipop", StanzaPath("message/ipop"), self.message_listener))
            # Get the friends list for the user
            self.get_roster()
            # Send sign-on presence
            self.send_presence(pstatus="ident#" + self.node_id)
        except Exception as err:
            self._sig.sig_log("XmppTransport: Exception:{0} Event:{1}"
                              .format(err, event), "LOG_ERROR")

    def presence_event_handler(self, presence):
        """
        Handles peer presence event messages
        """
        try:
            presence_sender = presence["from"]
            presence_receiver_jid = JID(presence["to"])
            presence_receiver = str(presence_receiver_jid.user) + "@" \
                + str(presence_receiver_jid.domain)
            status = presence["status"]
            if(presence_receiver == self.boundjid.bare and presence_sender != self.boundjid.full):
                if (status != "" and "#" in status):
                    pstatus, peer_id = status.split("#")
                    if pstatus == "ident":
                        self.presence_publisher.post_update(
                            dict(PeerId=peer_id, OverlayId=self.overlay_id))
                        self._sig.sig_log("Presence has resolved Peer@Overlay {0}@{1} -> {2}"
                                          .format(peer_id[:7], self.overlay_id, presence_sender))
                        self.jid_cache.add_entry(node_id=peer_id, jid=presence_sender)
                    elif pstatus == "uid?":
                        if self.node_id == peer_id:
                            payload = self.boundjid.full + "#" + self.node_id
                            self.send_msg(presence_sender, "uid!", payload)
                    else:
                        self._sig.sig_log("Unrecognized PSTATUS: {0}".format(pstatus),
                                          "LOG_WARNING")
        except Exception as err:
            self._sig.sig_log("XmppTransport:Exception:{0} presence:{1}".format(err, presence),
                              "LOG_ERROR")

    def message_listener(self, msg):
        """
        Listen for matched messages on the xmpp stream, extract the header
        and payload, and takes suitable action.
        """
        try:
            sender_jid = msg["from"]
            # discard the message if it was initiated by this node
            if sender_jid == self.boundjid.full:
                return
            # extract header and content
            msg_type = msg["ipop"]["type"]
            msg_payload = msg["ipop"]["payload"]
            if msg_type == "uid!":
                match_jid, matched_uid = msg_payload.split("#")
                # put the learned JID in cache
                self.jid_cache.add_entry(matched_uid, match_jid)
                # send the remote actions that are waiting on JID refresh
                cbtq = self.cbts[matched_uid]
                while not cbtq.empty():
                    slot_load = cbtq.get()
                    msg_type, msg_data = slot_load[0], slot_load[1]
                    self.send_msg(match_jid, msg_type, json.dumps(msg_data))
                    self._sig.sig_log("Sent remote action: {0}".format(msg_payload))
                return
            elif msg_type == "invk":
                # invoke the rcvd remote action locally using a CBT
                rem_act = json.loads(msg_payload)
                # if the intended recipient is offline the XMPP server broadcasts the msg to all
                # matching ejabber ids. Verify recipient using Node ID and discard if mismatch
                if rem_act["RecipientId"] != self.node_id:
                    self._sig.sig_log("A mis-delivered remote action was discarded: {0}"
                                      .format(msg_payload))
                    return

                self._sig.sig_log("Rcvd remote action: {0}".format(msg_payload))
                n_cbt = self._sig.create_cbt(self._sig._module_name, rem_act["RecipientCM"],
                                             rem_act["Action"], rem_act["Params"])

                # store the remote action for completion
                self._sig._remote_acts[n_cbt.tag] = rem_act
                self._sig.submit_cbt(n_cbt)
                return
            elif msg_type == "cmpt":
                rem_act = json.loads(msg_payload)
                # Ensure that this node actually initated the remote action
                if rem_act["InitiatorId"] != self.node_id:
                    self._sig.sig_log("A mis-delivered completed remote action was discarded: {0}"
                                      .format(msg_payload), "LOG_WARNING")
                    return
                self._sig.sig_log("Rcvd completed remote act: {0}".format(msg_payload))
                tag = rem_act["ActionTag"]
                cbt_status = rem_act["Status"]
                pending_cbt = self._sig._cfx_handle._pending_cbts[tag]
                pending_cbt.set_response(data=rem_act, status=cbt_status)
                self._sig.complete_cbt(pending_cbt)
            else:
                self._sig.sig_log("Invalid message type received {0}".format(str(msg)),
                                  "LOG_WARNING")
        except Exception as err:
            self._sig.sig_log("XmppTransport:Exception:{0} msg:{1}".format(err, msg),
                              "LOG_ERROR")

    def send_msg(self, peer_jid, msg_type, payload):
        """Send a message to Peer JID via XMPP server"""
        msg = self.Message()
        msg["to"] = peer_jid
        msg["from"] = self.boundjid.full
        msg["type"] = "chat"
        msg["ipop"]["type"] = msg_type
        msg["ipop"]["payload"] = payload
        msg.send()

    def connect_to_server(self,):
        try:
            if self.connect(address=(self.host, self.port)):
                self.process(block=False)
                self._sig.sig_log("Starting connection to XMPP server {0}:{1}"
                                  .format(self.host, self.port))
        except Exception as err:
            self._sig.sig_log("Failed to initialize XMPP transport instanace {}".format(str(err)),
                              "LOG_ERROR")

    def shutdown(self,):
        self.disconnect()


class Signal(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Signal, self).__init__(cfx_handle, module_config, module_name)
        self._presence_publisher = None
        self._circles = {}
        self._remote_acts = {}

    #def _log(self, msg, severity="LOG_INFO"):
    #    self.register_cbt("Logger", severity, msg)

    def create_transport_instance(self, overlay_id, overlay_descr, jid_cache, jid_refresh_q):
        xport = XmppTransport.factory(overlay_id, overlay_descr, self,
                                      self._presence_publisher, jid_cache, jid_refresh_q)
        xport.connect_to_server()
        return xport

    def initialize(self):

        self._presence_publisher = self._cfx_handle.publish_subscription("SIG_PEER_PRESENCE_NOTIFY")
        for overlay_id in self._cm_config["Overlays"]:
            overlay_descr = self._cm_config["Overlays"][overlay_id]
            self._circles[overlay_id] = {}
            self._circles[overlay_id]["JidCache"] = JidCache(self, self._cm_config["CacheExpiry"])
            self._circles[overlay_id]["JidRefreshQ"] = {}
            self._circles[overlay_id]["Transport"] = \
                self.create_transport_instance(overlay_id, overlay_descr,
                                               self._circles[overlay_id]["JidCache"],
                                               self._circles[overlay_id]["JidRefreshQ"])
        self.sig_log("Module loaded", "LOG_INFO")

    def query_reporting_data(self, cbt):
        rpt = {}
        for overlay_id in self._cm_config["Overlays"]:
            rpt[overlay_id] = {
                "xmpp_host": self._circles[overlay_id]["Transport"].host,
                "xmpp_username": self._circles[overlay_id]["Transport"].boundjid.full
            }
        cbt.set_response(rpt, True)
        self.complete_cbt(cbt)

    def initiate_remote_action(self, cbt):
        """
        remote_act = dict(OverlayId="",
                          RecipientId="",
                          RecipientCM="",
                          Action="",
                          Params=json.dumps(opaque_msg),
                          # added by Signal
                          InitiatorId="",
                          InitiatorCM="",
                          ActionTag="",
                          Data="",
                          Status="")
        """
        rem_act = cbt.request.params
        peer_id = rem_act["RecipientId"]
        overlay_id = rem_act["OverlayId"]
        if overlay_id not in self._circles:
            cbt.set_response("Overlay ID not found", False)
            self.complete_cbt(cbt)
            return
        rem_act["InitiatorId"] = self._cm_config["NodeId"]
        rem_act["InitiatorCM"] = cbt.request.initiator
        rem_act["ActionTag"] = cbt.tag
        xmppobj = self._circles[overlay_id]["Transport"]
        jid_cache = self._circles[overlay_id]["JidCache"]
        peer_jid = jid_cache.lookup(peer_id)
        if peer_jid is not None:
            msg_type = "invk"
            payload = json.dumps(rem_act)
            xmppobj.send_msg(str(peer_jid), msg_type, payload)
            self.sig_log("Sent remote act to peer ID: {0}\n Payload: {1}"
                         .format(peer_id, payload))
        else:
            CBTQ = self._circles[overlay_id]["JidRefreshQ"]
            if peer_id not in CBTQ.keys():
                CBTQ[peer_id] = Queue(maxsize=0)
            CBTQ[peer_id].put(("invk", rem_act))
            xmppobj.send_presence(pstatus="uid?#" + peer_id)

    def complete_remote_action(self, cbt):
        rem_act = self._remote_acts[cbt.tag]
        olid = rem_act["OverlayId"]
        peer_id = rem_act["InitiatorId"]
        rem_act["Data"] = cbt.response.data
        rem_act["Status"] = cbt.response.status
        target_jid = self._circles[olid]["JidCache"].lookup(peer_id)
        xmppobj = self._circles[olid]["Transport"]
        if target_jid is None:
            cbt_q = self._circles[olid]["JidRefreshQ"]
            if peer_id not in cbt_q.keys():
                cbt_q[peer_id] = Queue(maxsize=0)
            cbt_q[peer_id].put(("cmpt", rem_act))
            xmppobj.send_presence(pstatus="uid?#" + peer_id)
        else:
            payload = json.dumps(rem_act)
            xmppobj.send_msg(str(target_jid), "cmpt", payload)
            self.sig_log("Sent completed remote action: {}".format(payload))

        self.free_cbt(cbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "SIG_REMOTE_ACTION":
                self.initiate_remote_action(cbt)
            elif cbt.request.action == "SIG_QUERY_REPORTING_DATA":
                self.query_reporting_data(cbt)
            else:
                self.req_handler_default(cbt)
        elif cbt.op_type == "Response":
            if cbt.tag in self._remote_acts:
                self.complete_remote_action(cbt)
            else:
                parent_cbt = self.get_parent_cbt(cbt)
                cbt_data = cbt.response.data
                cbt_status = cbt.response.status
                self.free_cbt(cbt)
                if (parent_cbt is not None and parent_cbt.child_count == 1):
                    parent_cbt.set_response(cbt_data, cbt_status)
                    self.complete_cbt(parent_cbt)

    def timer_method(self):
        for overlay_id in self._circles:
            self._circles[overlay_id]["JidCache"].scavenge()
            self._circles[overlay_id]["Transport"].send_presence(pstatus="ident#" +
                                                                 self._cm_config["NodeId"])

    def terminate(self):
        for overlay_id in self._circles:
            self._circles[overlay_id]["Transport"].shutdown()

    def sig_log(self, msg, level="LOG_DEBUG"):
        self.register_cbt("Logger", level, msg)
