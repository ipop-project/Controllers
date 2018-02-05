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
import sys
import ssl
import time
from controller.framework.ControllerModule import ControllerModule
import threading
try:
    import simplejson as json
except ImportError:
    import json

try:
    import sleekxmpp
    from sleekxmpp.xmlstream.stanzabase import ElementBase, JID
    from sleekxmpp.xmlstream import register_stanza_plugin
    from sleekxmpp.xmlstream.handler.callback import Callback
    from sleekxmpp.xmlstream.matcher import StanzaPath
    from sleekxmpp.stanza.message import Message
except:
    raise ImportError("Sleekxmpp Module not installed")

from queue import Queue
import _thread as thread

# set up a new custom message stanza
class IpopSignal(ElementBase):
    name = "ipop"
    namespace = "signal"
    plugin_attrib = "ipop"
    interfaces = set(("type", "payload"))

class JidCache:
    def __init__(self, cm_mod):
        self.lck = threading.Lock()
        self.cache = {}
        self.cm_mod = cm_mod

    def _log(self, msg, severity="LOG_INFO"):
        self.cm_mod._log(msg, severity)

    def add_entry(self, node_id, jid):
        self.lck.acquire()
        self.cache[node_id] = (jid, time.time())
        self.lck.release()


    def scavenge(self,):
        self.lck.acquire()
        curr_time = time.time()
        keys_to_be_deleted = [key for key, value in self.cache.items() if curr_time - value[1] >= 120]
        for key in keys_to_be_deleted:
            del self.cache[key]
            self._log("Deleted entry from JID cache {0}".format(key), severity="debug")
        self.lck.release()


    def lookup(self, node_id):
        jid = None
        self.lck.acquire()
        ent = self.cache.pop(node_id)
        if (ent is not None):
            jid = ent[0]
            self.cache[node_id] = (jid, time.time())
        self.lck.release()
        return jid

class XmppTransport:
    def __init__(self, overlay_id, overlay_descr, cm_mod, presence_publisher, jid_cache, cbts):
        self.transport = None
        self.overlay_id = overlay_id
        self.overlay_descr = overlay_descr
        self.IsEventHandlerInitialized = False
        self.cm_mod = cm_mod
        self.node_id = cm_mod._cm_config["NodeId"]
        self.presence_publisher = presence_publisher
        self.jid_cache = jid_cache
        self.cbts = cbts
        self._cbt_to_action_tag = {} # maps remote action tags to cbt tags

    def _log(self, msg, severity="LOG_INFO"):
        self.cm_mod._log(msg, severity)

    # Triggered at start of XMPP session
    def start_event_handler(self, event):
        self._log("Start event overlay_id {0}".format(self.overlay_id))
        try:
            # Check whether Callback functions are configured for XMPP
            # server messages
            if self.IsEventHandlerInitialized is False:
                self.IsEventHandlerInitialized = True
                self.transport.get_roster()  # Obtains the friends list for the user
                self.transport.send_presence(pstatus="ident#" + self.node_id)  # Sends signon presence
                # Notification of peer signon
                self.transport.add_event_handler("presence_available", self.presence_event_handler)
                # Register IPOP message with the server
                register_stanza_plugin(Message, IpopSignal)
                self.transport.registerHandler(Callback("ipop", StanzaPath("message/ipop"), self.message_listener))
            else:
                raise RuntimeError("Multiple invocations of start event handler")
        except Exception as err:
            self._log("XmppTransport:Exception:{0} Event:{1}".format(err, event), severity="LOG_ERROR")

    # Callback Function to keep track of Online XMPP Peers
    def presence_event_handler(self, presence):
        try:
            presence_sender = presence["from"]
            presence_receiver_jid = JID(presence["to"])
            presence_receiver = str(presence_receiver_jid.user) + "@" + str(presence_receiver_jid.domain)
            status = presence["status"]
            if presence_receiver == self.overlay_descr["Username"] and presence_sender != self.transport.boundjid.full:
                if (status != "" and "#" in status):
                    pstatus, peer_id = status.split("#")
                    if (pstatus == "ident"):
                        self.presence_publisher.post_update(dict(PeerId = peer_id, OverlayId = self.overlay_id))
                        self._log("Resolved Peer@Overlay {0}@{1} - {2}".format(peer_id[:7], self.overlay_id, presence_sender))
                        self.jid_cache.add_entry(node_id=peer_id, jid=presence_sender)
                    elif (pstatus == "uid?"):
                        if (self.node_id == peer_id):
                            payload = self.transport.boundjid.full + "#" + self.node_id
                            self.send_msg(presence_sender, "uid!", payload)
                    else:
                        self._log("Unrecognized PSTATUS: {0}".format(pstatus))
        except Exception as err:
            self._log("XmppTransport:Exception:{0} presence:{1}".format(err, presence), severity="LOG_ERROR")

    # This handler listens for matched messages on the xmpp stream,
    # extracts the header and payload, and takes suitable action.
    def message_listener(self, msg):
        try:
            receiver_jid = JID(msg["to"])
            sender_jid = msg["from"]
            receiver = str(receiver_jid.user) + "@" + str(receiver_jid.domain)
            # discard the message if it was initiated by this node
            if sender_jid == self.transport.boundjid.full:
                return
            # extract header and content
            type = msg["ipop"]["type"]
            payload = msg["ipop"]["payload"]
            action_tag = msg["ipop"]["action_tag"]

            if type == "uid!":
                match_jid, matched_uid = payload.split("#")
                cbtq = self.cbts[matched_uid]
                # send the remote actions that are waiting on JID refresh
                while not cbtq.empty():
                    cbt_data = cbtq.get()
                    type = "invk"
                    payload = json.dumps(cbt_data)
                    self.send_msg(match_jid, type, payload)
                    self._log("Sent remote act to peer ID: {0}\n "
                        "Payload: {1}".format(matched_uid, payload),
                        "LOG_DEBUG")
                # put the learned JID in cache
                self.jid_cache.add_entry(matched_uid, match_jid)
                return
            elif type == "invk":
                # invoke the rcvd remote action locally using a CBT
                rem_act = json.loads(payload)
                self._log("Rcvd remote act from peer ID: {0}\n "
                    "Payload: {1}".format(rem_act["InitiatorId"], payload),
                    "LOG_DEBUG")
                n_cbt = self.cm_mod.create_cbt(self.cm_mod._module_name, rem_act["RecipientCM"],
                                 rem_act["Action"], rem_act["Params"])
                # store the remote action for completion
                self.cm_mod._remote_acts[n_cbt.tag] = rem_act
                self.cm_mod.submit_cbt(n_cbt)
                return
            elif type == "cmpt":
                rem_act = json.loads(payload)
                tag = rem_act["ActionTag"]
                cbt_status = rem_act["Status"]
                pending_cbt = self.cm_mod._cfx_handle._pending_cbts[tag]
                pending_cbt.set_response(data=rem_act, status=cbt_status)
                self.cm_mod.complete_cbt(pending_cbt)
            else:
                self._log("Invalid message type received {0}".format(str(msg)), "LOG_WARNING")
        except Exception as err:
            self._log("XmppTransport:Exception:{0} msg:{1}".format(err, msg), severity="LOG_ERROR")

    # Send message to Peer JID via XMPP server
    def send_msg(self, peer_jid, type, payload):
        msg = self.transport.Message()
        msg["to"] = peer_jid
        msg["from"] = self.transport.boundjid.full
        msg["type"] = "chat"
        msg["ipop"]["type"] = type
        msg["ipop"]["payload"] = payload
        msg.send()

    def connect_to_server(self,):
        try:
            if self.transport.connect(address=(self.host, self.port)):
                self.transport.process()
                #thread.start_new_thread(self.transport.process, ())
                self._log("Starting connection to XMPP server {0}:{1}".format(self.host, self.port))
        except Exception as err:
            self._log("Unable to initialize XMPP transport instanace.\n" \
                + str(err), severity="LOG_ERROR")

    def initialize(self,):
        self.host = self.overlay_descr["HostAddress"]
        self.port = self.overlay_descr["Port"]
        user = self.overlay_descr.get("Username", None)
        pswd = self.overlay_descr.get("Password", None)
        auth_method = self.overlay_descr.get("AuthenticationMethod", "Password")
        if auth_method == "x509" and (user is not None or pswd is not None):
            er_log = "x509 Authentication is enbabled but credentials exists in IPOP configuration file; x509 will be used."
            self._log(er_log, "LOG_WARNING")
        if auth_method == "x509":
            self.transport = sleekxmpp.ClientXMPP(None, None, sasl_mech="EXTERNAL")
            self.transport.ssl_version = ssl.PROTOCOL_TLSv1
            self.transport.ca_certs = self.overlay_descr["TrustStore"]
            self.transport.certfile = self.overlay_descr["CertDirectory"] + self.overlay_descr["CertFile"]
            self.transport.keyfile = self.overlay_descr["CertDirectory"] + self.overlay_descr["Keyfile"]
            self.transport.use_tls = True
        elif auth_method == "PASSWORD":
            if user is None:
                raise RuntimeError("No username is provided in IPOP configuration file.")
            if pswd is None and self._keyring_installed is True:
                pswd = keyring.get_password("ipop", self.overlay_descr["Username"])
            if pswd is None:
                print("{0}@{1} Password: ".format(user, self.overlay_id))
                pswd = str(raw_input())
                if self._keyring_installed is True:
                    try:
                        keyring.set_password("ipop", xmpp_ele["Username"], pswd)
                    except Exception as err:
                        self._log("Failed to store password in keyring. {0}".format(str(err)), "LOG_ERROR")
        else:
            raise RuntimeError("Invalid authentication method specified in configuration: {0}".format(auth_method))

        self.transport = sleekxmpp.ClientXMPP(user, pswd, sasl_mech="PLAIN")
        del pswd
        # Server SSL Authenication required by default
        if self.overlay_descr.get("AcceptUntrustedServer", False) is True:
            self.transport.register_plugin("feature_mechanisms", pconfig={"unencrypted_plain": True})
            self.transport.use_tls = False
        else:
            self.transport.ca_certs = self.overlay_descr["TrustStore"]
        # event handler for session start and roster update
        self.transport.add_event_handler("session_start", self.start_event_handler)

    def shutdown(self,):
        #TODO: shut down xmpp thread
        pass


class Signal(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Signal, self).__init__(cfx_handle, module_config, module_name)
        self._presence_publisher = None
        self._circles = {}
        self._keyring_installed = False
        self._remote_acts = {}

    def _log(self, msg, severity="LOG_INFO"):
        self.register_cbt("Logger", severity, msg)

    def create_transport_instance(self, overlay_id, overlay_descr, jid_cache, jid_refresh_q):
        self._circles[overlay_id]["Transport"] = XmppTransport(overlay_id,
            overlay_descr, self, self._presence_publisher, jid_cache,
            jid_refresh_q)
        self._circles[overlay_id]["Transport"].initialize()
        self._circles[overlay_id]["Transport"].connect_to_server()

    def initialize(self):
        try:
            import keyring
            self._keyring_installed = True
        except:
            self._log("The key-ring module is not installed.", "LOG_INFO")
        self._presence_publisher = self._cfx_handle.publish_subscription("SIG_PEER_PRESENCE_NOTIFY")
        for overlay_id in self._cm_config["Overlays"]:
            overlay_descr = self._cm_config["Overlays"][overlay_id]
            self._circles[overlay_id] = {}
            self._circles[overlay_id]["JidCache"] = JidCache(self)
            self._circles[overlay_id]["XmppUser"] = overlay_descr["Username"] #TODO: Is this needed?
            self._circles[overlay_id]["IsEventHandlerInitialized"] = False #TODO: Is this needed?
            self._circles[overlay_id]["JidRefreshQ"] = {}
            self.create_transport_instance(overlay_id, overlay_descr,
                self._circles[overlay_id]["JidCache"],
                self._circles[overlay_id]["JidRefreshQ"])
        self._log("Module loaded")

    def query_reporting_data(self, data):
        rpt = {}
        for overlay_id in self._cm_config["Overlays"]:
            rpt[overlay_id] = {
                "xmpp_host": self._circles[overlay_id]["Transport"].host,
                "xmpp_username": self._circles[overlay_id]["Transport"].overlay_descr.get("Username", None)
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
        if (overlay_id not in self._circles):
            cbt.set_response("Overlay ID not found", False)
            self.complete_cbt(cbt)
            return
        rem_act["InitiatorId"] = self._cm_config["NodeId"]
        rem_act["InitiatorCM"] = cbt.request.initiator
        rem_act["ActionTag"] = cbt.tag
        xmppobj = self._circles[overlay_id]["Transport"]
        jid_cache = self._circles[overlay_id]["JidCache"]
        #cache_lk.acquire()
        peer_jid = jid_cache.lookup(peer_id)
        if peer_jid is not None:
            type = "invk"
            payload = json.dumps(rem_act)
            xmppobj.send_msg(str(peer_jid), type, payload)
            self._log("Sent remote act to peer ID: {0}\n "
                "Payload: {1}".format(peer_id, payload),
                "LOG_DEBUG")
        else:
            #cache_lk.release()
            CBTQ = self._circles[overlay_id]["JidRefreshQ"]
            if peer_id not in CBTQ.keys():
                CBTQ[peer_id] = Queue(maxsize=0)
            CBTQ[peer_id].put(rem_act)
            xmppobj.send_presence(pstatus="uid?#" + peer_id)

    def complete_remote_action(self, cbt):
        if cbt.tag in self._remote_acts:
            rem_act = self._remote_acts[cbt.tag]
            olid = rem_act["OverlayId"]
            target_jid = self._circles[olid]["JidCache"].lookup(rem_act["InitiatorId"])
            if (target_jid is None):
                pass # TODO: refresh JID
            rem_act["Data"] = cbt.response.data
            rem_act["Status"] = cbt.response.status
            xmppobj = self._circles[olid]["Transport"]
            xmppobj.send_msg(str(target_jid), "cmpt", json.dumps(rem_act))
            self.free_cbt(cbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "SIG_REMOTE_ACTION":
                self.initiate_remote_action(cbt)
            elif cbt.request.action == "SIG_QUERY_REPORTING_DATA":
                self.query_reporting_data(cbt)
        elif cbt.op_type == "Response":
            if (cbt.response.status == False):
                self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
                self.free_cbt(cbt)
            else:
                self.complete_remote_action(cbt)

    def timer_method(self):
        # Clean up JID cache for all XMPP connections
        for overlay_id in self._circles.keys():
            pass #self._circles[overlay_id]["JidCache"].scavenge()

    def terminate(self):
        for overlay_id in self._circles.keys():
            self._circles[overlay_id]["Transport"].shutdown
