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

import socket
import select
try:
    import simplejson as json
except ImportError:
    import json
import controller.framework.ipoplib as ipoplib
from threading import Thread
import traceback
from controller.framework.ControllerModule import ControllerModule


class TincanInterface(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(TincanInterface, self).__init__(cfx_handle, module_config, module_name)
        self._tincan_listener_thread = None    # UDP listener thread object
        self._tci_publisher = None
        # Preference for IPv6 control link
        if socket.has_ipv6:
            self._sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self._sock_svr = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            # Controller UDP listening socket
            self._sock_svr.bind((self._cm_config["RcvServiceAddress6"], self._cm_config["CtrlRecvPort"]))
            # Controller UDP sending socket
            self._dest = (self._cm_config["SndServiceAddress6"], self._cm_config["CtrlSendPort"])
        else:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock_svr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Controller UDP listening socket
            self._sock_svr.bind((self._cm_config["RcvServiceAddress"], self._cm_config["CtrlRecvPort"]))
            # Controller UDP sending socket
            self._dest = (self._cm_config["SndServiceAddress"], self._cm_config["CtrlSendPort"])
        self._sock.bind(("", 0))
        self._sock_list = [self._sock_svr]

    def initialize(self):
        self._tincan_listener_thread = Thread(target=self.__tincan_listener)
        self._tincan_listener_thread.setDaemon(True)
        self._tincan_listener_thread.start()
        self.create_control_link()
        self._tci_publisher = self._cfx_handle.publish_subscription("TCI_TINCAN_MSG_NOTIFY")
        #self.configure_tincan_logging(None, True)
        self.register_cbt("Logger", "LOG_QUERY_CONFIG")
        self.register_cbt("Logger", "LOG_INFO", "Module loaded")

    def __tincan_listener(self):
        try:
            while True:
                socks, _, _ = select.select(self._sock_list, [], [],
                                            self._cm_config["SocketReadWaitTime"])
                # Iterate across all socket list to obtain Tincan messages
                for sock in socks:
                    if sock == self._sock_svr:
                        data, addr = sock.recvfrom(self._cm_config["MaxReadSize"])
                        ctl = json.loads(data.decode("utf-8"))
                        if ctl["IPOP"]["ProtocolVersion"] != 5:
                            raise ValueError("Invalid control version detected")
                        # Get the original CBT if this is the response
                        if ctl["IPOP"]["ControlType"] == "TincanResponse":
                            cbt = self._cfx_handle._pending_cbts[ctl["IPOP"]["TransactionId"]]
                            cbt.set_response(ctl["IPOP"]["Response"]["Message"], ctl["IPOP"]["Response"]["Success"])
                            self.complete_cbt(cbt)
                        else:
                            self._tci_publisher.post_update(ctl["IPOP"]["Request"])
        except:
            log_cbt = self.register_cbt(
                "Logger", "LOG_WARNING", "Tincan Listener exception:\n"
                        "{0}".format(traceback.format_exc()))
            self.submit_cbt(log_cbt)

    def create_control_link(self,):
        self.register_cbt("Logger", "LOG_INFO", "Creating Tincan control link")
        cbt = self.create_cbt(self._module_name, self._module_name, "TCI_CREATE_CTRL_LINK")
        ctl = ipoplib.CTL_CREATE_CTRL_LINK
        ctl["IPOP"]["TransactionId"] = cbt.tag
        if self._cm_config["CtrlRecvPort"] is not None:
            ctl["IPOP"]["Request"]["Port"] = self._cm_config["CtrlRecvPort"]
        if socket.has_ipv6 is False:
            ctl["IPOP"]["Request"]["AddressFamily"] = "af_inet"
            ctl["IPOP"]["Request"]["IP"] = self._cm_config["RcvServiceAddress"]
        else:
            ctl["IPOP"]["Request"]["AddressFamily"] = "af_inetv6"
            ctl["IPOP"]["Request"]["IP"] = self._cm_config["RcvServiceAddress6"]
        self._cfx_handle._pending_cbts[cbt.tag] = cbt
        self.send_control(json.dumps(ctl))

    def resp_handler_create_control_link(self, cbt):
        if cbt.response.status == "False":
            msg = "Failed to create Tincan response link: CBT={0}".format(cbt)
            raise RuntimeError(msg)

    def configure_tincan_logging(self, log_cfg, use_defaults=False):
        cbt = self.create_cbt(self._module_name, self._module_name, "TCI_CONFIGURE_LOGGING")
        ctl = ipoplib.CTL_CONFIGURE_LOGGING
        ctl["IPOP"]["TransactionId"] = cbt.tag
        if not use_defaults:
            ctl["IPOP"]["Request"]["Level"] = log_cfg["LogLevel"]
            ctl["IPOP"]["Request"]["Device"] = log_cfg["Device"]
            ctl["IPOP"]["Request"]["Directory"] = log_cfg["Directory"]
            ctl["IPOP"]["Request"]["Filename"] = log_cfg["TincanLogFileName"]
            ctl["IPOP"]["Request"]["MaxArchives"] = log_cfg["MaxArchives"]
            ctl["IPOP"]["Request"]["MaxFileSize"] = log_cfg["MaxFileSize"]
            ctl["IPOP"]["Request"]["ConsoleLevel"] = log_cfg["ConsoleLevel"]
        self._cfx_handle._pending_cbts[cbt.tag] = cbt
        self.send_control(json.dumps(ctl))

    def resp_handler_configure_tincan_logging(self, cbt):
        if cbt.response.status == "False":
            msg = "Failed to configure Tincan logging: CBT={0}".format(cbt)
            self.register_cbt("Logger", "LOG_WARNING", msg)

    def req_handler_create_link(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_CREATE_LINK
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        req["PeerInfo"]["VIP4"] = msg["NodeData"].get("VIP4")
        req["PeerInfo"]["UID"] = msg["NodeData"].get("UID")
        req["PeerInfo"]["MAC"] = msg["NodeData"].get("MAC")
        req["PeerInfo"]["CAS"] = msg["NodeData"].get("CAS")
        req["PeerInfo"]["FPR"] = msg["NodeData"].get("FPR")
        # Optional overlay data to create overlay on demand
        req["StunAddress"] = msg.get("StunAddress")
        req["TurnAddress"] = msg.get("TurnAddress")
        req["TurnPass"] = msg.get("TurnPass")
        req["TurnUser"] = msg.get("TurnUser")
        req["Type"] = msg["Type"]
        req["TapName"] = msg.get("TapName")
        req["IP4"] = msg.get("IP4")
        req["IP4PrefixLen"] = msg.get("IP4PrefixLen")
        req["MTU4"] = msg.get("MTU4")
        self.send_control(json.dumps(ctl))

    def req_handler_create_overlay(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_CREATE_OVERLAY
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["StunAddress"] = msg["StunAddress"]
        req["TurnAddress"] = msg["TurnAddress"]
        req["TurnPass"] = msg["TurnPass"]
        req["TurnUser"] = msg["TurnUser"]
        req["Type"] = msg["Type"]
        req["TapName"] = msg["TapName"]
        req["IP4"] = msg["IP4"]
        req["IP4PrefixLen"] = msg["IP4PrefixLen"]
        req["MTU4"] = msg["MTU4"]
        req["OverlayId"] = msg["OverlayId"]
        self.send_control(json.dumps(ctl))

    def req_handler_inject_frame(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.INSERT_TAP_PACKET
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["Data"] = msg["Data"]
        self.send_control(json.dumps(ctl))

    def req_handler_query_candidate_address_set(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_QUERY_CAS
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        self.send_control(json.dumps(ctl))

    def req_handler_query_link_stats(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_QUERY_LINK_STATS
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayIds"] = msg
        self.send_control(json.dumps(ctl))

    def req_handler_query_overlay_info(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_QUERY_OVERLAY_INFO
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        self.send_control(json.dumps(ctl))

    def req_handler_remove_overlay(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_REMOVE_OVERLAY
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        self.send_control(json.dumps(ctl))

    def req_handler_remove_link(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_REMOVE_LINK
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        self.send_control(json.dumps(ctl))

    def req_handler_send_icc(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_SEND_ICC
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        req["Data"] = msg["Data"]
        self.send_control(json.dumps(ctl))

    def req_handler_set_ignored_net_interfaces(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_SET_IGNORED_NET_INTERFACES
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["IgnoredNetInterfaces"] = msg["IgnoredNetInterfaces"]
        self.send_control(json.dumps(ctl))

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "TCI_CREATE_LINK":
                self.req_handler_create_link(cbt)

            elif cbt.request.action == "TCI_REMOVE_LINK":
                self.req_handler_remove_link(cbt)

            elif cbt.request.action == "TCI_CREATE_OVERLAY":
                self.req_handler_create_overlay(cbt)

            elif cbt.request.action == "TCI_ICC":
                self.req_handler_send_icc(cbt)

            elif cbt.request.action == "TCI_INJECT_FRAME":
                self.req_handler_inject_frame(cbt)

            elif cbt.request.action == "TCI_QUERY_CAS":
                self.req_handler_query_candidate_address_set(cbt)

            elif cbt.request.action == "TCI_QUERY_LINK_STATS":
                self.req_handler_query_link_stats(cbt)

            elif cbt.request.action == "TCI_QUERY_OVERLAY_INFO":
                self.req_handler_query_overlay_info(cbt)

            elif cbt.request.action == "TCI_REMOVE_OVERLAY":
                self.req_handler_remove_overlay(cbt)

            elif cbt.request.action == "TCI_SET_IGNORED_NET_INTERFACES":
                self.req_handler_set_ignored_net_interfaces(cbt)

            else:
                self.req_handler_default(cbt)
        elif cbt.op_type == "Response":
            if cbt.request.action == "LOG_QUERY_CONFIG":
                self.configure_tincan_logging(cbt.response.data,
                                              not cbt.response.status)

            elif cbt.request.action == "TCI_CREATE_CTRL_LINK":
                self.resp_handler_create_control_link(cbt)

            elif cbt.request.action == "TCI_CONFIGURE_LOGGING":
                self.resp_handler_configure_tincan_logging(cbt)

            self.free_cbt(cbt)

    def send_control(self, msg):
        return self._sock.sendto(bytes(msg.encode("utf-8")), self._dest)

    def timer_method(self):
        pass

    def terminate(self):
        pass
