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
import socket
import select
import json
import ast
import controller.framework.ipoplib as ipoplib
import controller.framework.fxlib as fxlib
from threading import Thread
import uuid


class TincanInterface(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(TincanInterface, self).__init__(cfx_handle, module_config, module_name)
        self._tincan_listener_thread = None    # UDP listener thread object
        self.control_cbt = {}
        # Preference for IPv6 control link
        if socket.has_ipv6:
            self._sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self._sock_svr = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            # Controller UDP listening socket
            self._sock_svr.bind((self._cm_config["ServiceAddress6"], self._cm_config["CtrlRecvPort"]))
            # Controller UDP sending socket
            self._dest = (self._cm_config["ServiceAddress6"], self._cm_config["CtrlSendPort"])
        else:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock_svr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Controller UDP listening socket
            self._sock_svr.bind((self._cm_config["ServiceAddress"], self._cm_config["CtrlRecvPort"]))
            # Controller UDP sending socket
            self._dest = (self._cm_config["ServiceAddress"], self._cm_config["CtrlSendPort"])
        self._sock.bind(("", 0))
        self._sock_list = [self._sock_svr]

    def initialize(self):
        self._tincan_listener_thread = Thread(target=self.__tincan_listener)
        self._tincan_listener_thread.setDaemon(True)
        self._tincan_listener_thread.start()
        self.CreateControlLink()
        self.ConfigureTincanLogging(None, True)
        self.register_cbt("Logger", "LOG_INFO", "Module loaded")
        #self.register_cbt("Logger", "LOG_QUERY_CONFIG")

    def __tincan_listener(self):
        while True:
            socks, _, _ = select.select(self._sock_list, [], [],
                                        self._cm_config["SocketReadWaitTime"])
            # Iterate across all socket list to obtain Tincan messages
            for sock in socks:
                if sock == self._sock_svr:
                    data, addr = sock.recvfrom(self._cm_config["MaxReadSize"])
                    ctl = json.loads(data.decode("utf-8"))
                    if ctl["IPOP"]["ProtocolVersion"] != 5:
                        raise ValueError("Invalid control version detected");
                    #Get the original CBT if this is the response
                    if ctl["IPOP"]["ControlType"] == "TincanResponse":
                        cbt = self.control_cbt[ctl["IPOP"]["TransactionId"]]
                        cbt.set_response(ctl["IPOP"]["Response"]["Message"], ctl["IPOP"]["Response"]["Success"])
                        self.complete_cbt(cbt)
                    else:
                        self.register_cbt("TincanInterface", "TCI_TINCAN_REQ", ctl["IPOP"]["Request"])

    def CreateControlLink(self,):
        self.register_cbt("Logger", "LOG_INFO", "Creating Tincan control link")
        cbt = self.create_cbt(self._module_name, self._module_name, "TCI_CREATE_CTRL_LINK")
        ctl = ipoplib.CTL_CREATE_CTRL_LINK
        ctl["IPOP"]["TransactionId"] = cbt.tag
        if self._cm_config["CtrlRecvPort"] is not None:
          ctl["IPOP"]["Request"]["Port"] = self._cm_config["CtrlRecvPort"]
        if socket.has_ipv6 is False:
            ctl["IPOP"]["Request"]["AddressFamily"] = "af_inet"
            ctl["IPOP"]["Request"]["IP"] = self._cm_config["ServiceAddress"]
        else:
            ctl["IPOP"]["Request"]["AddressFamily"] = "af_inetv6"
            ctl["IPOP"]["Request"]["IP"] = self._cm_config["ServiceAddress6"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def CreateControlLinkResp(self, cbt):
        if cbt.response.status == "False":
            msg = "Failed to create Tincan response link: CBT={0}".format(cbt)
            raise RuntimeError(msg)

    def ConfigureTincanLogging(self, log_cfg, use_defaults=False):
        cbt = self.create_cbt(self._module_name, self._module_name, "TCI_CONFIGURE_LOGGING")
        ctl = ipoplib.CTL_CONFIGURE_LOGGING
        ctl["IPOP"]["TransactionId"] = cbt.tag
        if(not use_defaults):
            ctl["IPOP"]["Request"]["Level"] = log_cfg["LogLevel"]
            ctl["IPOP"]["Request"]["Device"] = log_cfg["Device"]
            ctl["IPOP"]["Request"]["Directory"] = log_cfg["Directory"]
            ctl["IPOP"]["Request"]["Filename"] = log_cfg["TincanLogFileName"]
            ctl["IPOP"]["Request"]["MaxArchives"] = log_cfg["MaxArchives"]
            ctl["IPOP"]["Request"]["MaxFileSize"] = log_cfg["MaxFileSize"]
            ctl["IPOP"]["Request"]["ConsoleLevel"] = log_cfg["ConsoleLevel"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ConfigureTincanLoggingResp(self, cbt):
        if cbt.response.status == "False":
            msg = "Failed to configure Tincan logging: CBT={0}".format(cbt)
            self.register_cbt("Logger", "LOG_WARNING", msg)

    def ReqCreateLink(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_CREATE_LINK
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        req["EncryptionEnabled"] = msg["EncryptionEnabled"]
        req["PeerInfo"]["VIP4"] = msg["NodeData"]["IP4"]
        req["PeerInfo"]["UID"] = msg["NodeData"]["UID"]
        req["PeerInfo"]["MAC"] = msg["NodeData"]["MAC"]
        req["PeerInfo"]["CAS"] = msg["NodeData"]["CAS"]
        req["PeerInfo"]["FPR"] = msg["NodeData"]["FPR"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqCreateOverlay(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_CREATE_OVERLAY
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["StunAddress"] = msg["StunAddress"]
        req["TurnAddress"] = msg["TurnAddress"]
        req["TurnPass"] = msg["TurnPass"]
        req["TurnUser"] = msg["TurnUser"]
        req["EnableIPMapping"] = msg["EnableIPMapping"]
        req["Type"] = msg["Type"]
        req["TapName"] = msg["TapName"]
        req["IP4"] = msg["IP4"]
        req["PrefixLen4"] = msg["PrefixLen4"]
        req["MTU4"] = msg["MTU4"]
        req["OverlayId"] = msg["OverlayId"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqInjectFrame(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_CREATE_OVERLAY
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["Data"] = msg["Data"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqQueryCandidateAddressSet(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_QUERY_CAS
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqQueryLinkStats(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_QUERY_LINK_STATS
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayIds"] = msg
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqQueryOverlayInfo(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_QUERY_OVERLAY_INFO
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqRemoveOverlay(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_CREATE_OVERLAY
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqRemoveLink(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_CREATE_LINK
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["LinkId"] = msg["LinkId"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqSendICC(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_SEND_ICC
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        req["Data"] = msg["Data"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    def ReqSetIgnoredNetInterfaces(self, cbt):
        msg = cbt.request.params
        ctl = ipoplib.CTL_SET_IGNORED_NET_INTERFACES
        ctl["IPOP"]["TransactionId"] = cbt.tag
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["IgnoredNetInterfaces"] = msg["IgnoredNetInterfaces"]
        self.control_cbt[cbt.tag] = cbt
        self.SendControl(json.dumps(ctl))

    #rework ICC messaging necessary
    def ProcessTincanRequest(self, cbt):
        if cbt.request.params["Command"] == "ICC":
            msg = {
                "OverlayId": cbt.request.params["OverlayId"],
                "LinkId": cbt.request.params["LinkId"],
                "Data": cbt.request.params["Data"]
                }
            self.register_cbt("InterControllerCommunicator", "ICC_RECIEVE", msg)
        else:
            erlog = "Unsupported request received from Tincan"
            self.register_cbt("Logger", "LOG_WARNING", erlog)
        self.complete_cbt(cbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "TCI_CREATE_LINK":
                self.ReqCreateLink(cbt)

            elif cbt.request.action == "TCI_REMOVE_LINK":
                self.ReqRemoveLink(cbt)

            elif cbt.request.action == "TCI_CREATE_OVERLAY":
                self.ReqCreateOverlay(cbt)

            elif cbt.request.action == "TCI_ICC":
                self.ReqICC(cbt)

            elif cbt.request.action == "TCI_INJECT_FRAME":
                self.ReqInjectFrame(cbt)

            elif cbt.request.action == "TCI_QUERY_CAS":
                self.ReqQueryCandidateAddressSet(cbt)

            elif cbt.request.action == "TCI_QUERY_LINK_STATS":
                self.ReqQueryLinkStats(cbt)

            elif cbt.request.action == "TCI_QUERY_OVERLAY_INFO":
                self.ReqQueryOverlayInfo(cbt)

            elif cbt.request.action == "TCI_REMOVE_OVERLAY":
                self.ReqRemoveOverlay(cbt)

            elif cbt.request.action == "TCI_SET_IGNORED_NET_INTERFACES":
                self.ReqSetIgnoredNetInterfaces(cbt)

            elif cbt.request.action == "TCI_TINCAN_REQ":
                self.ProcessTincanRequest(cbt)

        elif cbt.op_type == "Response":
            if cbt.request.action == "LOG_QUERY_CONFIG":
                self.ConfigureTincanLogging(cbt.response.data, not cbt.response.status)

            elif cbt.request.action == "TCI_CREATE_CTRL_LINK":
                self.CreateControlLinkResp(cbt)

            elif cbt.request.action == "TCI_CONFIGURE_LOGGING":
                self.ConfigureTincanLoggingResp(cbt)

            self.free_cbt(cbt)


    def SendControl(self, msg):
        return self._sock.sendto(bytes(msg.encode("utf-8")), self._dest)

    def timer_method(self):
        pass

    def terminate(self):
        pass
