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
import socket,select,json,ast
import controller.framework.ipoplib as ipoplib
import controller.framework.fxlib as fxlib
from threading import Thread
import uuid


class TincanInterface(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(TincanInterface, self).__init__(CFxHandle, paramDict, ModuleName)
        self.trans_counter = 0  # control transaction Ids
        self.TincanListenerThread = None    # UDP listener thread object
        self.ControlCbt = {}
        # Preference for IPv6 control link
        if socket.has_ipv6:
            self.sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self.sock_svr = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            # Controller UDP listening socket
            self.sock_svr.bind((self.CMConfig["ServiceAddress6"], self.CMConfig["CtrlRecvPort"]))
            # Controller UDP sending socket
            self.dest = (self.CMConfig["ServiceAddress6"], self.CMConfig["CtrlSendPort"])
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_svr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Controller UDP listening socket
            self.sock_svr.bind((self.CMConfig["ServiceAddress"], self.CMConfig["CtrlRecvPort"]))
            # Controller UDP sending socket
            self.dest = (self.CMConfig["ServiceAddress"], self.CMConfig["CtrlSendPort"])
        self.sock.bind(("", 0))
        self.sock_list = [self.sock_svr]

    def initialize(self):
        self.TincanListenerThread = Thread(target=self.__tincan_listener)
        self.TincanListenerThread.setDaemon(True)
        self.TincanListenerThread.start()
        self.CreateControlLink()
        self.registerCBT('Logger', 'LOG_INFO', "Module loaded")
        self.registerCBT('Logger', 'LOG_QUERY_CONFIG',)

    def __tincan_listener(self):
        while True:
            socks, _, _ = select.select(self.sock_list, [], [],
                                        self.CMConfig["SocketReadWaitTime"])
            # Iterate across all socket list to obtain Tincan messages
            for sock in socks:
                if sock == self.sock_svr:
                    data, addr = sock.recvfrom(self.CMConfig["MaxReadSize"])
                    ctl = json.loads(data.decode("utf-8"))
                    if ctl["IPOP"]["ProtocolVersion"] != 4:
                        raise ValueError("Invalid control version detected");
                    #Get the original CBT if this is the response
                    if ctl["IPOP"]["ControlType"] == "TincanResponse":
                        cbt = self.ControlCbt[ctl["IPOP"]["TransactionId"]]
                        cbt.SetResponse(self.ModuleName, ctl["IPOP"]["Request"]["Owner"], ctl["IPOP"]["Response"]["Message"], ctl["IPOP"]["Response"]["Success"])
                        self.CFxHandle.CompleteCBT(cbt)
                    else:
                        self.registerCBT("TincanInterface", "TCI_TINCAN_REQ", ctl["IPOP"]["Request"])

    def ConfigureLogging(self,):
        pass
    def CreateCtrlRespLink(self,):
        pass

    def ReqCreateLink(self, cbt):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_CREATE_LINK
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        #self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        req["EncryptionEnabled"] = msg["EncryptionEnabled"]
        req["PeerInfo"]["VIP4"] = msg["PeerInfo"]['IP4']
        req["PeerInfo"]["UID"] = msg["PeerInfo"]["UID"]
        req["PeerInfo"]["MAC"] = msg["PeerInfo"]["MAC"]
        req["PeerInfo"]["CAS"] = msg["PeerInfo"]["CAS"]
        req["PeerInfo"]["FPR"] = msg["PeerInfo"]["FPR"]
        self.SendControl(json.dumps(ctl))
        self.ControlCbt[cbt.Tag] = cbt

    def ReqCreateOverlay(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_CREATE_OVERLAY
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["StunAddress"] = msg["StunAddress"]
        req["TurnAddress"] = msg["TurnAddress"]
        req["TurnPass"] = msg["TurnPass"]
        req["TurnUser"] = msg["TurnUser"]
        req["EnableIPMapping"] = msg["EnableIPMapping"]
        req["TapName"] = msg["TapName"]
        req["IP4"] = msg["IP4"]
        req["PrefixLen4"] = msg["PrefixLen4"]
        req["MTU4"] = msg["MTU4"]
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt

    def RespCreateOverlay(self,):
        pass

    def ReqICC(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_ICC
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        req["Recipient"] = msg["PeerUID"]
        req["Data"] = json.dumps(msg)
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt

    def RespICC(self,):
        pass

    def ReqInjectFrame(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_CREATE_OVERLAY
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["Data"] = msg["Data"]
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt

    def RespInjectFrame(self,):
        pass

    def ReqQueryCandidateAddressSet(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_QUERY_CAS
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt

    def RespQueryCandidateAddressSet(self,):
        pass

    def ReqQueryLinkStats(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_QUERY_LINK_STATS
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["LinkId"] = msg["LinkId"]
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt

    def RespQueryLinkStats(self,):
        pass

    def ReqQueryOverlayInfo(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_QUERY_OVERLAY_INFO
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt

    def RespQueryOverlayInfo(self,):
        pass

    def ReqRemoveOverlay(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_CREATE_OVERLAY
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt


    def RespRemoveOverlay(self,):
        pass

    def ReqRemoveLink(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_CREATE_LINK
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["LinkId"] = msg["LinkId"]
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt

    def RespRemoveLink(self,):
        pass

    def ReqSetIgnoredNetInterfaces(self,):
        msg = cbt.Request.Data
        ctl = ipoplib.CTL_SET_IGNORED_NET_INTERFACES
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = cbt.Request.Intiator
        req = ctl["IPOP"]["Request"]
        req["OverlayId"] = msg["OverlayId"]
        req["IgnoredNetInterfaces"] = msg["IgnoredNetInterfaces"]
        self.SendControl(json.dumps(remove_node_details))
        self.ControlCbt[cbt.Tag] = cbt

    def RespSetIgnoredNetInterfaces(self,):
        pass
    
    def ProcessTincanRequest(self, cbt):
        pass
    
    def processCBT(self, cbt):
        if cbt.Request.Action == "TCI_CREATE_LINK":
            if cbt.OpType == "Request":
                self.ReqCreateLink(cbt)
            else:
                self.RespCreateLink(cbt)

        elif cbt.Request.Action == "TCI_REMOVE_LINK":
            if cbt.OpType == "Request":
                self.Req(cbt)
            else:
                self.Resp(cbt)

        elif cbt.Request.Action == "TCI_CREATE_OVERLAY":
            if cbt.OpType == "Request":
                self.ReqCreateOverlay(cbt)
            else:
                self.RespCreateOverlay(cbt)

        elif cbt.Request.Action == "TCI_ICC":
            if cbt.OpType == "Request":
                self.ReqICC(cbt)
            else:
                self.RespICC(cbt)

        elif cbt.Request.Action == "TCI_INJECT_FRAME":
            if cbt.OpType == "Request":
                self.ReqInjectFrame(cbt)
            else:
                self.RespInjectFrame(cbt)

        elif cbt.Request.Action == "TCI_QUERY_CAS":
            if cbt.OpType == "Request":
                self.ReqQueryCandidateAddressSet(cbt)
            else:
                self.RespQueryCandidateAddressSet(cbt)

        elif cbt.Request.Action == "TCI_QUERY_LINK_STATS":
            if cbt.OpType == "Request":
                self.ReqQueryLinkStats(cbt)
            else:
                self.RespQueryLinkStats(cbt)

        elif cbt.Request.Action == "TCI_QUERY_OVERLAY_INFO":
            if cbt.OpType == "Request":
                self.ReqQueryOverlayInfo(cbt)
            else:
                self.RespQueryOverlayInfo(cbt)

        elif cbt.Request.Action == "TCI_REMOVE_OVERLAY":
            if cbt.OpType == "Request":
                self.ReqRemoveOverlay(cbt)
            else:
                self.RespRemoveOverlay(cbt)

        elif cbt.Request.Action == "TCI_SET_IGNORED_NET_INTERFACEs":
            if cbt.OpType == "Request":
                self.ReqSetIgnoredNetInterfaces(cbt)
            else:
                self.RespSetIgnoredNetInterfaces(cbt)

        elif cbt.Request.Action == "TCI_TINCAN_REQ" and cbt.OpType == "Request":
            self.ProcessTincanRequest(cbt)

        elif cbt.Request.Action == "LOG_QUERY_CONFIG" and cbt.OpType == "Response":
            self.ConfigureTincanLogging(cbt.Response.Data)

        elif cbt.Request.Action == "TCI_CREATE_CTRL_LINK" and cbt.OpType == "Response":
            print(cbt)
            self.CFxHandle.FreeCBT(cbt)

        elif cbt.Request.Action == "TCI_CONFIGURE_LOGGING" and cbt.OpType == "Response":
            print(cbt)
            self.CFxHandle.FreeCBT(cbt)

        else:
            log = "Unsupported CBT {0}".format(cbt)
            self.registerCBT('Logger', 'LOG_WARNING', log)

    def SendControl(self, msg):
        return self.sock.sendto(bytes(msg.encode('utf-8')), self.dest)

    def timer_method(self):
        pass

    def terminate(self):
        pass

    def CreateControlLink(self,):
        self.registerCBT("Logger", "info", "Creating Tincan control link")
        cbt = self.CFxHandle.createCBT(self.ModuleName, self.ModuleName, "TCI_CREATE_CTRL_LINK", None)
        self.ControlCbt[cbt.Tag] = cbt
        ctl = ipoplib.CTL_CREATE_CTRL_LINK
        if self.CMConfig["CtrlRecvPort"] is not None:
          ctl["IPOP"]["Request"]["Port"] = self.CMConfig["CtrlRecvPort"]
        if socket.has_ipv6 is False:
            ctl["IPOP"]["Request"]["AddressFamily"] = "af_inet"
            ctl["IPOP"]["Request"]["IP"] = self.CMConfig["ServiceAddress"]
        else:
            ctl["IPOP"]["Request"]["AddressFamily"] = "af_inetv6"
            ctl["IPOP"]["Request"]["IP"] = self.CMConfig["ServiceAddress6"]
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = self.ModuleName
        self.SendControl(json.dumps(ctl))

    def ConfigureTincanLogging(self, log_cfg):
        cbt = self.CFxHandle.createCBT(self.ModuleName, self.ModuleName, "TCI_CONFIGURE_LOGGING", None)
        self.ControlCbt[cbt.Tag] = cbt
        ctl = ipoplib.CTL_CONFIGURE_LOGGING
        ctl["IPOP"]["Request"]["Level"] = log_cfg["LogLevel"]
        ctl["IPOP"]["Request"]["Device"] = log_cfg["LogOption"]
        ctl["IPOP"]["Request"]["Directory"] = log_cfg["LogFilePath"]
        ctl["IPOP"]["Request"]["Filename"] = log_cfg["TincanLogFileName"]
        ctl["IPOP"]["Request"]["MaxArchives"] = log_cfg["BackupLogFileCount"]
        ctl["IPOP"]["Request"]["MaxFileSize"] = log_cfg["LogFileSize"]
        ctl["IPOP"]["Request"]["ConsoleLevel"] = log_cfg["ConsoleLevel"]
        ctl["IPOP"]["TransactionId"] = cbt.Tag
        self.trans_counter += 1
        ctl["IPOP"]["Request"]["Owner"] = self.ModuleName
        self.SendControl(json.dumps(ctl))
