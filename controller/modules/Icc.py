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

class Icc(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Icc, self).__init__(cfx_handle, module_config, module_name)
        #Dictionary to hold data about overlayID->peerID->linkID mappings
        self._links = {}

    def initialize(self):
        # self.register_cbt("Logger", "LOG_INFO", "{0} Loaded".format(self.module_name))
        self.register_cbt("Logger", "LOG_INFO", "ICC Module Loaded")
        # Subscribe for link updates notifications from LinkManager
        self._cfx_handle.start_subscription("LinkManager",
                    "LNK_DATA_UPDATES")

    def update_links(self,cbt):
        if cbt.request.params["UpdateType"] == "ADDED":
            olid = cbt.request.params["OverlayId"]
            peerid = cbt.request.params["PeerId"]
            lnkid = cbt.request.params["LinkId"]
            if olid in self._links:
                self._links[olid]["Peers"][peerid] = lnkid
            else:
                self._links[olid] = {}
                self._links[olid]["Peers"] = {}
                self._links[olid]["Peers"][peerid] = lnkid

        else if cbt.request.params["UpdateType"] == "REMOVED":
            olid = cbt.request.params["OverlayId"]
            lnkid = cbt.request.params["LinkId"]
            for peerid in self._links[olid]["Peers"]:
                if self._links[olid]["Peers"][peerid] == lnkid:
                   del self._links[olid]["Peers"][peerid]

    def send_icc_data(self,cbt):
        param = {}
        olid = cbt.request.params["OverlayId"]
        peerid = cbt.request.params["PeerId"]
        param["OverlayId"] = olid
        param["LinkId"] = self._links[olid]["Peers"][peerid]
        param["IccType"] = "DATA"
        
        lcbt = self.create_linked_cbt(cbt)
        lcbt.set_request("TincanInterface", "TCI_ICC", param)
        self.submit_cbt(lcbt)

        #TODO: As these CBTs won't get any response, should they be completed  ??
        cbt.set_response(data="Data Sent", status=True)
        self.complete_cbt(cbt)

    def broadcast_icc_data(self,cbt):
        peer_list = cbt.request.params["PeerId"]
        olid = cbt.request.params["OverlayId"]
        for peerid in peer_list:
            param = {}
            param["OverlayId"] = olid
            param["LinkId"] = self._links[olid]["Peers"][peerid]
            param["IccType"] = "DATA"

            lcbt = self.create_linked_cbt(pcbt)
            lcbt.set_request("TincanInterface", "TCI_ICC", param)
            self.submit_cbt(lcbt)

            #TODO: As these CBTs won't get any response, should they be completed  ??
            cbt.set_response(data="Data Sent", status=True)
            self.complete_cbt(cbt)

    def send_icc_remote_action(self,cbt):
        param = {}
        olid = cbt.request.params["OverlayId"]
        peerid = cbt.request.params["PeerId"]
        param["OverlayId"] = olid
        param["LinkId"] = self._links[olid]["Peers"][peerid]
        param["IccType"] = "ACTION"
        
        lcbt = self.create_linked_cbt(cbt)
        lcbt.set_request("TincanInterface", "TCI_ICC", param)
        self.submit_cbt(lcbt)


    def recieve_icc_data(self,cbt):
        if cbt.request.params["IccType"] == "DATA":
            pcbt = self.get_parent_cbt(cbt)
            target_module_name = pcbt.request.params["ModuleName"]
            msg = pcbt.reuqest.params["Data"]

            self.register_cbt(target_module_name, "ICC_DELIVER_DATA", msg)

            #TODO: The CBTs from TCI to the receiver ICC for "DATA", should they be completed  ??
            cbt.set_response(data="Data Delivered", status=True)
            self.complete_cbt(cbt)

        elif cbt.request.params["IccType"] == "ACTION":
            pcbt = self.get_parent_cbt(cbt)
            target_module_name = pcbt.request.params["ModuleName"]
            remote_action_code = pcbt.request.params["ActionCode"]
            params = pcbt.reuqest.params["Params"]

            self.register_cbt(target_module_name, remote_action_code, params)

    def complete_remote_response(self,cbt):
        rcbt = self._cfx_handle._pending_cbts[cbt.tag]
        resp_data = cbt.response.data
        rcbt.set_response(data=resp_data, status=True)
        self.complete_cbt(rcbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "LNK_DATA_UPDATES":
                self.update_links(cbt)
                
            elif cbt.request.action == "ICC_SEND_DATA":
                self.send_icc_data(cbt)

            elif cbt.request.action == "ICC_BROADCAST_DATA":
                self.broadcast_icc_data(cbt)

            elif cbt.request.action == "ICC_REMOTE_ACTION":
                self.send_icc_remote_action(cbt)

            elif cbt.request.action == "ICC_RECIEVE":
                self.recieve_icc_data(cbt)

        elif cbt.op_type == "Response":
            if cbt.request.action == "ICC_REMOTE_RESPONSE":
                self.complete_remote_response(cbt)

            elif cbt.request.action == "ICC_RECIEVE":
                self.complete_remote_response(cbt)

            self.free_cbt(cbt)


    def terminate(self):
        pass

    def timer_method(self):
        pass