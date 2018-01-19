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
from controller.framework.CFx import CFX
import time
import math
import uuid


class Topology(ControllerModule, CFX):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Topology, self).__init__(cfx_handle, module_config, module_name)
        self._overlays = {}

    def initialize(self):
        self._sub_presence = self._cfx_handle.start_subscription("Signal", "SIG_PEER_PRESENCE_NOTIFY")
        for olid in self._cm_config["Overlays"]:
            param = {
                "StunAddress": self._cm_config["Stun"][0],
                "TurnAddress": self._cm_config["Turn"][0]["Address"],
                "TurnPass": self._cm_config["Turn"][0]["Password"],
                "TurnUser": self._cm_config["Turn"][0]["User"],
                "Type": self._cm_config["Overlays"][olid]["Type"],
                "EnableIPMapping": self._cm_config["Overlays"][olid].get("EnableIPMapping", False),
                "TapName": self._cm_config["Overlays"][olid]["TapName"],
                "IP4": self._cm_config["Overlays"][olid]["IP4"],
                "MTU4": self._cm_config["Overlays"][olid]["MTU4"],
                "PrefixLen4": self._cm_config["Overlays"][olid]["IP4PrefixLen"],
                "OverlayId": olid
            }
            self.register_cbt("TincanInterface", "TCI_CREATE_OVERLAY", param)
            self._overlays[olid] = dict(Peers=set())
        self.register_cbt("Logger", "LOG_INFO", "{0} Module loaded".format(self._module_name))

    def terminate(self):
        pass

    def connect_to_peer(sef, overlay_id, peer_id):
        #only send the create link request is we have overlay info from tincan
        #this is where we get the local fingerprint/mac and more which necessary
        #for creating the link
        if (self._overlays[overlay_id].get("Descriptor") is not None
            and self._overlays[overlay_id]["Peers"][peer_id] != "PeerStateConnected"):
            local_descr = {
                "OverlayId": overlay_id,
                "PeerId": peer_id,
                "EncryptionEnabled": self._cm_config["Overlays"][olid].get("EncryptionEnabled", True),
                "NodeData": self._overlays[overlay_id]["Descriptor"]
                }
            self.register_cbt("LinkManger", "LNK_CREATE_LINK", peer_descr)
        
    def update_overlay_info(self, cbt):
        if cbt.response.status:
            self._overlays[cbt.request.params["OverlayId"]] = {
                "Descriptor": cbt.response.data
            }
        else:
            self.register_cbt("Logger", "LOG_WARNING",
                "Query overlay info failed {0}".format(cbt.response.data))

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "SIG_PEER_PRESENCE_NOTIFY":
                peer = cbt.request.param
                if self._overlays.get(peer.overlay_id) is not None:
                    #if self._overlays[peer.overlay_id].get("Peers") is None:
                    #    self._overlays[peer.overlay_id]["Peers"] =  set()
                    self._overlays[peer.overlay_id]["Peers"].add(peer_id)
                cbt.set_response(None, True)
                self.complete_cbt(cbt)
        elif cbt.op_type == "Response":
            if cbt.request.action == "TCI_QUERY_OVERLAY_INFO":
                self.update_overlay_info(cbt)
            self.free_cbt(cbt)
        pass

    def timer_method(self):
        try:
            for overlay_id in self._overlays:
                self.register_cbt("TincanInterface", "TCI_QUERY_OVERLAY_INFO", {"OverlayId": overlay_id})
                for peer_id in self._overlays[overlay_id]["Peers"]:
                    self.connect_to_peer(overlay_id, peer_id)

        except Exception as err:
            self.register_cbt("Logger", "LOG_ERROR", "Exception in BTM timer:" + str(err))
