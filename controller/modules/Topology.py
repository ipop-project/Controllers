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
import json


class Topology(ControllerModule, CFX):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Topology, self).__init__(cfx_handle, module_config, module_name)
        self._overlays = {}

    def initialize(self):
        self._cfx_handle.start_subscription("Signal",
                 "SIG_PEER_PRESENCE_NOTIFY")
        for olid in self._cm_config["Overlays"]:
            self._overlays[olid] = (
                dict(Descriptor = dict(IsReady=False, State="Bootstrapping"),
                    Peers=set()))
            self.create_overlay(self._cm_config["Overlays"][olid], olid)
        try:
            # Subscribe for data request notifications from OverlayVisualizer
            self._cfx_handle.start_subscription("OverlayVisualizer",
                    "VIS_DATA_REQ")
        except NameError as err:
            if "OverlayVisualizer" in str(err):
                self.register_cbt("Logger", "LOG_WARNING",
                        "OverlayVisualizer module not loaded." \
                            " Visualization data will not be sent.")

        self.register_cbt("Logger", "LOG_INFO", "{0} Module loaded"
                          .format(self._module_name))

    def terminate(self):
        pass

    def create_overlay(self, overlay_cfg, overlay_id):
        param = {
            "StunAddress": self._cm_config["Stun"][0],
            "TurnAddress": self._cm_config["Turn"][0]["Address"],
            "TurnPass": self._cm_config["Turn"][0]["Password"],
            "TurnUser": self._cm_config["Turn"][0]["User"],
            "Type": overlay_cfg["Type"],
            "EnableIPMapping": overlay_cfg.get("EnableIPMapping", False),
            "TapName": overlay_cfg["TapName"],
            "IP4": overlay_cfg["IP4"],
            "MTU4": overlay_cfg["MTU4"],
            "PrefixLen4": overlay_cfg["IP4PrefixLen"],
            "OverlayId": overlay_id
        }
        self.register_cbt("TincanInterface", "TCI_CREATE_OVERLAY", param)

    def connect_to_peer(self, overlay_id, peer_id):
        #only send the create link request is we have overlay info from tincan
        #this is where we get the local fingerprint/mac and more which necessary
        #for creating the link
        if (self._overlays[overlay_id]["Descriptor"]["IsReady"]
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
            olid = cbt.request.params["OverlayId"]
            self._overlays[olid]["Descriptor"]["IsReady"] = cbt.response.status
            cbt_data = json.loads(cbt.response.data)
            self._overlays[olid]["Descriptor"]["MAC"] = cbt_data["MAC"]

            self._overlays[olid]["Descriptor"]["PrefixLen"] = cbt_data["IP4PrefixLen"]
            self._overlays[olid]["Descriptor"]["VIP4"] = cbt_data["VIP4"]
            self._overlays[olid]["Descriptor"]["TapName"] = cbt_data["TapName"]
            self._overlays[olid]["Descriptor"]["Fingerprint"] = cbt_data["Fingerprint"]
        else:
            self.register_cbt("Logger", "LOG_WARNING",
                "Query overlay info failed {0}".format(cbt.response.data))
            #retry the query
            self.register_cbt("TincanInterface", "TCI_QUERY_OVERLAY_INFO", cbt.request.params)

    def create_overlay_resp_handler(self, cbt):
        if cbt.response.status == True:
            self.register_cbt("TincanInterface", "TCI_QUERY_OVERLAY_INFO", {"OverlayId": cbt.request.params["OverlayId"]})
        else:
            self.register_cbt("Logger", "LOG_WARNING", cbt.response.data)
            #retry creating the overlay once
            if self._overlays[cbt.request.params["OverlayId"]].get("RetryCount", 0) < 1:
                self._overlays[cbt.request.params["OverlayId"]]["RetryCount"] = 1
                self.register_cbt("TincanInterface", "TCI_CREATE_OVERLAY", cbt.request.params)

    def peer_presence_handler(self, cbt):
        peer = cbt.request.params
        self._overlays[peer["overlay_id"]]["Peers"].add(peer["peer_id"])
        self._overlays[peer["overlay_id"]]["Descriptor"]["State"] = "Isolated"
        self.connect_to_peer(peer["overlay_id"], peer["peer_id"])

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "SIG_PEER_PRESENCE_NOTIFY":
                self.peer_presence_handler(cbt)
                cbt.set_response(None, True)
                self.complete_cbt(cbt)
            elif cbt.request.action == "VIS_DATA_REQ":
                dummy_topo_data = {
                    "test-overlay-id": {
                        "InterfaceName": "ipop_tap0",
                        "GeoIP": "1.2.3.4",
                        "VIP4": "2.3.4.5",
                        "PrefixLen": 16, 
                        "MAC": "FF:FF:FF:FF:FF"
                    }
                }
                vis_data_resp = dict(Topology=dummy_topo_data)

                cbt.set_response(data=vis_data_resp, status=True)
                self.complete_cbt(cbt) 

        elif cbt.op_type == "Response":
            if cbt.request.action == "TCI_CREATE_OVERLAY":
                self.create_overlay_resp_handler(cbt)
            if cbt.request.action == "TCI_QUERY_OVERLAY_INFO":
                self.update_overlay_info(cbt)
            self.free_cbt(cbt)

        pass

    def timer_method(self):
        try:
            #for overlay_id in self._overlays:
            #    for peer_id in self._overlays[overlay_id]["Peers"]:
            #        self.connect_to_peer(overlay_id, peer_id)
            pass
        except Exception as err:
            self.register_cbt("Logger", "LOG_ERROR", "Exception in BTM timer:" + str(err))
