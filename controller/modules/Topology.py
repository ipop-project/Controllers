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
import threading
from controller.framework.CFx import CFX
from controller.framework.ControllerModule import ControllerModule
from controller.modules.NetworkBuilder import NetworkBuilder
from  controller.modules.GraphBuilder import GraphBuilder

class Topology(ControllerModule, CFX):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Topology, self).__init__(cfx_handle, module_config, module_name)
        self._overlays = {}
        self._lock = threading.Lock()

    def initialize(self):
        self._cfx_handle.start_subscription("Signal",
                                            "SIG_PEER_PRESENCE_NOTIFY")
        self._cfx_handle.start_subscription("LinkManager", "LNK_DATA_UPDATES")
        nid = self._cm_config["NodeId"]
        for olid in self._cfx_handle.query_param("Overlays"):
            self._overlays[olid] = dict(NetBuilder=NetworkBuilder(self, olid, nid), KnownPeers=[],
                                        NewPeer=False)
        try:
            # Subscribe for data request notifications from OverlayVisualizer
            self._cfx_handle.start_subscription("OverlayVisualizer",
                                                "VIS_DATA_REQ")
        except NameError as err:
            if "OverlayVisualizer" in str(err):
                self.register_cbt("Logger", "LOG_WARNING",
                                  "OverlayVisualizer module not loaded."
                                  " Visualization data will not be sent.")
        self.register_cbt("Logger", "LOG_INFO", "Module loaded")

    def terminate(self):
        """
        Send goodbye ICC to known peers.
        """
        pass

    def resp_handler_create_link(self, cbt):
        if not cbt.response.status:
            self.register_cbt("Logger", "LOG_WARNING",
                              "Failed to create topology edge {0}".format(cbt.response.data))
        self.free_cbt(cbt)

    def resp_handler_remove_link(self, cbt):
        if not cbt.response.status:
            self.register_cbt("Logger", "LOG_WARNING",
                              "Failed to remove topology edge {0}".format(cbt.response.data))
        self.free_cbt(cbt)

    def req_handler_peer_presence(self, cbt):
        """
        Handles peer presence notification. Determines when to build a new graph and refresh
        connections.
        """
        peer = cbt.request.params
        peer_id = peer["PeerId"]
        overlay_id = peer["OverlayId"]
        with self._lock:
            nb = self._overlays[overlay_id]["NetBuilder"]
            if peer_id not in self._overlays[overlay_id]["KnownPeers"]:
                self._overlays[overlay_id]["NewPeer"] = True
                self._overlays[overlay_id]["KnownPeers"].append(peer_id)
                if nb.is_ready():
                    gb = GraphBuilder(overlay_id, self._cm_config["NodeId"],
                                      self._overlays[overlay_id]["KnownPeers"],
                                      self._cm_config["Overlays"][overlay_id].get("EnforcedLinks"),
                                      self._cm_config["Overlays"][overlay_id].get("ManualTopology"))
                    adjl = gb.build_adj_list()
                    self._overlays[overlay_id]["NewPeer"] = False
                    nb.refresh(adjl)
        cbt.set_response(None, True)
        self.complete_cbt(cbt)

    def req_handler_query_peer_ids(self, cbt):
        peer_ids = {}
        try:
            with self._lock:
                for olid in self._cm_config["Overlays"]:
                    peer_ids[olid] = set(self._overlays[olid]["KnownPeers"])
                cbt.set_response(data=peer_ids, status=True)
                self.complete_cbt(cbt)
        except KeyError:
            cbt.set_response(data=None, status=False)
            self.complete_cbt(cbt)
            self.register_cbt("Logger", "LOG_WARNING", "Overlay Id is not valid {0}".
                              format(cbt.response.data))

    def req_handler_vis_data(self, cbt):
        topo_data = {}
        try:
            with self._lock:
                for olid in self._overlays:
                    ks = [peer_id for peer_id in self._overlays[olid]["KnownPeers"]]
                    if ks:
                        topo_data[olid] = ks

                cbt.set_response({"Topology": topo_data},
                                 True if topo_data else False)
                self.complete_cbt(cbt)
        except KeyError:
            cbt.set_response(data=None, status=False)
            self.complete_cbt(cbt)
            self.register_cbt("Logger", "LOG_WARNING", "Topology data not available {0}".
                              format(cbt.response.data))

    def req_handler_link_data_update(self, cbt):
        params = cbt.request.params
        olid = params["OverlayId"]
        peer_id = params["PeerId"]
        with self._lock:
            if params["UpdateType"] == "REMOVED":
                i = self._overlays[olid]["KnownPeers"].index(peer_id)
                self._overlays[olid]["KnownPeers"].pop(i)
            self._overlays[olid]["NetBuilder"].on_connection_update(params)
        cbt.set_response(None, True)
        self.complete_cbt(cbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "SIG_PEER_PRESENCE_NOTIFY":
                self.req_handler_peer_presence(cbt)
            elif cbt.request.action == "VIS_DATA_REQ":
                self.req_handler_vis_data(cbt)
            elif cbt.request.action == "TOP_QUERY_PEER_IDS":
                self.req_handler_query_peer_ids(cbt)
            elif cbt.request.action == "LNK_DATA_UPDATES":
                self.req_handler_link_data_update(cbt)
            else:
                self.req_handler_default(cbt)
        elif cbt.op_type == "Response":
            if cbt.request.action == "LNK_CREATE_LINK":
                self.resp_handler_create_link(cbt)
            elif cbt.request.action == "LNK_REMOVE_LINK":
                self.resp_handler_remove_link(cbt)

    def manage_topology(self):
        # Periodically refresh the topology, making sure desired links exist and exipred ones are
        # removed.
        with self._lock:
            for olid in self._overlays:
                if (self._overlays[olid]["NewPeer"] and
                        self._overlays[olid]["NetBuilder"].is_ready()):
                    gb = GraphBuilder(olid, self._cm_config["NodeId"],
                                      self._overlays[olid]["KnownPeers"],
                                      self._cm_config["Overlays"][olid].get("EnforcedLinks"),
                                      self._cm_config["Overlays"][olid].get("ManualTopology"))
                    adjl = gb.build_adj_list()
                    self._overlays[olid]["NewPeer"] = False
                    self._overlays[olid]["NetBuilder"].refresh(adjl)
                elif not self._overlays[olid]["NetBuilder"].is_ready():
                    self.register_cbt("Logger", "LOG_INFO", "A network refresh operation is "
                                      " already in progress, skipping ...")

    def timer_method(self):
        self.manage_topology()

    def top_add_edge(self, overlay_id, peer_id):
        """
        Start the connection process to a peer if a direct edge is desirable
        """
        self.register_cbt("Logger", "LOG_INFO", "Adding peer edge {0}:{1}->{2}"
                          .format(overlay_id, self._cm_config["NodeId"][:7], peer_id[:7]))
        params = {"OverlayId": overlay_id, "PeerId": peer_id}
        self.register_cbt("LinkManager", "LNK_CREATE_LINK", params)

    def top_remove_edge(self, overlay_id, peer_id):
        self.register_cbt("Logger", "LOG_INFO", "Removing peer edge {0}:{1}->{2}"
                          .format(overlay_id, self._cm_config["NodeId"][:7], peer_id[:7]))
        params = {"OverlayId": overlay_id, "PeerId": peer_id}
        self.register_cbt("LinkManager", "LNK_REMOVE_LINK", params)

    def top_log(self, msg, level="LOG_DEBUG"):
        self.register_cbt("Logger", level, msg)





    #def req_handler_broadcast_frame(self, cbt):
    #    if cbt.request.params["Command"] == "ReqRouteUpdate":
    #        eth_frame = cbt.request.params["Data"]
    #        tgt_mac_id = eth_frame[:12]
    #        if tgt_mac_id == "FFFFFFFFFFFF":
    #            arp_broadcast_req = {
    #                "overlay_id": cbt.request.params["OverlayId"],
    #                "tgt_module": "TincanInterface",
    #                "action": "TCI_INJECT_FRAME",
    #                "payload": eth_frame
    #            }
    #            self.register_cbt("Broadcaster", "BDC_BROADCAST",
    #                              arp_broadcast_req)
    #        cbt.set_response(data=None, status=True)
    #    else:
    #        cbt.set_response(data=None, status=False)
    #    self.complete_cbt(cbt)
