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
import random
import threading
import time
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
        self._cfx_handle.start_subscription("LinkManager", "LNK_TUNNEL_EVENTS")
        nid = self._cm_config["NodeId"]
        for olid in self._cfx_handle.query_param("Overlays"):
            self._overlays[olid] = dict(NetBuilder=NetworkBuilder(self, olid, nid), KnownPeers=[],
                                        NewPeerCount=0, Blacklist=dict())
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
        pass

    def resp_handler_create_tnl(self, cbt):
        params = cbt.request.params
        olid = params["OverlayId"]
        peer_id = params["PeerId"]
        if not cbt.response.status:
            self.register_cbt("Logger", "LOG_WARNING", "Failed to create topology edge to {0}. {1}"
                              .format(cbt.request.params["PeerId"], cbt.response.data))
            interval = self._cm_config["TimerInterval"]
            self._overlays[olid]["Blacklist"][peer_id] = \
                {"RemovalTime": (random.randint(0, 5) * interval) + time.time()}
        self.free_cbt(cbt)

    def resp_handler_remove_tnl(self, cbt):
        if not cbt.response.status:
            self.register_cbt("Logger", "LOG_WARNING",
                              "Failed to remove topology edge {0}".format(cbt.response.data))
            params = cbt.request.params
            params["UpdateType"] = "RemoveEdgeFailed"
            params["LinkId"] = None
            olid = params["OverlayId"]
            self._overlays[olid]["NetBuilder"].on_connection_update(params)
        self.free_cbt(cbt)

    def req_handler_peer_presence(self, cbt):
        """
        Handles peer presence notification. Determines when to build a new graph and refresh
        connections.
        """
        peer = cbt.request.params
        peer_id = peer["PeerId"]
        olid = peer["OverlayId"]
        with self._lock:
            if peer_id not in self._overlays[olid]["KnownPeers"]:
                self._overlays[olid]["KnownPeers"].append(peer_id)
                self._overlays[olid]["NewPeerCount"] += 1
                nb = self._overlays[olid]["NetBuilder"]
                if (nb.is_ready() and self._overlays[olid]["NewPeerCount"]
                        >= self._cm_config["PeerDiscoveryCoalesce"]):
                    self.register_cbt("Logger", "LOG_DEBUG", "Coalesced {0} new peer discovery, "
                                      "initiating network refresh"
                                      .format(self._overlays[olid]["NewPeerCount"]))
                    enf_lnks = self._cm_config["Overlays"][olid].get("EnforcedLinks", {})
                    peer_list = [item for item in self._overlays[olid]["KnownPeers"] \
                        if item not in self._overlays[olid]["Blacklist"]]
                    manual_topo = self._cm_config["Overlays"][olid].get("ManualTopology", False)
                    params = {"OverlayId": olid, "NodeId": self._cm_config["NodeId"],
                              "Peers": peer_list,
                              "EnforcedEdges": enf_lnks, "MaxSuccessors": 1, "MaxLongDistLinks": 4,
                              "ManualTopology": manual_topo}
                    gb = GraphBuilder(params)
                    adjl = gb.build_adj_list(nb.get_adj_list())
                    nb.refresh(adjl)
                    self._overlays[olid]["NewPeerCount"] = 0
                else:
                    self.register_cbt("Logger", "LOG_DEBUG", "{0} new peers discovered, delaying "
                                      "refresh".format(self._overlays[olid]["NewPeerCount"]))
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
                edges = {}
                for olid in self._overlays:
                    nb = self._overlays[olid]["NetBuilder"]
                    if nb:
                        adjl = nb.get_adj_list()
                        for k in adjl.conn_edges:
                            ce = adjl.conn_edges[k]
                            ced = {"PeerId": ce.peer_id, "LinkId": ce.link_id,
                                   "MarkedForDeleted": ce.marked_for_delete,
                                   "CreatedTime": ce.created_time,
                                   "ConnectedTime": ce.connected_time,
                                   "State": ce.state, "Type": ce.edge_type}
                            edges[ce.link_id] = ced
                        topo_data[olid] = edges
            cbt.set_response({"Topology": topo_data}, bool(topo_data))
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
                self.top_log("Removing peer id from peer list {0}".format(peer_id))
                i = self._overlays[olid]["KnownPeers"].index(peer_id)
                self._overlays[olid]["KnownPeers"].pop(i)
            self._overlays[olid]["NetBuilder"].on_connection_update(params)
        cbt.set_response(None, True)
        self.complete_cbt(cbt)

    def request_handler_tunnel_req(self, cbt):
        cbt.set_response("Accept", True)
        self.complete_cbt(cbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "SIG_PEER_PRESENCE_NOTIFY":
                self.req_handler_peer_presence(cbt)
            elif cbt.request.action == "VIS_DATA_REQ":
                self.req_handler_vis_data(cbt)
            elif cbt.request.action == "TOP_QUERY_PEER_IDS":
                self.req_handler_query_peer_ids(cbt)
            elif cbt.request.action == "LNK_TUNNEL_EVENTS":
                self.req_handler_link_data_update(cbt)
            elif cbt.request.action == "TOP_INCOMING_TUNNEL_REQ":
                self.request_handler_tunnel_req(cbt)
            else:
                self.req_handler_default(cbt)
        elif cbt.op_type == "Response":
            if cbt.request.action == "LNK_CREATE_TUNNEL":
                self.resp_handler_create_tnl(cbt)
            elif cbt.request.action == "LNK_REMOVE_TUNNEL":
                self.resp_handler_remove_tnl(cbt)
            else:
                parent_cbt = cbt.parent
                cbt_data = cbt.response.data
                cbt_status = cbt.response.status
                self.free_cbt(cbt)
                if (parent_cbt is not None and parent_cbt.child_count == 1):
                    parent_cbt.set_response(cbt_data, cbt_status)
                    self.complete_cbt(parent_cbt)

    def _cleanup_blacklist(self):
        # Remove peers from the duration based blacklist. Higher successive connection failures
        # resuts in potentially longer duration in the blacklist.
        tmp = []
        for olid in self._overlays:
            for peer_id in self._overlays[olid]["Blacklist"]:
                rt = self._overlays[olid]["Blacklist"][peer_id]["RemovalTime"]
                if rt >= time.time():
                    tmp.append(peer_id)
            for peer_id in tmp:
                self._overlays[olid]["Blacklist"].pop(peer_id, None)
                self.register_cbt("Logger", "LOG_INFO",
                                  "Node {0} removed from blacklist".format(peer_id[:7]))

    def manage_topology(self):
        # Periodically refresh the topology, making sure desired links exist and exipred ones are
        # removed.
        with self._lock:
            self._cleanup_blacklist()
            for olid in self._overlays:
                nb = self._overlays[olid]["NetBuilder"]
                if nb.is_ready():
                    self.register_cbt("Logger", "LOG_DEBUG", "Refreshing topology...")
                    enf_lnks = self._cm_config["Overlays"][olid].get("EnforcedLinks", {})
                    manual_topo = self._cm_config["Overlays"][olid].get("ManualTopology", False)
                    params = {"OverlayId": olid, "NodeId": self._cm_config["NodeId"],
                              "Peers": self._overlays[olid]["KnownPeers"],
                              "EnforcedEdges": enf_lnks, "MaxSuccessors": 1, "MaxLongDistLinks": 4,
                              "ManualTopology": manual_topo}
                    gb = GraphBuilder(params)
                    adjl = gb.build_adj_list(nb.get_adj_list())
                    nb.refresh(adjl)
                    self._overlays[olid]["NewPeerCount"] = 0
                else:
                    self.register_cbt("Logger", "LOG_DEBUG", "Net builder busy, skipping...")

    def timer_method(self):
        self.manage_topology()

    def top_add_edge(self, overlay_id, peer_id):
        """
        Start the connection process to a peer if a direct edge is desirable
        """
        self.register_cbt("Logger", "LOG_INFO", "Creating peer edge {0}:{1}->{2}"
                          .format(overlay_id, self._cm_config["NodeId"][:7], peer_id[:7]))
        params = {"OverlayId": overlay_id, "PeerId": peer_id}
        self.register_cbt("LinkManager", "LNK_CREATE_TUNNEL", params)

    def top_remove_edge(self, overlay_id, peer_id):
        self.register_cbt("Logger", "LOG_INFO", "Removing peer edge {0}:{1}->{2}"
                          .format(overlay_id, self._cm_config["NodeId"][:7], peer_id[:7]))
        params = {"OverlayId": overlay_id, "PeerId": peer_id}
        self.register_cbt("LinkManager", "LNK_REMOVE_TUNNEL", params)

    def top_log(self, msg, level="LOG_DEBUG"):
        self.register_cbt("Logger", level, msg)
