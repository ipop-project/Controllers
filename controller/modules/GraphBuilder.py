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

import math
import random
from controller.modules.NetworkGraph import ConnectionEdge
from controller.modules.NetworkGraph import ConnEdgeAdjacenctList

class GraphBuilder():
    """
    Creates the adjacency list of connections edges from this node that are necessary to
    maintain the Topology
    """
    def __init__(self, cfg, top=None):
        self.overlay_id = cfg["OverlayId"]
        self._node_id = cfg["NodeId"]
        self._peers = None
        # enforced is a list of peer ids that should always have a direct edge
        self._enforced = cfg.get("EnforcedEdges", {})
        # only create edges from the enforced list
        self._manual_topo = cfg.get("ManualTopology", False)
        self._max_successors = int(cfg["MaxSuccessors"])
        # the number of symphony edges that shoulb be maintained
        self._max_ldl_cnt = int(cfg["MaxLongDistEdges"])
        self._max_ond = int(cfg["MaxOnDemandEdges"])
        # Currently active adjacency list, needed to minimize changes in chord selection
        self._nodes = []
        self._my_idx = 0
        self._top = top

    def _build_enforced(self, adj_list):
        for peer_id in self._enforced:
            ce = ConnectionEdge(peer_id, edge_type="CETypeEnforced")
            adj_list.add_connection_edge(ce)

    def _get_successors(self):
        """ Generate a list of successor UIDs from the list of peers """
        #todo: fix for max succ > 1
        successors = []
        num_peers = len(self._peers)
        if not self._peers or (num_peers == 1 and self._node_id > self._peers[0]):
            return successors

        num_nodes = len(self._nodes)
        successor_index = self._my_idx + 1
        num_succ = self._max_successors if (num_peers >= self._max_successors) else num_peers
        for _ in range(num_succ):
            successor_index %= num_nodes
            successors.append(self._nodes[successor_index])
            successor_index += 1
        return successors

    def _build_successors(self, adj_list, transition_adj_list):
        num_ideal_conn_succ = 0
        successors = self._get_successors()
        suc_ces = transition_adj_list.filter([("CETypeSuccessor", "CEStateConnected")])
        # add the ideal successors to the new adj list
        for peer_id in successors:
            if peer_id not in adj_list:
                adj_list[peer_id] = ConnectionEdge(peer_id, edge_type="CETypeSuccessor")
                if peer_id in suc_ces:
                    # this is an ideal succ that was previously connected
                    num_ideal_conn_succ += 1
                    del suc_ces[peer_id]
        # do not remove the existing successor until the new one is connected
        for peer_id in suc_ces:
            # these are to be replaced when the ideal ones are in connected state
            if num_ideal_conn_succ < self._max_successors:
                # not an ideal successor but keep until better succ is connected
                adj_list[peer_id] = ConnectionEdge(peer_id, edge_type="CETypeSuccessor")
                num_ideal_conn_succ += 1
            else:
                break # consider selecting the best of these

    @staticmethod
    def symphony_prob_distribution(network_sz, samples):
        """exp (log(n) * (rand() - 1.0))"""
        results = [None]*(samples)
        for i in range(0, samples):
            rnd_val = random.random()
            results[i] = math.exp(math.log10(network_sz) * (rnd_val - 1.0))
        return results

    def _get_long_dist_links(self, num_ldl):
        # Calculates long distance link candidates.
        long_dist_links = []
        net_sz = len(self._nodes)
        node_off = GraphBuilder.symphony_prob_distribution(net_sz, num_ldl)
        for i in node_off:
            idx = math.floor(net_sz*i)
            ldl_idx = (self._my_idx + idx) % net_sz
            long_dist_links.append(self._nodes[ldl_idx])
        return long_dist_links

    def _build_long_dist_links(self, adj_list, transition_adj_list):
        # Preserve existing long distance
        ldlnks = transition_adj_list.edges_bytype(["CETypeILongDistance"])
        for peer_id, ce in ldlnks.items():
            if ce.edge_state in ("CEStateUnknown", "CEStateCreated", "CEStateConnected") and \
                peer_id not in adj_list:
                adj_list[peer_id] = ConnectionEdge(peer_id, ce.edge_id, ce.edge_type)

        ldlnks = transition_adj_list.edges_bytype(["CETypeLongDistance"])
        num_existing_ldl = 0
        for peer_id, ce in ldlnks.items():
            if ce.edge_state in ("CEStateUnknown", "CEStateCreated", "CEStateConnected") and \
                peer_id not in adj_list and not self.is_too_close(ce.peer_id):
                adj_list[peer_id] = ConnectionEdge(peer_id, ce.edge_id, ce.edge_type)
                num_existing_ldl += 1
        num_ldl = self._max_ldl_cnt - num_existing_ldl
        if num_ldl < 0:
            return
        ldl = self._get_long_dist_links(num_ldl)
        for peer_id in ldl:
            if peer_id not in adj_list:
                ce = ConnectionEdge(peer_id, edge_type="CETypeLongDistance")
                adj_list.add_connection_edge(ce)

    def _build_ondemand_links(self, adj_list, transition_adj_list, request_list):
        ond = {}
        # add existing on demand links
        existing = transition_adj_list.edges_bytype(["CETypeOnDemand", "CETypeIOnDemand"])
        for peer_id, ce in existing.items():
            if ce.edge_state in ("CEStateUnknown", "CEStateCreated", "CEStateConnected") and \
                peer_id not in adj_list:
                ond[peer_id] = ConnectionEdge(peer_id, ce.edge_id, ce.edge_type)
        for task in request_list:
            peer_id = task["PeerId"]
            op = task["Operation"]
            if op == "ADD":
                if peer_id in self._peers and (peer_id not in adj_list or
                                               peer_id not in transition_adj_list):
                    ce = ConnectionEdge(peer_id, edge_type="CETypeOnDemand")
                    ond[peer_id] = ce
            elif op == "REMOVE":
                ond.pop(peer_id, None)
        for peer_id in ond:
            if peer_id not in adj_list:
                adj_list[peer_id] = ond[peer_id]
        request_list.clear()

    def build_adj_list(self, peers, transition_adj_list, request_list=None):
        self._prep(peers)
        adj_list = ConnEdgeAdjacenctList(self.overlay_id, self._node_id,
                                         self._max_successors, self._max_ldl_cnt, self._max_ond)
        self._build_enforced(adj_list)
        if not self._manual_topo:
            self._build_successors(adj_list, transition_adj_list)
            self._build_long_dist_links(adj_list, transition_adj_list)
            self._build_ondemand_links(adj_list, transition_adj_list, request_list)
        for _, ce in adj_list.conn_edges.items():
            assert ce.edge_state == "CEStateUnknown", "Invalid CE edge state, CE={}".format(ce)
        return adj_list

    def build_adj_list_ata(self,):
        """
        Generates a new adjacency list from the list of available peers
        """
        adj_list = ConnEdgeAdjacenctList(self.overlay_id, self._node_id,
                                         self._max_successors, self._max_ldl_cnt, self._max_ond)
        for peer_id in self._peers:
            if self._enforced and peer_id in self._enforced:
                ce = ConnectionEdge(peer_id)
                ce.edge_type = "CETypeEnforced"
                adj_list.add_connection_edge(ce)
            elif not self._manual_topo and self._node_id < peer_id:
                ce = ConnectionEdge(peer_id)
                ce.edge_type = "CETypeSuccessor"
                adj_list.add_connection_edge(ce)
        return adj_list

    def _distance(self, peer_id):
        nsz = len(self._nodes)
        pr_i = self._nodes.index(peer_id)
        return (pr_i + nsz - self._my_idx) % nsz

    def _ideal_closest_distance(self):
        nsz = len(self._nodes)
        off = math.exp(-1 * math.log10(nsz))
        return math.floor(nsz * off)

    def is_too_close(self, peer_id):
        return self._distance(peer_id) < self._ideal_closest_distance()

    def _prep(self, peers):
        self._peers = peers
        self._nodes = list(self._peers)
        self._nodes.append(self._node_id)
        self._nodes.sort()
        self._my_idx = self._nodes.index(self._node_id)
