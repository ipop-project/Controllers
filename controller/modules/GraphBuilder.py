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
import numpy.random
from controller.modules.NetworkGraph import ConnectionEdge
from controller.modules.NetworkGraph import ConnEdgeAdjacenctList

class GraphBuilder(object):
    """
    Creates the adjacency list of connections edges from this node that are necessary to
    maintain the Topology
    """
    def __init__(self, cfg, current_adj_list=None):
        self.overlay_id = cfg["OverlayId"]
        self._node_id = cfg["NodeId"]
        self._peers = cfg.get("Peers", [])
        # enforced is a list of peer ids that should always have a direct edge
        self._enforced = cfg.get("EnforcedEdges", {})
        # only create edges from the enforced list
        self._manual_topo = cfg.get("ManualTopology", False)
        self._max_successors = int(cfg.get("MaxSuccessors", 1))
        # the number of chord edges that shoulb be maintained
        self._max_ldl_cnt = int(cfg.get("MaxLongDistLinks", 4))
        # Currently active adjacency list, needed to minimize changes in chord selection
        self._curr_adj_lst = current_adj_list
        self._successors = []

    def build_adj_list_ata(self,):
        """
        Generates a new adjacency list from the list of available peers
        """
        adj_list = ConnEdgeAdjacenctList(self.overlay_id, self._node_id)
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

    def _build_successors(self, adj_list):
        successors = self._get_successors()
        for peer_id in successors:
            #exclude if peer was previously added as another edge type
            if peer_id not in adj_list.conn_edges:
                ce = ConnectionEdge(peer_id, "CETypeSuccessor")
                adj_list.add_connection_edge(ce)

    def _build_long_dist_links(self, adj_list, transition_adj_list):
        # Add potential long distance link candidates to the adjacency list up to the difference
        # of the max link and existing links
        ldl = self._get_long_dist_links()
        existing_ldlnk_cnt = transition_adj_list.edge_type_count("CETypeLongDistance")
        for peer_id in ldl:
            if self._max_ldl_cnt - existing_ldlnk_cnt > 0:
                if peer_id not in adj_list.conn_edges:
                    ce = ConnectionEdge(peer_id, "CETypeLongDistance")
                    adj_list.add_connection_edge(ce)
                    existing_ldlnk_cnt = existing_ldlnk_cnt + 1
            else:
                return

    def _build_enforced(self, adj_list):
        for peer_id in self._enforced:
            ce = ConnectionEdge(peer_id, "CETypeEnforced")
            adj_list.add_connection_edge(ce)

    def _get_successors(self):
        if len(self._peers) == 1 and self._node_id > self._peers[0]:
            return []
        node_set = set(self._peers)
        node_set.add(self._node_id)
        node_set = sorted(node_set)
        bgn = node_set.index(self._node_id) + 1
        if bgn == len(node_set):
            bgn = 0
        end = bgn + self._max_successors
        self._successors = node_set[bgn:end]
        return node_set[bgn:end]

    def _get_long_dist_links(self):
        # Calculates long distance link candidates.
        long_dist_links = []
        all_nodes = sorted(self._peers + [self._node_id])
        network_sz = len(all_nodes)
        my_index = all_nodes.index(self._node_id)
        num_peers = len(self._peers)
        if num_peers - 2 < self._max_ldl_cnt:
            return long_dist_links
        num_ldl = self._max_ldl_cnt + 2
        node_off = self.symphony_prob_distribution(network_sz, num_ldl)
        for i in node_off:
            idx = math.floor(network_sz*i)
            ldl_idx = (my_index + idx)%network_sz
            long_dist_links.append(all_nodes[ldl_idx])
        return long_dist_links

    def build_adj_list(self, transition_adj_list):
        adj_list = ConnEdgeAdjacenctList(self.overlay_id, self._node_id)
        self._build_enforced(adj_list)
        if not self._manual_topo:
            self._build_successors(adj_list)
            self._build_long_dist_links(adj_list, transition_adj_list)
        return adj_list

    def symphony_prob_distribution(self, network_sz, samples):
        """exp (log(n) * (rand() - 1.0))"""
        results = [None]*(samples)
        dist = numpy.random.uniform(size=samples)
        for i in range(0, samples):
            results[i] = math.exp(math.log10(network_sz) * (dist[i] - 1.0))
        return results
