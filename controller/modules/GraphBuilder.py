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
from controller.modules.NetworkGraph import ConnectionEdge
from controller.modules.NetworkGraph import ConnEdgeAdjacenctList

class GraphBuilder(object):
    """
    Creates the adjacency list of connections edges from this node that are necessary to
    maintain the Topology
    """
    def __init__(self, overlay_id, node_id, peers, enforced=None, manual_topo=False):
        self.overlay_id = overlay_id
        self._node_id = node_id          # local node id
        self._peers = peers              # list of peer ids
        self._enforced = enforced        # list of peer ids that should always have a direct edge
        self._manual_topo = manual_topo  # only create edges from the enforced list

    def build_adj_list(self,):
        """
        Generates a new adjacency list from the list of available peers
        """
        adj_list = ConnEdgeAdjacenctList(self.overlay_id, self._node_id)
        for peer_id in self._peers:
            if self._enforced and peer_id in self._enforced:
                ce = ConnectionEdge(peer_id)
                ce.type = "CETypeEnforced"
                adj_list.conn_edges[peer_id] = ce
            elif not self._manual_topo and self._node_id < peer_id:
                ce = ConnectionEdge(peer_id)
                ce.type = "CETypeSuccessor"
                adj_list.conn_edges[peer_id] = ce
        return adj_list
