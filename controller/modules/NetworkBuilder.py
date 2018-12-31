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
from copy import deepcopy
from controller.modules.NetworkGraph import ConnectionEdge
from controller.modules.NetworkGraph import ConnEdgeAdjacenctList

class NetworkBuilder(object):
    """description of class"""
    def __init__(self, top_man, overlay_id, node_id):
        self._current_adj_list = ConnEdgeAdjacenctList(overlay_id, node_id)
        self._refresh_in_progress = 0
        self._pending_adj_list = None
        self._lock = threading.Lock()
        self._top = top_man

    def is_ready(self):
        with self._lock:
            return self._refresh_in_progress == 0

    def get_adj_list(self):
        with self._lock:
            return deepcopy(self._current_adj_list)

    def refresh(self, net_graph=None):
        """
        Updates the networks connections. Invoked on different threads: 1) Periodically without
        parameters to last to last provided network graph, 2) attempt to refresh now or schedule
        the provide graph for refresh.
        """
        with self._lock:
            self._top.top_log("New net graph:{0}\nCurrent adj list:{1}"
                              .format(net_graph, self._current_adj_list))
            if self._pending_adj_list:
                self._top.top_log("Pending adj list:{0}"
                                  .format(self._pending_adj_list))
            if self._refresh_in_progress < 0:
                raise ValueError("A precondition violation occurred. The refresh reference count"
                                 " is negative {}".format(self._refresh_in_progress))
            """
            This conditon is expected to be met on the timer invocation when no net_graph is
            supplied but when there is _pending_edges waiting to be applied.
            """
            if self._refresh_in_progress == 0 and not net_graph and self._pending_adj_list:
                self._update_net_connections()
                return
            """
            Overwrite any previous pending_edges as we are only interested in the most recent one.
            """
            if net_graph:
                self._pending_adj_list = net_graph
            """
            To minimize network disruption wait until a previous sync operation is completed before
            starting a new one.
            """
            if self._refresh_in_progress > 0:
                return
            """
            Attempt to sync the network state to the pending net graph.
            """
            if self._pending_adj_list:
                self._update_net_connections()

    def on_connection_update(self, connection_event):
        """
        Updates the connection edge's current state based on the provided event. This is the
        completion for a create or remove connection request to Link Manager.
        """
        peer_id = connection_event["PeerId"]
        link_id = connection_event["LinkId"]
        overlay_id = connection_event["OverlayId"]
        with self._lock:
            if connection_event["UpdateType"] == "CREATING":
                conn_edge = self._current_adj_list.conn_edges.get(peer_id, None)
                if not conn_edge:
                    # this happens when the neighboring peer initiates the connection bootstrap
                    self._refresh_in_progress += 1
                    conn_edge = ConnectionEdge(peer_id, "CETypePredecessor")
                    self._current_adj_list.conn_edges[peer_id] = conn_edge
                conn_edge.state = "CEStateCreated"
                conn_edge.link_id = link_id
            elif connection_event["UpdateType"] == "REMOVED":
                self._current_adj_list.conn_edges.pop(peer_id, None)
                self._refresh_in_progress -= 1
            elif connection_event["UpdateType"] == "CONNECTED":
                self._current_adj_list.conn_edges[peer_id].state = "CEStateConnected"
                self._current_adj_list.conn_edges[peer_id].connected_time = \
                    connection_event["ConnectedTimestamp"]
                self._refresh_in_progress -= 1
            elif connection_event["UpdateType"] == "DISCONNECTED":
                # the local topology did not request removal of the connection
                self._top.top_log("CEStateDisconnected event recvd peer_id: {0}, link_id: {1}".
                                  format(peer_id, link_id))
                self._current_adj_list.conn_edges[peer_id].state = "CEStateDisconnected"
                self._refresh_in_progress += 1
                self._top.top_remove_edge(overlay_id, peer_id)
            elif connection_event["UpdateType"] == "AddEdgeFailed":
                self._current_adj_list.conn_edges.pop(peer_id, None)
                self._refresh_in_progress -= 1
            elif connection_event["UpdateType"] == "RemoveEdgeFailed":
                # leave the node in the adj list and marked for removal to be retried.
                self._refresh_in_progress -= 1
            else:
                self._top.top_log("Logger", "LOG_WARNING",
                                  "Invalid UpdateType specified for connection update")

    def _update_net_connections(self):
        """
        Sync the network state by determining the difference between the active and pending net
        graphs.
        """
        if self._current_adj_list.overlay_id != self._pending_adj_list.overlay_id:
            raise ValueError("Overlay ID mismatch adj lists, active:{0}, pending:{1}".
                             format(self._current_adj_list.overlay_id,
                                    self._pending_adj_list.overlay_id))
        # Anything in the set (Active - Pending) is marked for deletion
        overlay_id = self._current_adj_list.overlay_id
        for peer_id in self._current_adj_list.conn_edges:
            if (peer_id not in self._pending_adj_list.conn_edges and
                    self._current_adj_list.conn_edges[peer_id].edge_type != "CETypePredecessor"):
                self._current_adj_list.conn_edges[peer_id].marked_for_delete = True
        # Any edge in set (Pending - Active) is created and added to Active
        for peer_id in self._pending_adj_list.conn_edges:
            if not peer_id in self._current_adj_list.conn_edges:
                self._current_adj_list.conn_edges[peer_id] = \
                    self._pending_adj_list.conn_edges[peer_id]
                if self._current_adj_list.conn_edges[peer_id].state == "CEStateUnknown":
                    self._refresh_in_progress += 1
                    self._top.top_add_edge(overlay_id, peer_id)
            else:
                # Existing edges in both Active and Pending are updated in place
                self._current_adj_list.conn_edges[peer_id].marked_for_delete = \
                   self._pending_adj_list.conn_edges[peer_id].marked_for_delete
        self._pending_adj_list = None
        # Minimize churn by removing a single connection per refresh
        for peer_id in self._current_adj_list.conn_edges:
            if self._current_adj_list.conn_edges[peer_id].marked_for_delete:
                self._refresh_in_progress += 1
                self._top.top_remove_edge(overlay_id, peer_id)
                return
