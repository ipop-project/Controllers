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
import time
try:
    import simplejson as json
except ImportError:
    import json
import struct
import uuid

EdgeTypes1 = ["CETypeUnknown", "CETypeEnforced", "CETypeSuccessor", "CETypeLongDistance",
              "CETypeOnDemand"]
EdgeTypes2 = ["CETypeUnknown", "CETypeIEnforced", "CETypePredecessor", "CETypeILongDistance",
              "CETypeIOnDemand"]
EdgeStates = ["CEStateUnknown", "CEStateCreated", "CEStateConnected", "CEStateDisconnected",
              "CEStateDeleting"]

def transpose_edge_type(edge_type):
    et = EdgeTypes1[0]
    if edge_type == "CETypeEnforced":
        et = EdgeTypes2[1]
    elif edge_type == "CETypeSuccessor":
        et = EdgeTypes2[2]
    elif edge_type == "CETypeLongDistance":
        et = EdgeTypes2[3]
    elif edge_type == "CETypeOnDemand":
        et = EdgeTypes2[4]
    elif edge_type == "CETypeIEnforced":
        et = EdgeTypes1[1]
    elif edge_type == "CETypePredecessor":
        et = EdgeTypes1[2]
    elif edge_type == "CETypeILongDistance":
        et = EdgeTypes1[3]
    elif edge_type == "CETypeIOnDemand":
        et = EdgeTypes1[4]
    return et

class ConnectionEdge():
    """ A discriptor of the edge/link between two peers."""
    _PACK_STR = '!16s16sff18s19s?'
    def __init__(self, peer_id=None, edge_id=None, edge_type="CETypeUnknown"):
        self.peer_id = peer_id
        self.edge_id = edge_id
        if not self.edge_id:
            self.edge_id = uuid.uuid4().hex
        self.created_time = time.time()
        self.connected_time = None
        self.edge_state = "CEStateUnknown"
        self.edge_type = edge_type
        self.marked_for_delete = False

    def __key__(self):
        return int(self.peer_id, 16)

    def __eq__(self, other):
        return self.__key__() == other.__key__()

    def __ne__(self, other):
        return self.__key__() != other.__key__()

    def __lt__(self, other):
        return self.__key__() < other.__key__()

    def __le__(self, other):
        return self.__key__() <= other.__key__()

    def __gt__(self, other):
        return self.__key__() > other.__key__()

    def __ge__(self, other):
        return self.__key__() >= other.__key__()

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        msg = ("ConnectionEdge<peer_id = %s, edge_id = %s, created_time = %s, connected_time = %s,"
               " state = %s, edge_type = %s, marked_for_delete = %s>" %
               (self.peer_id[:7], self.edge_id[:7], str(self.created_time), str(self.connected_time),
                self.edge_state, self.edge_type, self.marked_for_delete))
        #msg = ("ConnectionEdge<peer_id = %s, edge_id = %s, state = %s, edge_type = %s>" %
        #       (self.peer_id, self.edge_id, self.edge_state, self.edge_type))
        return msg

    def __iter__(self):
        yield("peer_id", self.peer_id)
        yield("edge_id", self.edge_id)
        yield("created_time", self.created_time)
        yield("connected_time", self.connected_time)
        yield("edge_state", self.edge_state)
        yield("edge_type", self.edge_type)
        yield("marked_for_delete", self.marked_for_delete)

    def serialize(self):
        return struct.pack(ConnectionEdge._PACK_STR, self.peer_id, self.edge_id, self.created_time,
                           self.connected_time, self.edge_state, self.edge_type,
                           self.marked_for_delete)

    @classmethod
    def from_bytes(cls, data):
        ce = cls()
        (ce.peer_id, ce.edge_id, ce.created_time, ce.connected_time, ce.edge_state,
         ce.edge_type, ce.marked_for_delete) = struct.unpack_from(cls._PACK_STR, data)
        return ce

    def to_json(self):
        return json.dumps(dict(self))

    #def to_json(self):
    #    return json.dumps(dict(peer_id=self.peer_id, edge_id=self.edge_id,
    #                           created_time=self.created_time, connected_time=self.connected_time,
    #                           state=self.edge_state, edge_type=self.edge_type,
    #                           marked_for_delete=self.marked_for_delete))
    @classmethod
    def from_json_str(cls, json_str):
        ce = cls()
        jce = json.loads(json_str)
        ce.peer_id = jce["peer_id"]
        ce.edge_id = jce["edge_id"]
        ce.created_time = jce["created_time"]
        ce.connected_time = jce["connected_time"]
        ce.edge_state = jce["edge_state"]
        ce.edge_type = jce["edge_type"]
        ce.marked_for_delete = jce["marked_for_delete"]
        return ce

class ConnEdgeAdjacenctList():
    """ A series of ConnectionEdges that are incident on the local node"""
    #def __init__(self, overlay_id, node_id, cfg):
    def __init__(self, overlay_id, node_id, max_succ=1, max_ldl=1, max_ond=1):
        self.overlay_id = overlay_id
        self.node_id = node_id
        self.conn_edges = {}
        self._successor_nid = node_id
        self._predecessor_nid = node_id
        self.degree_threshold = (2 * (max_succ + max_ldl)) + max_ond
        self.max_successors = max_succ
        self.max_ldl = max_ldl
        self.max_ondemand = max_ond

    def __len__(self):
        return len(self.conn_edges)

    def __repr__(self):
        msg = "ConnEdgeAdjacenctList<overlay_id = %s, node_id = %s, predecessor_nid=%s, "\
              "successor_nid=%s, num_edges=%d, max_successors=%d, max_ldl=%d, max_ondemand=%d, " \
              "degree_threshold=%d, conn_edges = %s>" % \
              (self.overlay_id[:7], self.node_id[:7], self._predecessor_nid[:7],
               self._successor_nid[:7], len(self.conn_edges), self.max_successors, self.max_ldl,
               self.max_ondemand, self.degree_threshold, self.conn_edges)
        return msg

    def __bool__(self):
        return bool(self.conn_edges)

    def __contains__(self, peer_id):
        if peer_id in self.conn_edges:
            return True
        return False

    def __setitem__(self, peer_id, ce):
        #self.conn_edges[peer_id] = ce
        self.add_connection_edge(ce)

    def __getitem__(self, peer_id):
        return self.conn_edges[peer_id]

    def __delitem__(self, peer_id):
        #del self.conn_edges[peer_id]
        self.remove_connection_edge(peer_id)

    def __iter__(self):
        return self.conn_edges.__iter__()

    def is_successor(self, peer_id):
        return bool(peer_id == self._successor_nid)

    def is_predecessor(self, peer_id):
        return bool(peer_id == self._predecessor_nid)

    def at_threshold(self):
        return bool(len(self.conn_edges) >= self.degree_threshold)

    def add_connection_edge(self, ce):
        self.conn_edges[ce.peer_id] = ce
        self.update_closest()

    def remove_connection_edge(self, peer_id):
        ce = self.conn_edges.pop(peer_id, None)
        if peer_id in (self._successor_nid, self._predecessor_nid):
            self.update_closest()
        return ce

    def edges_bytype(self, edge_type):
        conn_edges = {}
        for peer_id in self.conn_edges:
            if self.conn_edges[peer_id].edge_type in edge_type:
                conn_edges[peer_id] = self.conn_edges[peer_id]
        return conn_edges

    def edge_bystate(self, edge_state):
        conn_edges = {}
        for peer_id in self.conn_edges:
            if self.conn_edges[peer_id].edge_state in edge_state:
                conn_edges[peer_id] = self.conn_edges[peer_id]
        return conn_edges

    def filter(self, edges):
        """ Input is a list of edge state/type tuples """
        conn_edges = {}
        for peer_id in self.conn_edges:
            for etup in edges:
                if (self.conn_edges[peer_id].edge_type == etup[0] and
                        self.conn_edges[peer_id].edge_state == etup[1]):
                    conn_edges[peer_id] = self.conn_edges[peer_id]
        return conn_edges

    def update_closest(self):
        """ track the closest successor and predecessor """
        if not self.conn_edges:
            self._successor_nid = self.node_id
            self._predecessor_nid = self.node_id
            return
        nl = [*self.conn_edges.keys()]
        nl.append(self.node_id)
        nl = sorted(nl)
        idx = nl.index(self.node_id)
        nlen = len(nl)

        succ_i = (idx+1) % nlen
        self._successor_nid = nl[succ_i]

        pred_i = (idx + nlen - 1) % nlen
        self._predecessor_nid = nl[pred_i]
