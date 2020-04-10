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
from controller.modules.NetworkGraph import EdgeTypesOut

OpType = ["OpTypeAdd", "OpTypeRemove", "OpTypeUpdate"]

class OperationsModel():
    def __init__(self, conn_edge, op_type, priority):
        self.conn_edge = conn_edge
        self.op_type = op_type
        self.op_priority = priority
        self.is_completed = False

    def __repr__(self):
        msg = "conn_edge = %s, op_type = %s, op_priority=%s>" % \
              (self.conn_edge, self.op_type, self.op_priority)
        return msg


class NetworkOperations():
    def __init__(self, current_net_graph, tgt_net_graph):
        self.curr_net_graph = current_net_graph
        self.tgt_net_graph = tgt_net_graph
        self.operations = {}
        self._remain = 0

    def __iter__(self):
        sorted_list = sorted(
            self.operations, key=lambda x: self.operations[x].op_priority)
        for x in sorted_list:
            if not self.operations[x].is_completed:
                self.operations[x].is_completed = True
                if self._remain > 0:
                    self._remain -= 1
                yield self.operations[x]

    def __repr__(self):
        msg = "current_net_graph = %s, tgt_net_graph = %s, num_operations=%d, " \
              "operations=%s>" % \
              (self.curr_net_graph, self.tgt_net_graph,
               len(self.operations), self.operations)
        return msg

    def __str__(self):
        msg = "num_operations=%d, operations=%s>" % (len(self.operations), self.operations)
        return msg

    def __bool__(self):
        return self._remain > 0

    def diff(self):
        for peer_id in self.tgt_net_graph.conn_edges:
            if peer_id not in self.curr_net_graph.conn_edges:
                # Op Add
                if self.tgt_net_graph.conn_edges[peer_id].edge_type == 'CETypeEnforced':
                    op = OperationsModel(
                        self.tgt_net_graph.conn_edges[peer_id], OpType[0], 1)
                    self.operations[peer_id] = op
                elif self.tgt_net_graph.conn_edges[peer_id].edge_type == "CETypeSuccessor":
                    op = OperationsModel(
                        self.tgt_net_graph.conn_edges[peer_id], OpType[0], 2)
                    self.operations[peer_id] = op
                elif self.tgt_net_graph.conn_edges[peer_id].edge_type == "CETypeOnDemand":
                    op = OperationsModel(
                        self.tgt_net_graph.conn_edges[peer_id], OpType[0], 4)
                    self.operations[peer_id] = op
                elif self.tgt_net_graph.conn_edges[peer_id].edge_type == "CETypeLongDistance":
                    op = OperationsModel(
                        self.tgt_net_graph.conn_edges[peer_id], OpType[0], 7)
                    self.operations[peer_id] = op
            else:
                # Op Update
                op = OperationsModel(
                    self.tgt_net_graph.conn_edges[peer_id], OpType[2], 0)
                self.operations[peer_id] = op

        for peer_id in self.curr_net_graph.conn_edges:
            if peer_id not in self.tgt_net_graph.conn_edges:
                if self.curr_net_graph.conn_edges[peer_id].edge_type in EdgeTypesOut:
                    # Op Remove
                    if self.curr_net_graph.conn_edges[peer_id].edge_state == "CEStateConnected" and\
                           time.time() - self.curr_net_graph[peer_id].connected_time > 30:
                        if self.curr_net_graph.conn_edges[peer_id].edge_type == "CETypeOnDemand":
                            op = OperationsModel(
                                self.curr_net_graph.conn_edges[peer_id], OpType[1], 3)
                            self.operations[peer_id] = op
                        elif self.curr_net_graph.conn_edges[peer_id].edge_type == "CETypeSuccessor":
                            op = OperationsModel(
                                self.curr_net_graph.conn_edges[peer_id], OpType[1], 5)
                            self.operations[peer_id] = op
                        elif self.curr_net_graph.conn_edges[peer_id].edge_type == \
                            "CETypeLongDistance":
                            op = OperationsModel(
                                self.curr_net_graph.conn_edges[peer_id], OpType[1], 6)
                            self.operations[peer_id] = op
        #self._remain - len(self.operations)
