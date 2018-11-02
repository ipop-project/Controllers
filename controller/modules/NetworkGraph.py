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
class ConnectionEdge(object):
    """ A discriptor of the edge/link between two peers."""
    def __init__(self, peer_id=None, edge_type="CETypeUnknown"):
        self.peer_id = peer_id
        self.link_id = None
        self.marked_for_delete = False
        self.created_time = time.time()
        self.connected_time = None
        self.state = "CEStateUnknown"
        self.edge_type = edge_type

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
        #state = "<peer_id = %s, link_id = %s, marked_for_delete = %s, created_time = %s,"\
        #        "state = %s, type = %s>" % (self.peer_id, self.link_id, self.marked_for_delete,
        #                                    str(self.created_time), self.state, self.edge_type)
        msg = "<peer_id = %s, type = %s>" % (self.peer_id, self.edge_type)
        return msg

class ConnEdgeAdjacenctList(object):
    """ A series of ConnectionEdges that are incident on the local node"""
    def __init__(self, overlay_id=None, node_id=None):
        self.overlay_id = overlay_id
        self.node_id = node_id
        self.conn_edges = {}

    def __len__(self):
        return len(self.conn_edges)

    def __repr__(self):
        msg = "<overlay_id = %s, node_id = %s, conn_edges = %s>"%(self.overlay_id,
                                                                  self.node_id, self.conn_edges)
        return msg

    def add_connection_edge(self, ce):
        self.conn_edges[ce.peer_id] = ce

    def get_edges(self):
        return self.conn_edges.values()

    def edge_type_count(self, edge_type):
        cnt = 0
        for peer_id in self.conn_edges:
            if self.conn_edges[peer_id].edge_type == edge_type:
                cnt = cnt + 1
        return cnt

class NetworkGraph(object):
    """Describes the structure of the Topology as a dict of node IDs to ConnEdgeAdjacenctList"""
    def __init__(self, graph=None):
        self._graph = graph
        if self._graph is None:
            self._graph = {}

    def vertices(self):
        """ returns the vertices of a graph """
        return list(self._graph.keys())

    def edges(self):
        """ returns the edges of a graph """
        return self._generate_edges()

    def find_isolated_nodes(self):
        """ returns a list of isolated nodes. """
        isolated = []
        for node in self._graph:
            if not self._graph[node]:
                isolated += node
        return isolated

    def add_adj_list(self, adj_list):
        self._graph[adj_list.node_id] = adj_list

    def add_vertex(self, vertex):
        """ Adds vertex "vertex" as a key with an empty ConnEdgeAdjacenctList to self._graph. """
        if vertex not in self._graph:
            self._graph[vertex] = ConnEdgeAdjacenctList()

    def add_edge(self, edge):
        pass

    def _generate_edges(self):
        """
        Generating the edges of the graph "graph". Edges are represented as sets
        with one (a loop back to the vertex) or two vertices
        """
        edges = set()
        for vertex in self._graph:
            for neighbour in self._graph[vertex].get_edges():
                edge = (vertex, neighbour)
                edges.add(edge)
        return sorted(edges)

    def __str__(self):
        res = "vertices: "
        for k in self._graph:
            res += str(k) + " "
        res += "\nedges:\n"
        for edge in self._generate_edges():
            res += str(edge) + "\n"
        return res

    # todo: fix methods below
    def find_path(self, start_vertex, end_vertex, path=None):
        """
        Find a path from start_vertex to end_vertex in graph
        """
        if path is None:
            path = []
        graph = self._graph
        path = path + [start_vertex]
        if start_vertex == end_vertex:
            return path
        if start_vertex not in graph:
            return None
        for vertex in graph[start_vertex]:
            if vertex not in path:
                extended_path = self.find_path(vertex,
                                               end_vertex,
                                               path)
                if extended_path:
                    return extended_path
        return None

    def find_all_paths(self, start_vertex, end_vertex, path=None):
        """ find all paths from start_vertex to
            end_vertex in graph """
        if not path:
            path = []
        graph = self._graph
        path = path + [start_vertex]
        if start_vertex == end_vertex:
            return [path]
        if start_vertex not in graph:
            return []
        paths = []
        for vertex in graph[start_vertex]:
            if vertex not in path:
                extended_paths = self.find_all_paths(vertex,
                                                     end_vertex,
                                                     path)
                for p in extended_paths:
                    paths.append(p)
        return paths

    def vertex_degree(self, vertex):
        """ The degree of a vertex is the number of edges connecting
            it, i.e. the number of adjacent vertices. Loops are counted
            double, i.e. every occurence of vertex in the list
            of adjacent vertices. """
        adj_vertices = self._graph[vertex]
        degree = len(adj_vertices) + adj_vertices.count(vertex)
        return degree

    def delta(self):
        """ the minimum degree of the graph """
        minv = 100000000
        for vertex in self._graph:
            vertex_degree = self.vertex_degree(vertex)
            if vertex_degree < minv:
                minv = vertex_degree
        return minv

    def Delta(self):
        """ the maximum degree of the graph """
        maxv = 0
        for vertex in self._graph:
            vertex_degree = self.vertex_degree(vertex)
            if vertex_degree > maxv:
                maxv = vertex_degree
        return maxv
