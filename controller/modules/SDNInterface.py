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


import json
import socket
import threading

from controller.framework.ControllerModule import ControllerModule


class SDNInterface(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(SDNInterface, self).__init__(cfx_handle, module_config,
                                           module_name)
        self.nid = self._cm_config["NodeId"]

        self.ip4 = self._cm_config["IP4"]
        self._sdn_comm_port = self._cm_config["SDNCommPort"]

        self._client_sockets = dict()
        self._client_threads = dict()

        # self._lock = threading.Lock()
        self._keep_running_server = threading.Event()
        self._keep_running_server.set()

    def initialize(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip4, self._sdn_comm_port))
        server_socket.listen()
        self._server_socket = server_socket
        self._server_thread = threading.Thread(
            target=self._adj_list_server_loop)
        self._server_thread.start()

    def _adj_list_server_loop(self):
        """
        Main body of the adjacency list server loop
        """

        while self._keep_running_server:
            cs, addr = self._server_socket.accept()
            cs.setblocking(False)
            self._client_sockets[addr] = cs

            ct = threading.Thread(
                target=self._client_req_loop, args=(addr,))
            self._client_threads[addr] = ct
            ct.start()

    def _client_req_loop(self, client_addr):
        """
        The SDN controller is our client.

        This is a blocking call. Must be run in a separate thread.
        """

        cs = self._client_sockets[client_addr]

        keep_serving_client = True
        while self._keep_running_server.is_set() and keep_serving_client:
            req = cs.recv(4096)
            print("Received request {} from SDN controller".format(req))

            # Triggered when client breaks the TCP connection
            if not req:
                keep_serving_client = False
            else:
                req_data = json.loads(req.decode("utf-8"))
                if req_data["RequestType"] == "NID":
                    cs.sendall(json.dumps({"NID": self.nid}))
                elif req_data["RequestType"] == "Neighbours":
                    # TODO Code to request updated neighbours from TOP via a CBT
                    # Note that because the CBT's response is received asynchronously,
                    # we must maintain an association between the request and the
                    # address of the host on whose behalf we are making this request
                    # The response to the client is sent directly from the response
                    # handler of this CBT
                    cs.sendall(json.dumps({"Neighbours": ["a", "b", "c"]}))

    def process_cbt(self, cbt):
        if cbt.op_type == "Response":
            cbt_data = cbt.response.data

            client_addr = cbt_data["ClientAddress"]
            cs = self._client_sockets[client_addr]
            with cs:
                neighbours = cbt_data["NeighboursList"]
                cs.sendall(json.dumps({"Neighbours": neighbours}))

            self.free_cbt(cbt)

    def timer_method(self):
        pass

    def terminate(self):
        self._keep_running_server.clear()
