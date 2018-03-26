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

from controller.framework.ControllerModule import ControllerModule


class Broadcaster(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Broadcaster, self).__init__(cfx_handle,
                                          module_config,
                                          module_name)
        self._bcast_data = None
        self._node_id = str(self._cm_config["NodeId"])

        # Cache for all the peers that Topology thinks it has a link to
        # The reason we are not populating this cache at start time is because
        # Topology might not have enough data to send
        # This cache is updated for every invocation of self.timer_method
        self._overlay_peers = dict()
        self._overlay_peers_lock = threading.Lock()

    def initialize(self):
        self.register_cbt("Logger", "LOG_INFO", "{} module"
                          " loaded".format(self._module_name))

    def _bcast_on_icc(self, bcast_data):
        for recipient_id in self._overlay_peers[bcast_data["overlay_id"]]:
            icc_req = {
                "OverlayId": bcast_data["overlay_id"],
                "RecipientId": recipient_id,
                "RecipientCM": bcast_data["tgt_module"],
                "Action": bcast_data["action"],
                "Params": {
                    "OverlayId": bcast_data["overlay_id"],
                    "Data": bcast_data["payload"]
                }
            }
            self.register_cbt("Icc",
                              "ICC_REMOTE_ACTION", icc_req)

    def req_handler_broadcast(self, cbt):
        if self._overlay_peers:
            self._bcast_on_icc(cbt.request.params)
        else:
            # Get all peers from Topology
            lcbt = self.create_linked_cbt(cbt)
            lcbt.set_request(self._module_name, "Topology",
                             "TOP_QUERY_PEER_IDS", "BuildCache")
            self.submit_cbt(lcbt)

    def resp_handler_remote_act(self, cbt):
        if not cbt.response.status:
            self._overlay_peers = None
            self.register_cbt("Topology", "TOP_QUERY_PEER_IDS", "RefreshCache")
        parent_cbt = self.get_parent_cbt(cbt)
        self.free_cbt(cbt)
        if parent_cbt:
            parent_cbt.set_response(None, cbt.response.status)
            self.complete_cbt(parent_cbt)

    def resp_handler_query_peers(self, cbt):
        if cbt.request.params == "RefreshCache":
            with self._overlay_peers_lock:
                self._overlay_peers = cbt.response.data
            self.free_cbt(cbt)
        elif cbt.request.params == "BuildCache":
            with self._overlay_peers_lock:
                self._overlay_peers = cbt.response.data

            bcast_data = self.get_parent_cbt(cbt).request.params
            self._bcast_on_icc(bcast_data)
            parent_cbt = self.get_parent_cbt(cbt)
            # free child first
            self.free_cbt(cbt)
            # then complete parent
            self.complete_cbt(parent_cbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "BDC_BROADCAST":
                self.req_handler_broadcast(cbt)
            else:
                self.req_handler_default(cbt)
        elif cbt.op_type == "Response":
            if cbt.request.action == "TOP_QUERY_PEER_IDS":
                self.resp_handler_query_peers(cbt)
            elif cbt.request.action == "ICC_REMOTE_ACTION":
                self.resp_handler_remote_act(cbt)

    def timer_method(self):
        self.register_cbt("Topology", "TOP_QUERY_PEER_IDS", "RefreshCache")

    def terminate(self):
        pass
