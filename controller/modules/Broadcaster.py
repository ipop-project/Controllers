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
        super(Broadcaster, self).__init__(cfx_handle, module_config,
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

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "BDC_BROADCAST":
                print("BDC rcvd req from ", cbt.request.initiator)
                print("Data is:", cbt.request.params)

                if not self._overlay_peers:
                    # Get all peers from Topology
                    print("Sent cbt to top for peer list")
                    lcbt = self.create_linked_cbt(cbt)
                    lcbt.set_request(self._module_name, "Topology",
                                     "TOP_QUERY_PEER_IDS", "BuildCache")
                    self.submit_cbt(lcbt)
                else:
                    self._handle_resp_top_query_peer_ids(cbt.request.params)
            else:
                errlog = "Unsupported CBT action requested. CBT: "\
                    "{}".format(cbt)
                self.register_cbt("Logger", "LOG_WARNING", errlog)

        elif cbt.op_type == "Response":
            if not cbt.response.status:
                self.register_cbt(
                    "Logger", "LOG_WARNING",
                    "CBT failed {0}".format(cbt.response.data))
                parent_cbt = self.get_parent_cbt(cbt)
                self.free_cbt(cbt)
                if parent_cbt:
                    self.complete_cbt(parent_cbt, status=False)

                return

            if cbt.request.action == "TOP_QUERY_PEER_IDS":
                if cbt.request.params == "RefreshCache":
                    with self._overlay_peers_lock:
                        self._overlay_peers = cbt.response.data
                    self.free_cbt(cbt)
                elif cbt.request.params == "BuildCache":
                    bcast_data = cbt.response.data
                    self._handle_resp_top_query_peer_ids(bcast_data)
                    parent_cbt = self.get_parent_cbt(cbt)
                    # free child first
                    self.free_cbt(cbt)
                    # then complete parent
                    self.complete_cbt(parent_cbt)
            elif cbt.request.action == "ICC_REMOTE_ACTION":
                self.free_cbt(cbt)

    def _handle_resp_top_query_peer_ids(self, bcast_data):
        for recipient_id in self._overlay_peers[bcast_data["overlay_id"]]:
            icc_req = {
                "OverlayId": bcast_data["overlay_id"],
                "RecipientId": recipient_id,
                "RecipientCM": bcast_data["tgt_module"],
                "Action": bcast_data["action"],
                "Params": bcast_data["payload"]
            }
            self.register_cbt("Icc",
                              "ICC_REMOTE_ACTION", icc_req)
            print("Sent broadcast req to icc")

    def timer_method(self):
        self.register_cbt("Topology", "TOP_QUERY_PEERS_IDS", "CacheRefresh")

    def terminate(self):
        pass
