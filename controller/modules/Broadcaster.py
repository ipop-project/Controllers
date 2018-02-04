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


from controller.framework.ControllerModule import ControllerModule


class Broadcaster(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Broadcaster, self).__init__(cfx_handle, module_config,
            module_name)
        self._bcast_data = None
        self._node_id = str(self._cm_config["NodeId"])

    def initialize(self):
        self.register_cbt("Logger", "LOG_INFO", "{} module"
                " loaded".format(self._module_name))

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "BDC_ARP_BROADCAST":
                print("BDC rcvd req from ", cbt.request.initiator)
                print("Data is:", cbt.request.params)
                # We need to save the broadcast request data as the actual
                # broadcast request will be forwarded to ICC after receipt
                # of peer list from Topology
                self.bcast_data = cbt.request.params
                self.bcast_data["action"] = "TCI_INJECT_FRAME"

                # Get all peers from Topology
                print("Sent cbt to top for peer list")
                lcbt = self.create_linked_cbt(cbt)
                lcbt.set_request(self._module_name, "Topology",
                                 "TOP_QUERY_PEER_IDS", None)
                self.submit_cbt(lcbt)
            else:
                errlog = "Unsupported CBT action requested. CBT: "\
                    "{}".format(cbt)
                self.register_cbt("Logger", "LOG_WARNING", errlog)

        elif cbt.op_type == "Response":
            if not cbt.response.status:
                self.register_cbt(
                    "Logger", "LOG_WARNING",
                    "CBT failed {0}".format(cbt.response.data))
                return

            if cbt.request.action == "TOP_QUERY_PEER_IDS":
                self._handle_resp_top_query_peer_ids(cbt)

    def _handle_resp_top_query_peer_ids(self, cbt):
        if not cbt.response.status:
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
            return

        overlay_peers = cbt.response.data

        icc_req = {
            "overlay_id": self.bcast_data["overlay_id"],
            "src_node_id": self._node_id,
            "peer_ids": overlay_peers[
                self.bcast_data["overlay_id"]],
            "src_module":
                    self.bcast_data["src_module"],
            "tgt_modules":
                    self.bcast_data["tgt_modules"],
            "action": self.bcast_data["action"],
            "payload":
                    self.bcast_data["payload"]
        }
        self.register_cbt("Icc",
                          "ICC_REMOTE_ACTION", icc_req)
        print("Sent broadcast req to icc")
        self.complete_cbt(self.get_parent_cbt(cbt))
        self.free_cbt(cbt)

    def timer_method(self):
        pass

    def terminate(self):
        pass
