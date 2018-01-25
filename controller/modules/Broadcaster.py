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
        self.register_cbt("Logger", "LOG_INFO", "{} module" \
                "loaded".format(self._module_name))

    def process_cbt(self, cbt):
        try:
            if cbt.op_type == "Request":
                if cbt.request.action == "BDC_DO_BROADCAST":
                    # We need to save the broadcast request data as the actual
                    # broadcast request will be forwarded to ICC after receipt
                    # of peer list from Topology
                    self.bcast_data = cbt.request.params

                    # Get all peers from Topology
                    self.register_cbt("Topology", "BDC_GET_PEER_LIST", None)
                else:
                    errlog = "Unsupported CBT action requested. CBT: "\
                        "{}".format(cbt)
                    self.register_cbt("Logger", "LOG_WARNING", errlog)

            elif cbt.op_type == "Response":
                if cbt.request.action == "BDC_GET_PEER_LIST":
                    peer_list = cbt.response.data

                    for peer_id in peer_list:
                        icc_req = {
                            "src_node_id": self._node_id,
                            "peer_id": peer_id,
                            "overlay_id": peer_list["peer_id"],
                            "src_module":
                                    self.bcast_data["peer_id"]["src_module"],
                            "tgt_module":
                                    self.bcast_data[peer_id]["tgt_module"],
                            "payload":
                                    self.bcast_data[peer_id]["payload"]
                        }
                        self.register_cbt("InterControllerCommunicator",
                            "BDC_DO_BROADCAST", icc_req)
                    self.free_cbt(cbt)
        except Exception as e:
            errlog = "Exception encountered in process_cbt: {}".format(str(e))
            self.register_cbt("Logger", "LOG_WARNING", errlog)

    def terminate(self):
        pass

