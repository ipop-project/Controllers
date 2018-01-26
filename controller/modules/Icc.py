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


class Icc(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Icc, self).__init__(cfx_handle, module_config, module_name)

    def initialize(self):
        self._flag = True
        self.register_cbt("Logger", "LOG_INFO", "Module Loaded")

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "ICC_SEND_DATA":
                self.convert_peerid_to_linkid(cbt)
        elif cbt.op_type == "Response":
            if cbt.request.action == "LNK_GET_LINKID":
                if (not cbt.response.status):
                    self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.message))
                else:
                    self.send_icc_data(cbt)
            self.free_cbt(cbt)

    def convert_peerid_to_linkid(self, cbt):
        param = {}
        param["OverlayId"] = cbt.request.params["OverlayId"]
        param["PeerId"] = cbt.request.params["PeerId"]
        lcbt = self.create_linked_cbt(cbt)
        lcbt.set_request(self._module_name, "LinkManager", "LNK_GET_LINKID", param)
        self.submit_cbt(lcbt)

    def send_icc_data(self, cbt):
        param = {}
        param["OverlayId"] = cbt.response.data["OverlayId"]
        param["LinkId"] = cbt.response.data["LinkId"]
        pcbt = self.get_parent_cbt(cbt)
        param["Data"] = pcbt.request.params["Data"]
        param["ModuleName"] = pcbt.request.params["ModuleName"]

    def terminate(self):
        pass

    def timer_method(self):
        if self._flag:
            msg = {}
            msg["OverlayId"] = "OLN1"
            msg["PeerId"] = "Peer1"
            msg["ModuleName"] = "ICC"
            msg["Data"] = "Test ICC Data"
            cbt = self.create_cbt(self._module_name, self._module_name, "ICC_SEND_DATA", msg)
            self.submit_cbt(cbt)
            self._flag = False
