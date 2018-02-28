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

from abc import ABCMeta, abstractmethod


# abstract ControllerModule (CM) class
# all CM implementations inherit the variables declared here
# all CM implementations must override the abstract methods declared here
class ControllerModule(object):

    __metaclass__ = ABCMeta

    def __init__(self, cfx_handle, module_config, module_name):
        #self._pending_cbt = {}
        self._cfx_handle = cfx_handle
        self._cm_config = module_config
        self._module_name = module_name

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def process_cbt(self, cbt):
        pass

    @abstractmethod
    def timer_method(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    def req_handler_default(self, cbt):
        log = "Unsupported CBT action {0}".format(cbt)
        self.register_cbt("Logger", "LOG_WARNING", log)
        cbt.set_response(log, False)
        self.complete_cbt(cbt)

    # create and submit CBT mask method
    def register_cbt(self, _recipient, _action, _params=None):
        cbt = self._cfx_handle.create_cbt(
            initiator=self._module_name,
            recipient=_recipient,
            action=_action,
            params=_params
        )
        self._cfx_handle.submit_cbt(cbt)
        return cbt

    def create_cbt(self, initiator, recipient, action, params=None):
        return self._cfx_handle.create_cbt(initiator, recipient, action, params)

    def create_linked_cbt(self, parent):
        return self._cfx_handle.create_linked_cbt(parent)

    def complete_cbt(self, cbt):
        self._cfx_handle.complete_cbt(cbt)

    def free_cbt(self, cbt):
        self._cfx_handle.free_cbt(cbt)

    def get_parent_cbt(self, cbt):
        return self._cfx_handle.get_parent_cbt(cbt)

    def submit_cbt(self, cbt):
        self._cfx_handle.submit_cbt(cbt)
