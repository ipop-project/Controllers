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

    def __init__(self, CFxHandle, paramDict, ModuleName):
        self.pendingCBT = {}
        self.CBTMappings = {}
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.ModuleName = ModuleName

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def processCBT(self, cbt):
        pass

    @abstractmethod
    def timer_method(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    # create and submit CBT mask method
    def registerCBT(self, _recipient, _action, _data='', _tag=None):
        cbt = self.CFxHandle.createCBT(
            #vnet = _vnet,
            initiator = self.ModuleName,
            recipient = _recipient,
            action = _action,
            data = _data
        )
        if _tag is not None:
            cbt.Tag = _tag
        self.CFxHandle.submitCBT(cbt)
        return cbt
