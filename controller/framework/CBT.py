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

import uuid

class CBT(object):
    class Request(object):
        def __init__(self, initiator='', recipient='', action='', data=None):
            self.Initiator = initiator
            self.Recipient = recipient
            self.Action = action
            self.Data = data

    class Response(object):
        def __init__(self,):
            self.Status = False
            self.Initiator = None
            self.Recipient = None
            self.Data = None

    def __init__(self, initiator='', recipient='', action='', data=''):
        self.Tag = uuid.uuid4()  # Unique identifier for CBTs
        #self.vnet = vnet
        self.Parent = None
        self.ChildCount = 0
        self.Completed = False
        self.OpType = "Request"
        self.initiator = initiator #deprecated
        self.recipient = recipient #deprecated
        self.action = action #deprecated
        self.data = data
        self.Request = self.Request(initiator, recipient, action, data)

    def SetRequest(self, initiator='', recipient='', action='', data=''):
        self.Request.Initiator = initiator
        self.Request.Recipient = recipient
        self.Request.Action = action
        self.Request.Data = data

    def Response(self, initiator='', recipient='', data='', status = False):
        self.optype = "Response"
        self.initiator = initiator
        self.recipient = recipient
        self.Completed = True
        self.Response = self.Response()
        self.Response.Status = status
        self.Response.Initiator = initiator
        self.Response.Recipient = recipient
        self.Response.Data = data
