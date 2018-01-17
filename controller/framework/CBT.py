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
    TagCounter = 0
    class Request(object):
        def __init__(self, initiator='', recipient='', action='', params=None):
            self.Initiator = initiator
            self.Recipient = recipient
            self.Action = action
            self.Params = params

        def __repr__(self):
            msg = "{\n\t\tInitiator: %s,\n\t\tRecipient: %s,\n\t\tAction: %s,\n\t\tData: %s\n\t}" % (self.Initiator, self.Recipient, self.Action, str(self.Params))
            return msg

    class Response(object):
        def __init__(self,):
            self.Status = False
            self.Initiator = None
            self.Recipient = None
            self.Data = None

        def __repr__(self):
            msg = "{\n\t\tStatus: %s,\n\t\tInitiator: %s,\n\t\tRecipient: %s,\n\t\tData: %s\n\t}" % (self.Status, self.Initiator, self.Recipient, str(self.Data))
            return msg

    def __init__(self, initiator='', recipient='', action='', params=''):
        self.Tag = CBT.TagCounter
        CBT.TagCounter = CBT.TagCounter + 1
        self.Parent = None
        self.ChildCount = 0
        self.Completed = False
        self.OpType = "Request"
        self.Request = self.Request(initiator, recipient, action, params)

    def __repr__(self):
        msg = "{\n\tParent: %s,\n\tChildCount: %d,\n\tCompleted: %r,\n\tOpType: %s,\n\tRequest: %r,\n\tResponse: %r\n}" % (str(self.Parent), self.ChildCount, self.Completed, self.OpType, self.Request, self.Response)
        return msg

    def SetRequest(self, initiator='', recipient='', action='', params=''):
        self.Request.Initiator = initiator
        self.Request.Recipient = recipient
        self.Request.Action = action
        self.Request.Params = params

    def SetResponse(self, data='', status = False):
        self.OpType = "Response"
        self.Response = self.Response()
        self.Response.Initiator = self.Request.Recipient
        self.Response.Recipient = self.Request.Initiator
        self.Response.Status = status
        self.Response.Data = data
