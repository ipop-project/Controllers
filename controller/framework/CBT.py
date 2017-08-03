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
        def __init__(self, initiator='', recipient='', action='', data=''):
            self.initiator = initiator
            self.recipient = recipient
            self.action = action
            self.data = data
    class Response(object):
        def __init__(self,):
            self.status = False
            self.initiator = None
            self.recipient = None
            self.data = None

    def __init__(self, initiator='', recipient='', action='', data=''):
        self.uid = uuid.uuid4()  # Unique identifier for CBTs
        self.optype = "Request"
        self.initiator = initiator
        self.recipient = recipient
        self.action = action
        self.data = data
        self.request = self.Request(initiator, recipient, action, data)

    def response(self, initiator='', recipient='', data='', status = False):
        self.optype = "Response"
        self.initiator = initiator
        self.recipient = recipient
        self.response = self.Response()
        self.response.status = status
        self.response.initiator = initiator
        self.response.recipient = recipient
        self.response.data = data
