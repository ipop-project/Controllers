#!/usr/bin/env python
import uuid


class CBT(object):

    def __init__(self, initiator='', recipient='', action='', data=''):

        self.uid = uuid.uuid4() # Unique identifier for CBTs
        self.initiator = initiator
        self.recipient = recipient
        self.action = action
        self.data = data
