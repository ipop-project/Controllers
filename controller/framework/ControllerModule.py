#!/usr/bin/env python
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
    def processCBT(self):
        pass

    @abstractmethod
    def timer_method(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    # returns the source CBT if the given CBT is a request CBT associated with
    # the original CBT; returns None otherwise
    def checkMapping(self, cbt):
        for key in self.CBTMappings:
            if cbt.uid in self.CBTMappings[key]:
                return key
        return None

    # tests if all request CBTs associated with the given source CBT have been
    # serviced
    def allServicesCompleted(self, sourceCBT_uid):
        requested_services = self.CBTMappings[sourceCBT_uid]
        for service in requested_services:
            if service not in self.pendingCBT:
                return False
        return True

    # create and submit CBT mask method
    def registerCBT(self, _recipient, _action, _data='', _uid=None):
        cbt = self.CFxHandle.createCBT(
            initiator = self.ModuleName,
            recipient = _recipient,
            action = _action,
            data = _data
        )
        if _uid is not None:
            cbt.uid = _uid
        self.CFxHandle.submitCBT(cbt)
        return cbt
