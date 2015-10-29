from abc import ABCMeta, abstractmethod  # Only Python 2.6 and above


"""
Defining an abstract class which the controller
modules will implement, forcing them to override
all the abstract methods
"""


class ControllerModule(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.pendingCBT = {}
        self.CBTMappings = {}

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

    # Check if the given cbt is a request sent by the current module
    # If yes, returns the source CBT for which the request has been
    # created, else return None
    def checkMapping(self, cbt):
        for key in self.CBTMappings:
            if(cbt.uid in self.CBTMappings[key]):
                return key
        return None

    # For a given sourceCBT's uid, check if all requests are serviced
    def allServicesCompleted(self, sourceCBT_uid):
        requested_services = self.CBTMappings[sourceCBT_uid]
        for service in requested_services:
            if(service not in self.pendingCBT):
                return False
        return True
