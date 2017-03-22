#!/usr/bin/env/ python
import logging
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule

class BroadCastController(ControllerModule):
    def __init__(self,CFxHandle, paramDict,ModuleName):
        super(BroadCastController,self).__init__(CFxHandle,paramDict,ModuleName)
        
    def initialize(self):
        self.registerCBT('Logger','info',"{0} Loaded".format(self.ModuleName))
        
    def processCBT(self, cbt):
        if cbt.action == 'BroadcastPkt':
            recvd_frame = cbt.data
            self.registerCBT('Logger','info',"Controller has Recvd Broadcast Frame")
            #self.registerCBT('Logger', 'info', recvd_frame)
            self.registerCBT('BroadCastForwarder','BroadcastPkt',recvd_frame)
        elif cbt.action  == "BroadcastData":
            recvd_frame = cbt.data
            self.registerCBT('Logger', 'info', "Controller has Recvd Broadcast Data")
            # self.registerCBT('Logger', 'info', recvd_frame)
            self.registerCBT('BroadCastForwarder', 'BroadcastData', recvd_frame)
        else:
            recvd_frame = cbt.data
            self.registerCBT('Logger', 'info', "Controller has Recvd Multicast Frame")
            # self.registerCBT('Logger', 'info', recvd_frame)
            self.registerCBT('BroadCastForwarder', 'multicast', recvd_frame)

        
    def terminate(self):
        pass
        
    def timer_method(self):
        pass
        
    
