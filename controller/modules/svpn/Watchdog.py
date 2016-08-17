#!/usr/bin/env python
from controller.framework.ControllerModule import ControllerModule


class Watchdog(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(Watchdog, self).__init__(CFxHandle, paramDict, ModuleName)

        self.ipop_state = None
        # Nodes discovered from XMPP
        self.discovered_nodes = []
        # Nodes to which a connection was initiated-may be online/offline
        self.linked_nodes = []
        
    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        if cbt.action == 'XMPP_MSG':
            msg = cbt.data
            msg_type = msg.get("type", None)
            if (msg_type == "xmpp_advertisement"):
                self.discovered_nodes.append(msg["data"])
                self.discovered_nodes = list(set(self.discovered_nodes))
                log = "recv xmpp_advt: {0}".format(msg["uid"])
                self.registerCBT('Logger', 'info', log)
                
        elif cbt.action == 'STORE_IPOP_STATE':
            # cbt.data contains the local state
            msg = cbt.data
            self.ipop_state = msg

        elif cbt.action == 'QUERY_IPOP_STATE':
            self.registerCBT(cbt.initiator, 'QUERY_IPOP_STATE_RESP', self.ipop_state, cbt.uid)

        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def timer_method(self):
        self.registerCBT('TincanSender', 'DO_GET_STATE','')
        for uid in self.discovered_nodes:
            self.registerCBT('TincanSender', 'DO_GET_STATE',uid)

    def terminate(self):
        pass
