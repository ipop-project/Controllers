#!/usr/bin/env python
from controller.framework.ControllerModule import ControllerModule


class Watchdog(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(Watchdog, self).__init__(CFxHandle, paramDict, ModuleName)

        self.ipop_state = None

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        if cbt.action == 'STORE_IPOP_STATE':
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
        self.registerCBT('TincanSender', 'DO_GET_STATE')

    def terminate(self):
        pass
