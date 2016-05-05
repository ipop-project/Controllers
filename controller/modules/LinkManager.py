#!/usr/bin/env python
from controller.framework.ControllerModule import ControllerModule


class LinkManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(LinkManager, self).__init__(CFxHandle, paramDict, ModuleName)

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        if cbt.action == "CREATE_LINK":
            # cbt.data contains DO_CREATE_LINK arguments
            self.registerCBT('TincanSender', 'DO_CREATE_LINK', cbt.data)
            self.registerCBT('Logger', 'info', 'creating link with peer')

        elif cbt.action == "TRIM_LINK":
            # cbt.data contains the UID of the peer
            self.registerCBT('TincanSender', 'DO_TRIM_LINK', cbt.data)

        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def timer_method(self):
        pass

    def terminate(self):
        pass
