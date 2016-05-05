#!/usr/bin/env python
import json
import socket
from controller.framework.ControllerModule import ControllerModule


class CentralVisualizer(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(CentralVisualizer, self).__init__(CFxHandle, paramDict, ModuleName)

        self.vis_address = (
            self.CMConfig["central_visualizer_addr"],
            self.CMConfig["central_visualizer_port"])

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

        self.vis_dbg_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def processCBT(self, cbt):
        if cbt.action == 'SEND_INFO':
            cbt.data["name"] = self.CMConfig["name"]

            message = json.dumps(cbt.data).encode("utf8")
            self.vis_dbg_sock.sendto(message, self.vis_address)

        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def timer_method(self):
        pass

    def terminate(self):
        pass
