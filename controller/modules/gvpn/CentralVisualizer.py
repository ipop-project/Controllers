import json
import socket
from controller.framework.ControllerModule import ControllerModule


class CentralVisualizer(ControllerModule):

    def __init__(self, CFxHandle, paramDict):

        super(CentralVisualizer, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict

        self.vis_address = (
            self.CMConfig["central_visualizer_addr"],
            self.CMConfig["central_visualizer_port"]
        )

    def initialize(self):
        self.vis_dbg_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        logCBT = self.CFxHandle.createCBT(initiator='CentralVisualizer',
                                          recipient='Logger',
                                          action='info',
                                          data="CentralVisualizer Loaded")
        self.CFxHandle.submitCBT(logCBT)

    def processCBT(self, cbt):
        if(cbt.action == 'SEND_INFO'):
            message = json.dumps(cbt.data).encode("utf8")
            self.vis_dbg_sock.sendto(message, self.vis_address)

    def timer_method(self):
        pass

    def terminate(self):
        pass
