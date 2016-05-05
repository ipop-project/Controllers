#!/usr/bin/env python
import select
from threading import Thread
from controller.framework.ControllerModule import ControllerModule


class TincanListener(ControllerModule):

    def __init__(self, sock_list, CFxHandle, paramDict, ModuleName):

        super(TincanListener, self).__init__(CFxHandle, paramDict, ModuleName)

        self.sock = sock_list[0]
        self.sock_svr = sock_list[1]
        self.sock_list = sock_list

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

        # create a listener thread (listens to tincan notifications
        self.TincanListenerThread = Thread(target=self.__tincan_listener)
        self.TincanListenerThread.setDaemon(True)
        self.TincanListenerThread.start()

    def processCBT(self, cbt):
        pass

    def timer_method(self):
        pass

    def __tincan_listener(self):
        while True:
            socks, _, _ = select.select(self.sock_list, [], [],
                                        self.CMConfig["socket_read_wait_time"])

            for sock in socks:
                if sock == self.sock or sock == self.sock_svr:
                    data,addr = sock.recvfrom(self.CMConfig["buf_size"])
                    self.registerCBT('TincanDispatcher', 'TINCAN_PKT', [data, addr])

    def terminate(self):
        pass
