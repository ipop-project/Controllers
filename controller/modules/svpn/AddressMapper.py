#!/usr/bin/env python
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule


class AddressMapper(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(AddressMapper, self).__init__(CFxHandle, paramDict, ModuleName)

        self.ip_map = {}

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        if cbt.action == 'ADD_MAPPING':
            try:
                # cbt.data contains a {'uid': <uid>, 'ip': <ip>} mapping
                self.ip_map[cbt.data['uid']] = cbt.data['ip']
            except KeyError:
                log = "invalid ADD_MAPPING configuration"
                self.registerCBT('Logger', 'warning', log)

        elif cbt.action == 'DEL_MAPPING':
            self.ip_map.pop(cbt.data)

        elif cbt.action == 'RESOLVE':
            data = ipoplib.gen_ip4(cbt.data, self.ip_map, self.CMConfig["ip4"])
            self.registerCBT(cbt.initiator, 'RESOLVE_RESP', data, cbt.uid)

        elif cbt.action == 'QUERY_IP_MAP':
            self.registerCBT(cbt.initiator, 'QUERY_IP_MAP_RESP', self.ip_map, cbt.uid)

        elif cbt.action == 'REVERSE_RESOLVE':
            self.registerCBT(cbt.initiator, 'REVERSE_RESOLVE_RESP', data, cbt.uid)

        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def timer_method(self):
        pass

    def terminate(self):
        pass
