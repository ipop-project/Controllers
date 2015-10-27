import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule


class AddressMapper(ControllerModule):

    def __init__(self, CFxHandle, paramDict):

        super(AddressMapper, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.ip_map = dict(ipoplib.IP_MAP)

    def initialize(self):

        logCBT = self.CFxHandle.createCBT(initiator='AddressMapper',
                                          recipient='Logger',
                                          action='info',
                                          data="AddressMapper Loaded")
        self.CFxHandle.submitCBT(logCBT)

    def processCBT(self, cbt):

        if(cbt.action == 'ADD_MAPPING'):

            try:
                # cbt.data is a dict with uid and ip keys
                self.ip_map[cbt.data['uid']] = cbt.data['ip']
            except KeyError:

                logCBT = self.CFxHandle.createCBT(initiator='AddressMapper',
                                                  recipient='Logger',
                                                  action='warning',
                                                  data="Invalid ADD_MAPPING"
                                                  " Configuration")
                self.CFxHandle.submitCBT(logCBT)

        elif(cbt.action == 'DEL_MAPPING'):

            self.ip_map.pop(cbt.data)  # Remove mapping if it exists

        elif(cbt.action == 'RESOLVE'):

            # Modify the CBT with the response data and send it back
            cbt.action = 'RESOLVE_RESP'

            # Compute the IP4 address
            cbt.data = ipoplib.gen_ip4(cbt.data, self.ip_map)

            # Swap inititator and recipient
            cbt.initiator, cbt.recipient = cbt.recipient, cbt.initiator

            self.CFxHandle.submitCBT(cbt)

        elif (cbt.action == 'QUERY_IP_MAP'):

            cbt.action = 'QUERY_IP_MAP_RESP'

            cbt.data = self.ip_map
            cbt.initiator, cbt.recipient = cbt.recipient, cbt. initiator
            self.CFxHandle.submitCBT(cbt)

        elif(cbt.action == 'REVERSE_RESOLVE'):

            # Modify the CBT with the response data and send it back
            cbt.action = 'REVERSE_RESOLVE_RESP'
            ip = cbt.data
            cbt.data = None
            # Iterate through all items in dict for reverse lookup
            for key, value in self.ip_map.items():
                if(value == ip):
                    cbt.data = key
                    break

            # Swap inititator and recipient
            cbt.initiator, cbt.recipient = cbt.recipient, cbt.initiator

            self.CFxHandle.submitCBT(cbt)

        else:
            logCBT = self.CFxHandle.createCBT(initiator='AddressMapper',
                                              recipient='Logger',
                                              action='warning',
                                              data="AddressMapper: "
                                              "Invalid CBT received"
                                              " from " + cbt.initiator)
            self.CFxHandle.submitCBT(logCBT)

    def timer_method(self):
        pass

    def terminate(self):
        pass
