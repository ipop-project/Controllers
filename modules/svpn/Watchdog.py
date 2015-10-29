from controller.framework.ControllerModule import ControllerModule


class Watchdog(ControllerModule):

    def __init__(self, CFxHandle, paramDict):

        super(Watchdog, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.ipop_state = None

    def initialize(self):

        logCBT = self.CFxHandle.createCBT(initiator='Watchdog',
                                          recipient='Logger',
                                          action='info',
                                          data="Watchdog Loaded")
        self.CFxHandle.submitCBT(logCBT)

    def processCBT(self, cbt):

        if(cbt.action == 'STORE_IPOP_STATE'):

            # cbt.data contains the state of local node
            msg = cbt.data
            self.ipop_state = msg

        elif(cbt.action == 'QUERY_IPOP_STATE'):

            cbt.action = 'QUERY_IPOP_STATE_RESP'
            cbt.data = self.ipop_state
            cbt.initiator, cbt.recipient = cbt.recipient, cbt.initiator

            # Submit the CBT back to the initiator
            # cbt.data contains ipop_state
            self.CFxHandle.submitCBT(cbt)

        else:

            logCBT = self.CFxHandle.createCBT(initiator='Watchdog',
                                              recipient='Logger',
                                              action='error',
                                              data="Watchdog: Unrecognized CBT"
                                              "from: " + cbt.initiator)
            self.CFxHandle.submitCBT(logCBT)

    def timer_method(self):

        TincanCBT = self.CFxHandle.createCBT(initiator='Watchdog',
                                             recipient='TincanSender',
                                             action='DO_GET_STATE',
                                             data='')
        self.CFxHandle.submitCBT(TincanCBT)

    def terminate(self):
        pass
