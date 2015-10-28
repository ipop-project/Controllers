from controller.framework.ControllerModule import ControllerModule


class LinkManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict):

        super(LinkManager, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict

    def initialize(self):

        logCBT = self.CFxHandle.createCBT(initiator='LinkManager',
                                          recipient='Logger',
                                          action='info',
                                          data="LinkManager Loaded")
        self.CFxHandle.submitCBT(logCBT)

    def processCBT(self, cbt):

        if(cbt.action == "CREATE_LINK"):

            # cbt.data is a dict containing all the required
            # paramters to create a link

            TincanCBT = self.CFxHandle.createCBT(initiator='LinkManager',
                                                 recipient='TincanSender',
                                                 action='DO_CREATE_LINK',
                                                 data=cbt.data)
            self.CFxHandle.submitCBT(TincanCBT)

            logCBT = self.CFxHandle.createCBT(initiator='LinkManager',
                                              recipient='Logger',
                                              action='info',
                                              data="Creating Link with peer")
            self.CFxHandle.submitCBT(logCBT)

        elif(cbt.action == "TRIM_LINK"):

            # cbt.data is the  UID of the peer node, whose link has
            # to be trim
            TincanCBT = self.CFxHandle.createCBT(initiator='LinkManager',
                                                 recipient='TincanSender',
                                                 action='DO_TRIM_LINK',
                                                 data=cbt.data)
            self.CFxHandle.submitCBT(TincanCBT)

            logCBT = self.CFxHandle.createCBT(initiator='LinkManager',
                                              recipient='Logger',
                                              action='info',
                                              data="Trimming Link "
                                              "with peer " + cbt.data)
            self.CFxHandle.submitCBT(logCBT)

        else:
            logCBT = self.CFxHandle.createCBT(initiator='LinkManager',
                                              recipient='Logger',
                                              action='warning',
                                              data="LinkManager: Invalid CBT "
                                              "received from " + cbt.initiator)
            self.CFxHandle.submitCBT(logCBT)

    def timer_method(self):
        pass

    def terminate(self):
        pass
