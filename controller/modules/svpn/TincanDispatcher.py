import json
from controller.framework.ControllerModule import ControllerModule


class TincanDispatcher(ControllerModule):

    ipop_ver = "\x02"
    tincan_control = "\x01"
    tincan_packet = "\x02"

    def __init__(self, CFxHandle, paramDict):

        super(TincanDispatcher, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict

    def initialize(self):

        logCBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                          recipient='Logger',
                                          action='info',
                                          data="TincanDispatcher Loaded")
        self.CFxHandle.submitCBT(logCBT)

    def processCBT(self, cbt):

        data = cbt.data[0]
        addr = cbt.data[1]

        # Data format:
        # ---------------------------------------------------------------
        # | offset(byte) |                                              |
        # ---------------------------------------------------------------
        # |      0       | ipop version                                 |
        # |      1       | message type                                 |
        # |      2       | Payload (JSON formatted control message)     |
        # ---------------------------------------------------------------

        if data[0] != self.ipop_ver:

            logCBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                              recipient='Logger',
                                              action='debug',
                                              data="ipop version mismatch:"
                                              "tincan:{0} controller: {1}"
                                              .format(data[0].encode("hex"),
                                                      ipop_ver.encode("hex")))
            self.CFxHandle.submitCBT(logCBT)
            sys.exit()

        if data[1] == self.tincan_control:

            msg = json.loads(data[2:])
            logCBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                              recipient='Logger',
                                              action='debug',
                                              data="recv {0} {1}"
                                              .format(addr, data[2:]))
            self.CFxHandle.submitCBT(logCBT)
            msg_type = msg.get("type", None)

            if msg_type == "echo_request":

                # Reply to the echo_request

                echo_data = {
                   'm_type': tincan_control,
                   'dest_addr': addr[0],
                   'dest_port': addr[1]
                }

                echoCBT = self.CFxHandle.createCBT(initiator='Tincan'
                                                   'Dispatcher',
                                                   recipient='TincanSender',
                                                   action='ECHO_REPLY',
                                                   data=echo_data)
                self.CFxHandle.submitCBT(echoCBT)

            elif msg_type == "local_state":

                # Send CBT to Watchdog to store ipop_state

                CBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                               recipient='Watchdog',
                                               action='STORE_IPOP_STATE',
                                               data=msg)
                self.CFxHandle.submitCBT(CBT)

            elif msg_type == "peer_state":

                # Send CBT to Monitor to store peer state

                CBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                               recipient='Monitor',
                                               action='PEER_STATE',
                                               data=msg)
                self.CFxHandle.submitCBT(CBT)

            elif (msg_type == "con_stat" or msg_type == "con_req" or
                  msg_type == "con_resp" or msg_type == "send_msg"):

                CBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                               recipient='BaseTopologyManager',
                                               action='TINCAN_MSG',
                                               data=msg)
                self.CFxHandle.submitCBT(CBT)

        #  If a packet that is destined to yet no p2p connection
        #  established node, the packet as a whole is forwarded to
        #  controller
        # |-------------------------------------------------------------|
        # | offset(byte) |                                              |
        # |-------------------------------------------------------------|
        # |      0       | ipop version                                 |
        # |      1       | message type                                 |
        # |      2       | source uid                                   |
        # |     22       | destination uid                              |
        # |     42       | Payload (Ethernet frame)                     |
        # |-------------------------------------------------------------|

        # Pass for now
        elif data[1] == self.tincan_packet:
            pass

        else:

            logCBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                              recipient='Logger',
                                              action='error',
                                              data="Tincan: "
                                              "Unrecognized message "
                                              "received from Tincan")
            self.CFxHandle.submitCBT(logCBT)

            logCBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                              recipient='Logger',
                                              action='debug',
                                              data="{0}".format(data[0:].
                                                                encode("hex")))
            self.CFxHandle.submitCBT(logCBT)
            sys.exit()

    def timer_method(self):
        pass

    def terminate(self):
        pass
