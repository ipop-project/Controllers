import json
import sys
from controller.framework.ControllerModule import ControllerModule
import controller.framework.ipoplib as ipoplib

class TincanDispatcher(ControllerModule):

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

        if data[0] != ipoplib.ipop_ver:

            logCBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                              recipient='Logger',
                                              action='debug',
                                              data="ipop version mismatch:"
                                              "tincan:{0} controller: {1}"
                                              .format(data[0].encode("hex"),
                                                      ipoplib.ipop_ver.encode("hex")))
            self.CFxHandle.submitCBT(logCBT)
            sys.exit()

        if "TINCAN_PKT" == cbt.action:

            if data[1] == ipoplib.tincan_control:

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
                       'm_type': ipoplib.tincan_control,
                       'dest_addr': addr[0],
                       'dest_port': addr[1]
                    }

                    echoCBT = self.CFxHandle.createCBT(initiator='Tincan'
                                                       'Dispatcher',
                                                       recipient='TincanSender',
                                                       action='ECHO_REPLY',
                                                       data=echo_data)
                    self.CFxHandle.submitCBT(echoCBT)

                elif msg_type in ["con_stat", "con_req", "con_ack", "con_resp", "peer_state", "local_state"]:

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

            elif data[1] == ipoplib.tincan_packet:

                # Send the Tincan Packet to BaseTopologyManager

                packet = data[2:]
                CBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                               recipient='BaseTopologyManager',
                                               action='TINCAN_PACKET',
                                               data=packet)
                self.CFxHandle.submitCBT(CBT)

            elif data[1] == ipoplib.icc_control:

                msg = json.loads(data[56:].split("\x00")[0])

                CBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                               recipient='BaseTopologyManager',
                                               action='ICC_MSG',
                                               data=msg)
                self.CFxHandle.submitCBT(CBT)

            elif data[1] == ipoplib.icc_packet:

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
