import json
import sys
from controller.framework.ControllerModule import ControllerModule
import controller.framework.ipoplib as ipoplib

py_ver = sys.version_info[0]


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

        if py_ver == 3:
            pkt_ver = data[0].to_bytes(1, byteorder='big')
            pkt_type = data[1].to_bytes(1, byteorder='big')
        else:
            pkt_ver = data[0]
            pkt_type = data[1]

        if pkt_ver != ipoplib.ipop_ver:

            logCBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                              recipient='Logger',
                                              action='error',
                                              data="ipop version mismatch:"
                                              "tincan:{0} controller: {1}"
                                              .format(hex(data[0]), ipoplib.ipop_ver))
            self.CFxHandle.submitCBT(logCBT)
#            sys.exit() #XXX

        if "TINCAN_PKT" == cbt.action:

            if pkt_type == ipoplib.tincan_control:

                msg = json.loads(data[2:].decode('utf-8'))
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

                elif msg_type in ["con_stat", "con_req", "con_ack", "con_resp",
                            "peer_state", "local_state", "ping", "ping_resp"]:

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

            elif pkt_type == ipoplib.tincan_packet:

                # Send the Tincan Packet to BaseTopologyManager

                # ignore ipv6 packets
                if data[56:58] == b'\x86\xdd':
                    return

                packet = ipoplib.b2hexstr(data[2:])

                CBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                               recipient='BaseTopologyManager',
                                               action='TINCAN_PACKET',
                                               data=packet)
                self.CFxHandle.submitCBT(CBT)

            elif pkt_type == ipoplib.icc_control:

                msg = json.loads(data[56:].decode('utf-8').split("\x00")[0])

                CBT = self.CFxHandle.createCBT(initiator='TincanDispatcher',
                                               recipient='BaseTopologyManager',
                                               action='ICC_MSG',
                                               data=msg)
                self.CFxHandle.submitCBT(CBT)

            elif pkt_type == ipoplib.icc_packet:

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
                                                  data="{0}".format(data[0:]))
                self.CFxHandle.submitCBT(logCBT)
#                sys.exit() #XXX

    def timer_method(self):
        pass

    def terminate(self):
        pass
