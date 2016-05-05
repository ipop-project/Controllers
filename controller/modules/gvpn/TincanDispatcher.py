import json
import sys
from controller.framework.ControllerModule import ControllerModule
import controller.framework.ipoplib as ipoplib

py_ver = sys.version_info[0]


class TincanDispatcher(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(TincanDispatcher, self).__init__(CFxHandle, paramDict, ModuleName)

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        data = cbt.data[0]
        addr = cbt.data[1]

        # packet format (data):
        # +---------------+-----------------------------------------------+
        # | offset (byte) |                                               |
        # +---------------+-----------------------------------------------+
        # |      0        | ipop version                                  |
        # |      1        | message type                                  |
        # |      2        | payload (JSON formatted control message)      |
        # +---------------+-----------------------------------------------+

        if py_ver == 3:
            pkt_ver = data[0].to_bytes(1, byteorder='big')
            pkt_type = data[1].to_bytes(1, byteorder='big')
        else:
            pkt_ver = data[0]
            pkt_type = data[1]

        if pkt_ver != ipoplib.ipop_ver:
            log = "ipop version mismatch: tincan: {0} controller: {1}"\
                        .format(hex(data[0]), ipoplib.ipop_ver)
            self.registerCBT('Logger', 'error', log)
#            sys.exit() #TODO

        if cbt.action == "TINCAN_PKT":

            if pkt_type == ipoplib.tincan_control:

                msg = json.loads(data[2:].decode('utf-8'))

                log = "recv {0} {1}".format(addr, data[2:])
                self.registerCBT('Logger', 'debug', log)

                msg_type = msg.get("type", None)

                if msg_type == "echo_request":

                    # reply to the echo_request
                    echo_data = {
                       'm_type': ipoplib.tincan_control,
                       'dest_addr': addr[0],
                       'dest_port': addr[1]
                    }

                    self.registerCBT('TincanSender', 'ECHO_REPLY', echo_data)

                elif msg_type in ["con_stat", "con_req", "con_ack", "con_resp",
                            "peer_state", "local_state", "ping", "ping_resp"]:
                    self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', msg)

            # tincan packets destined to a node in which a tincan connection
            # has not yet been established are forwarded to the controller
            # +---------------+-------------------------------------------+
            # | offset (byte) |                                           |
            # +---------------+-------------------------------------------+
            # |      0        | ipop version                              |
            # |      1        | message type                              |
            # |      2        | source uid                                |
            # |     22        | destination uid                           |
            # |     42        | Payload (Ethernet frame)                  |
            # +---------------+-------------------------------------------+

            elif pkt_type == ipoplib.tincan_packet:
                # ignore ipv6 packets
                if data[56:58] == b'\x86\xdd':
                    return

                packet = ipoplib.b2hexstr(data[2:])
                self.registerCBT('BaseTopologyManager', 'TINCAN_PACKET', packet)

            elif pkt_type == ipoplib.icc_control:
                msg = json.loads(data[56:].decode('utf-8').split("\x00")[0])
                self.registerCBT('BaseTopologyManager', 'ICC_CONTROL', msg)

            elif pkt_type == ipoplib.icc_packet:
                pass

            else:
                log = "Unrecognized message received from Tincan {0}"\
                        .format(data)
                self.registerCBT('Logger', 'error', log)
#                sys.exit() #TODO

        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def timer_method(self):
        pass

    def terminate(self):
        pass
