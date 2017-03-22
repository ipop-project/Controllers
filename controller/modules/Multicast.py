from controller.framework.ControllerModule import ControllerModule
import time

class Multicast(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(Multicast, self).__init__(CFxHandle, paramDict, ModuleName)
        self.ConfigData = paramDict
        self.tincanparams = self.CFxHandle.queryParam("Tincan","Vnets")
        self.ipop_interface_details = {}
        for k in range(len(self.tincanparams)):
            interface_name  = self.tincanparams[k]["TapName"]
            self.ipop_interface_details[interface_name] = {}
            interface_detail                            = self.ipop_interface_details[interface_name]
            interface_detail["uid"]                     = self.tincanparams[k]["uid"]
            interface_detail["msgcount"]                = {}
            interface_detail["mac"]                     = ""
            interface_detail["ip"]                      = self.tincanparams[k]["IP4"]
            interface_detail["local_mac_ip_table"]      = {}
        self.tincanparams = None

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        frame               = cbt.data.get("dataframe")
        interface_name      = cbt.data["interface_name"]
        interface_details   = self.ipop_interface_details[interface_name]
        srcmac,destmac,srcip,destip = "","","",""

        if cbt.action == "getlocalmacaddress":
            self.ipop_interface_details[interface_name]["mac"] = cbt.data.get("localmac")
            return
        elif cbt.action == "RECV_PEER_MAC_DETAILS":
            self.registerCBT('Logger', 'info', "Inside Multicast Module Update Peer MAC details")
            self.registerCBT('Logger', 'info', "Multicast Message:: "+str(cbt.data))

            mac_ip_table  = cbt.data["mac_ip_table"]
            src_uid         = cbt.data["src_uid"]

            UpdateBTMMacUIDTable = {
                "uid"               : src_uid,
                "mac_ip_table"      : mac_ip_table,
                "interface_name"    : interface_name,
                "location"          : "remote",
                "type"              : "UpdateMACUIDIp"
            }
            self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', UpdateBTMMacUIDTable)
            return
        elif cbt.action=="ARP_PACKET":
            self.registerCBT('Logger', 'info', "Inside Multicast ARP module")
            self.registerCBT('Logger', 'debug', "Multicast Message::"+str(cbt.data))
            maclen      = int(frame[36:38],16)
            iplen       = int(frame[38:40],16)
            op          = int(frame[40:44],16)
            srcmacindex = 44 + 2 * maclen
            srcmac      = frame[44:srcmacindex]
            srcipindex  = srcmacindex + 2 * iplen
            srcip       =  '.'.join(str(int(i, 16)) for i in [frame[srcmacindex:srcipindex][i:i+2] for i in range(0, 8, 2)])
            destmacindex= srcipindex + 2 * maclen
            destmac     = frame[srcipindex:destmacindex]
            destipindex = destmacindex + 2 * iplen
            destip      = '.'.join(str(int(i, 16)) for i in [frame[destmacindex:destipindex][i:i+2] for i in range(0, 8, 2)])


        # TO DO Remove the below statements after development
        self.registerCBT('Logger', 'debug', "Source MAC:: "+ str(srcmac))
        self.registerCBT('Logger', 'debug', "Source ip::  " + str(srcip))
        self.registerCBT('Logger', 'debug', "Destination MAC:: " + str(destmac))
        self.registerCBT('Logger', 'debug', "Destination ip:: " + str(destip))

        current_node_uid = interface_details["uid"]
        # ARP Request Packet
        if op == 1:
            if cbt.data["type"] == "local":
                mac_ip_table = {}
                if int(srcmac,16) != 0:
                    interface_details["local_mac_ip_table"][srcmac] = srcip
                    mac_ip_table[srcmac] = srcip
                UpdateBTMMacUIDTable = {
                    "uid"         : current_node_uid,
                    "mac_ip_table": mac_ip_table,
                    "interface_name": interface_name,
                    "location": "local",
                    "type": "UpdateMACUIDIp"
                }

            else:
                uid = cbt.data["init_uid"]
                mac_ip_table = {}
                if int(srcmac, 16) != 0:
                    mac_ip_table[srcmac] = srcip

                UpdateBTMMacUIDTable = {
                    "uid"               : uid,
                    "mac_ip_table"      : mac_ip_table,
                    "interface_name"    : interface_name,
                    "location"          : "remote",
                    "type"              : "UpdateMACUIDIp"
                }

            # Broadcast the ARP Message using the Overlay
            if destip != self.ipop_interface_details[interface_name]["ip"]:
                self.registerCBT('BroadCastForwarder', 'multicast', cbt.data)
            elif destip == self.ipop_interface_details[interface_name]["ip"] and srcip == "0.0.0.0":
                self.registerCBT('BroadCastForwarder', 'multicast', cbt.data)
            elif destmac in list(self.ipop_interface_details[interface_name]["local_mac_ip_table"].keys()):
                self.registerCBT('TincanSender', 'DO_INSERT_DATA_PACKET', cbt.data)
            else:
                self.registerCBT('TincanSender', 'DO_INSERT_DATA_PACKET', cbt.data)
                self.registerCBT('BroadCastForwarder', 'multicast', cbt.data)
            # Update BTM MAC-UID-IP Tables
            self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', UpdateBTMMacUIDTable)
        else:
            if int(srcmac, 16) != 0:
                interface_details["local_mac_ip_table"][srcmac] = srcip
            self.registerCBT('BaseTopologyManager', 'TINCAN_PACKET', cbt.data)
            sendlocalmacdetails = {
                        "interface_name": interface_name,
                        "type"          : "local",
                        "src_uid"       : current_node_uid,
                        "dataframe"     : {
                                "src_uid"       : current_node_uid,
                                "src_node_mac"  : interface_details["mac"],
                                "mac_ip_table": interface_details["local_mac_ip_table"],
                                "message_type"  : "SendMacDetails"
                        }
            }
            self.registerCBT('Logger', 'debug', "Sending Local/Peer MAC details:: "+str(sendlocalmacdetails))
            self.registerCBT('BroadCastForwarder', 'BroadcastData', sendlocalmacdetails)

            # Use BTM TINCAN_PKT to route ARP Reply Message using OVERLAY





    def terminate(self):
        pass

    def timer_method(self):
        pass