from controller.framework.ControllerModule import ControllerModule


class ArpCache(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(ArpCache, self).__init__(CFxHandle, paramDict, ModuleName)
        # Query CFX to get properties of virtual networks configured by the user
        self.tincanparams = self.CFxHandle.queryParam("TincanInterface", "Vnets")
        self.ipop_vnets_details = {}
        # Iterate across the virtual networks to get UID,IP4 and TAPName
        for k in range(len(self.tincanparams)):
            interface_name = self.tincanparams[k]["TapName"]
            self.ipop_vnets_details[interface_name] = {}
            interface_detail = self.ipop_vnets_details[interface_name]
            interface_detail["uid"] = self.tincanparams[k]["UID"]
            interface_detail["ip"] = self.tincanparams[k]["IP4"]
            # Table to store Unmanaged Node MAC Address and IPV4 details
            interface_detail["local_mac_ip_table"] = {}
            # Stores local node's mac address obtained from LinkManager
            interface_detail["mac"] = ""
        # Clear the copy of network details from CFX after loading
        self.tincanparams = None

    def initialize(self):
        # Iterate across the IPOP interface to extract local node MAC details
        for interface_name in list(self.ipop_vnets_details.keys()):
            self.registerCBT("LinkManager", "GET_NODE_MAC_ADDRESS", {"interface_name": interface_name})
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        frame = cbt.data.get("dataframe")
        interface_name = cbt.data["interface_name"]
        interface_details = self.ipop_vnets_details[interface_name]
        srcmac, destmac, srcip, destip, op = "", "", "", "", 0

        # Populate Local UID's MAC details. Data is send by LinkManager
        if cbt.action == "NODE_MAC_ADDRESS":
            # Check whether LinkManager has send a valid MAC Address if not request for MAC details again
            if cbt.data.get("localmac") != "":
                self.ipop_vnets_details[interface_name]["mac"] = cbt.data.get("localmac")
            else:
                self.registerCBT("LinkManager", "GET_NODE_MAC_ADDRESS", {"interface_name": interface_name})
            return
        # Process UID-MAC-IP details from other nodes in the network
        elif cbt.action == "PeerMACIPDetails":
            self.registerCBT('Logger', 'debug', "Remote node Unmanaged node details: {0}".format(str(cbt.data)))
            mac_ip_table = cbt.data["mac_ip_table"]
            src_uid = cbt.data["src_uid"]
            # Message for BTM to update the master UID-MAC-IP Tables
            UpdateBTMMacUIDTable = {
                "uid": src_uid,
                "mac_ip_table": mac_ip_table,
                "interface_name": interface_name,
                "location": "remote"
            }
            self.registerCBT('BaseTopologyManager', 'UPDATE_MAC_UID_IP_TABLES', UpdateBTMMacUIDTable)
            return
        # Process ARP Packets received
        elif cbt.action == "ARPPacket":
            self.registerCBT('Logger', 'debug', "ARP Packet: {0}".format(str(cbt.data)))
            # Variables to store length of MACAddress and IPV4 Address
            maclen = int(frame[36:38], 16)
            iplen = int(frame[38:40], 16)
            # Variable to store operation 1- ARP Request 2- ARP Reply
            op = int(frame[40:44], 16)
            srcmacindex = 44 + 2 * maclen
            srcmac = frame[44:srcmacindex]
            srcipindex = srcmacindex + 2 * iplen
            # Converting Source IPV4 address in hex format to ASCII format (XXX.XXX.XXX.XXX)
            srcip = '.'.join(str(int(i, 16)) for i in [frame[srcmacindex:srcipindex][i:i + 2] for i in range(0, 8, 2)])
            destmacindex = srcipindex + 2 * maclen
            destmac = frame[srcipindex:destmacindex]
            destipindex = destmacindex + 2 * iplen
            # Converting Destination IPV4 address in hex format to ASCII format (XXX.XXX.XXX.XXX)
            destip = '.'.join(str(int(i, 16)) for i in [frame[destmacindex:destipindex][i:i + 2] for i in range(0, 8, 2)])

            self.registerCBT('Logger', 'debug', "Source MAC:: " + str(srcmac))
            self.registerCBT('Logger', 'debug', "Source IP Address::  " + str(srcip))
            self.registerCBT('Logger', 'debug', "Destination MAC:: " + str(destmac))
            self.registerCBT('Logger', 'debug', "Destination IP Address:: " + str(destip))
        local_uid = interface_details["uid"]
        # ARP Request Packet
        if op == 1:
            # Check whether ARP Message is from local unmanaged nodes
            if cbt.data["type"] == "local":
                mac_ip_table = {}
                # Update Local MAC-IP Table with Unmanaged node MAC and IP details
                if int(srcmac, 16) != 0:
                    interface_details["local_mac_ip_table"][srcmac] = srcip
                    mac_ip_table[srcmac] = srcip
                UpdateBTMMacUIDTable = {
                    "uid": local_uid,
                    "mac_ip_table": mac_ip_table,
                    "interface_name": interface_name,
                    "location": "local"
                }
            else:
                uid = cbt.data["init_uid"]          # Get the remote control UID
                mac_ip_table = {}
                if int(srcmac, 16) != 0:
                    mac_ip_table[srcmac] = srcip

                UpdateBTMMacUIDTable = {
                    "uid": uid,
                    "mac_ip_table": mac_ip_table,
                    "interface_name": interface_name,
                    "location": "remote"
                }
            # Update BTM MAC-UID-IP Tables
            self.registerCBT('BaseTopologyManager', 'UPDATE_MAC_UID_IP_TABLES', UpdateBTMMacUIDTable)

            # Broadcast the ARP Message using the Overlay
            if destip != self.ipop_vnets_details[interface_name]["ip"]:
                self.registerCBT('BroadcastForwarder', 'BroadcastPkt', cbt.data)
            elif destip == self.ipop_vnets_details[interface_name]["ip"] and srcip == "0.0.0.0":
                self.registerCBT('BroadcastForwarder', 'BroadcastPkt', cbt.data)
            elif destmac in list(self.ipop_vnets_details[interface_name]["local_mac_ip_table"].keys()):
                self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', cbt.data)
            else:
                self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', cbt.data)
                self.registerCBT('BroadcastForwarder', 'BroadcastPkt', cbt.data)
        # ARP Reply Packet: Send ARP Reply as unicast to the source and Broadcast local MAC-IP
        # Table for setting up routing rules in the Tincan
        else:
            if int(srcmac, 16) != 0:
                interface_details["local_mac_ip_table"][srcmac] = srcip
            # Send ARP Reply as a Unicast packet
            self.registerCBT('BaseTopologyManager', 'TINCAN_PACKET', cbt.data)
            # Message format to send Unmanaged node MAC-IP details
            sendlocalmacdetails = {
                        "interface_name": interface_name,
                        "type": "local",
                        "src_uid": local_uid,
                        "dataframe": {
                                "src_uid": local_uid,
                                "src_node_mac": interface_details["mac"],
                                "mac_ip_table": interface_details["local_mac_ip_table"],
                                "message_type": "SendMacDetails"
                        }
            }
            # Broadcast Unmanaged node details to all nodes in the network
            self.registerCBT('BroadcastForwarder', 'BroadcastData', sendlocalmacdetails)

    def terminate(self):
        pass

    def timer_method(self):
        pass
