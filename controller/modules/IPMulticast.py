# ipop-project
# Copyright 2016, University of Florida
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from controller.framework.ControllerModule import ControllerModule
import controller.framework.ipoplib as ipoplib


class IPMulticast(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(IPMulticast, self).__init__(CFxHandle, paramDict, ModuleName)
        # Table to store Multicast Group address details for virtual network intefaces configured in the ipop-config.json
        self.multicast_details = {}
        # Query CFX to get properties of virtual networks configured by the user
        tincanparams = self.CFxHandle.queryParam("TincanInterface", "Vnets")
        # Iterate across the virtual networks to get UID,IPv4, IPv6 and TAPName
        for k in range(len(tincanparams)):
            interface_name = tincanparams[k]["TapName"]
            self.multicast_details[interface_name] = {}
            self.multicast_details[interface_name]["uid"] = tincanparams[k]["UID"]
            self.multicast_details[interface_name]["ip4"] = tincanparams[k]["IP4"]
            #self.multicast_details[interface_name]["ip6"] = tincanparams[k]["ip6"]
            # Stores local node's mac address obtained from LinkManager
            self.multicast_details[interface_name]["mac"] = ""
            # Table to store Peer UID which has subscribed to a multicast address
            self.multicast_details[interface_name]["Group"] = {}
        tincanparams = None

    def initialize(self):
        # Iterate across the IPOP interface to extract local node MAC details
        for interface_name in self.multicast_details.keys():
            self.registerCBT("LinkManager", "GET_NODE_MAC_ADDRESS", {"interface_name": interface_name})
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    # Method to send multicast data as unicast messages to all the IPOP node UIDs subscribed
    # to the given multicast address
    def sendmulticastdata(self, dataframe, interface_name, multicast_address):
        self.registerCBT("Logger", "debug", "Multicast Data: {0}".format(str(dataframe)))
        # Check if there is an entry in Multicast table for the multicast group IP
        if multicast_address in self.multicast_details[interface_name]["Group"].keys():
            # Extract the subscriber UID list from the table
            multicast_dst_list = self.multicast_details[interface_name]["Group"][multicast_address]
            self.registerCBT("Logger", "info", "Multicast Candidate List: {0}".format(str(multicast_dst_list)))
            # Iterate across the subscriber list and send the multicast data as a unicast message
            for dst_uid in multicast_dst_list:
                new_msg = {
                    "msg_type": "forward",
                    "src_uid": self.multicast_details[interface_name]["uid"],
                    "dst_uid": dst_uid,
                    "interface_name": interface_name,
                    "datagram": dataframe
                }
                # Send the mutlicast data to BTM for forwarding
                self.registerCBT("BaseTopologyManager", "ICC_CONTROL", new_msg)

    # Method to construct Membership Report version 3 message for Membership Query request received by the node
    def buildmembershipreportv3(self, multicast_address_list, interface_name):
        igmpdata = ""   # Variable to store IGMP Data
        # Iterate across the Multicast group subscriber list
        for multicastaddress in multicast_address_list:
            # Append all IP address to the variable
            igmpdata += "04000000" + multicastaddress
        # Append the number of multicast addresses the node has subscribed in Hex format
        igmpdata = "{0:04x}".format(len(multicast_address_list)) + igmpdata
        # variable to store checksum for IGMP Data
        checksum = ipoplib.getchecksum("2200" + "0000" + igmpdata)
        # Append the Check sum to the IGMP data
        igmpdata = "2200" + checksum[2:] + "0000" + igmpdata
        packet_total_length = "{0:04x}".format(20 + len(igmpdata)/2)
        # Variable that stores Nodes IPv4 address as IPv4 source address and "224.0.0.22" as the destination addresss
        ipheader = ipoplib.ip4_a2hex(self.multicast_details["interface_name"]["ip4"])+"E0000016"
        # Get the checksum for IPv4 header
        checksum = ipoplib.getchecksum("45C0" + packet_total_length + "0000" + "0000" + "0102" + ipheader)
        # Create the ipv4 packet with the Membership Report data
        ip_packet = "45C0" + packet_total_length + "0000" + "0000" + "0102" + checksum[2:] + ipheader + igmpdata
        nodemac = self.multicast_details[interface_name]["mac"]
        # Append the Destination MAC which is always '01005E000016' and node MAC to create the Ethernet frame
        dataframe = "01005E000016"+nodemac+"0800"+ip_packet
        return dataframe

    # Method to construct Membership Report version 1 and 2 messages for Membership Query request received by the node
    def buildmembershipreportv1_2(self, multicast_address_list, interface_name, ip_version):
        if ip_version == "4":
            # Initialize the IGMPData variable with multicast group address
            igmpdata = multicast_address_list[0]
            # Get the checksum for the Multicast message
            checksum = ipoplib.getchecksum("1600" + igmpdata)
            # Append the checksum to the IGMPData
            igmpdata = "1600" + checksum[2:] + igmpdata
            packet_total_length = "{0:04x}".format(20 + len(igmpdata)/2)
            # Variable that stores Nodes IPv4 address as IPv4 source address and Multicast address as the destination addresss
            ipheader = ipoplib.ip4_a2hex(self.multicast_details["interface_name"]["ip4"]) + \
                       ipoplib.ip4_a2hex(multicast_address_list[0])
            # Get the checksum for IPv4 header
            checksum = ipoplib.getchecksum("45C0" + packet_total_length + "0000" + "0000" + "0102" + ipheader)
            ip_packet = "45C0" + packet_total_length + "0000" + "0000" + "0102" + checksum[2:] + ipheader + igmpdata
            nodemac = self.multicast_details[interface_name]["mac"]
            # Append the Destination MAC  and node MAC to create the Ethernet frame
            # Note: the Destination MAC's first 3 bytes would always be '01005E' and remaining bytes obtained from
            # the last 3 octets of the IPv4 address
            dataframe = "01005E" + multicast_address_list[0][2:] + nodemac + "0800" + ip_packet
        else:
            igmpdata = ""  # Variable to store IGMP Data
            # Iterate across the Multicast group subscriber list
            for multicastaddress in multicast_address_list:
                # Append all IP address to the variable
                igmpdata += "03000000" + multicastaddress
            # Append the number of multicast addresses the node has subscribed in Hex format
            igmpdata = "{0:04x}".format(len(multicast_address_list)) + igmpdata
            # variable to store checksum for IGMP Data
            checksum = ipoplib.getchecksum("8f00" + "0000" + igmpdata)
            # Append the Check sum to the IGMP data
            igmpdata = "8f00" + checksum[2:] + "0000" + igmpdata
            payload_length = "{0:04x}".format(len(igmpdata) / 2)
            # IPv6 header creation (Refer IPv6 Header spec to understand default values for various details)
            ipheader = "60000000"+ payload_length + "0001" + self.multicast_details["interface_name"]["ip6"] \
                       + "FF020000000000000000000000000016" + "3A00050200000100"
            # Create the ipv6 packet with the Membership Report data
            ip_packet = ipheader + igmpdata
            nodemac = self.multicast_details[interface_name]["mac"]
            # Append the Destination MAC, which is always '333300000016' and node MAC to create the Ethernet frame
            dataframe = "333300000016" + nodemac + "86DD" + ip_packet
        return dataframe

    def processCBT(self, cbt):
        interface_name = cbt.data["interface_name"]
        # Populate Local UID's MAC details. Data is send by LinkManager
        if cbt.action == "NODE_MAC_ADDRESS":
            # Check whether LinkManager has send a valid MAC Address if not request for MAC details again
            if cbt.data.get("localmac") != "":
                self.multicast_details[interface_name]["mac"] = cbt.data.get("localmac")
            else:
                self.registerCBT("LinkManager", "GET_NODE_MAC_ADDRESS", {"interface_name": interface_name})
            return
        elif cbt.action == "IPv4_MULTICAST":
            self.registerCBT("Logger", "debug", "Inside IPv4 Multicast:: {0}".format(str(cbt.data)))
            self.process_multicast_pkt(cbt, "4", interface_name)
        elif cbt.action == "IPv6_MULTICAST":
            self.registerCBT("Logger", "debug", "Inside IPv6 Multicast:: {0}".format(str(cbt.data)))
            self.process_multicast_pkt(cbt, "6", interface_name)
        else:
            log = '{0}: unrecognized CBT {1} received from {2}' \
                .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def process_multicast_pkt(self, cbt, ip_version, interface_name):
        dataframe = cbt.data.get("dataframe")
        # Check whether the IP Packet is version 4 or 6
        if ip_version == "4":
            protocol = dataframe[46:48]  # Variable to store IPv4 protocol (02- IGMP messages)
            # Extract IGMP message from IPv4 packet
            IGMPData = dataframe[28 + (int(dataframe[29:30]) * 4 * 2):]
            version = IGMPData[0:1]
            operation = IGMPData[1:2]
            bytelength = 8  # Variable to store IPv4 address length
        else:
            protocol = "02"  # Set default value for IPv6 Packet for code reuse
            # Extract IGMP message from IPv6 packet
            IGMPData = dataframe[124:]
            operation = IGMPData[0:2]
            bytelength = 32   # Variable to store IPv6 address length
            # Check IGMP operation to determine the IGMP version number
            if operation in ["83", "84"]:
                version = "1"
            elif operation in ["8F", "8f"]:
                version = "2"
            else:
                version = "3"
        # Check whether IP message contains IGMP as protocol
        if protocol == "02":
            # Check IGMP Membership Query message for IPv4 and IPv6 packets
            if operation in [ "1", "82"]:
                self.registerCBT("Logger", "info", "IGMP Group Membership Query message received")
                self.registerCBT("Logger", "debug", "Multicast Table::{0}".
                                 format(str(self.multicast_details[interface_name])))
                # Extract multicast group address from the IGMP data
                if ip_version == "4":
                    multicast_address = IGMPData[8:8 + bytelength]
                else:
                    multicast_address = IGMPData[16:16 + bytelength]
                # Check if source of the Packet is the local network interface
                if cbt.data.get("type") == "local":
                    msg = {
                        "interface_name": interface_name,
                        "dataframe": dataframe,
                        "type": "local"
                    }
                    # Broadcast the MembershipQuery packet to all IPOP nodes in the network
                    self.registerCBT("BroadcastForwarder", "BroadcastPkt", msg)
                    # Check whether message is a general membership query or not
                    if multicast_address in ["0" * bytelength] + self.multicast_details[interface_name].keys() and \
                                    self.multicast_details[interface_name]["mac"] not in [None, ""]:
                        # Check whether message is a general membership query or not
                        if multicast_address == "0" * bytelength:
                            multicast_address_list = self.multicast_details[interface_name].keys()
                        else:
                            multicast_address_list = [multicast_address]

                        if ip_version == "4":
                            # Check IGMP version of Membership is it 1,2 or not
                            if len(IGMPData) == 16:
                                # Create multiple MembershipReport messages for IGMP Version 1 and 2 as it does not
                                # support multiple multicast addresses into a single MembershipReport message
                                for multicast_address in multicast_address_list:
                                    report_dataframe = self.buildmembershipreportv1_2([multicast_address], interface_name, "4")
                                    self.registerCBT("TincanInterface", "DO_INSERT_DATA_PACKET", {
                                        "dataframe": report_dataframe,
                                        "interface_name": interface_name
                                    })
                            else:
                                # Insert IGMPv3 IPv4 Membership Report into the IPOP Tap
                                report_dataframe = self.buildmembershipreportv3(multicast_address_list, interface_name)
                                self.registerCBT("TincanInterface", "DO_INSERT_DATA_PACKET", {
                                    "dataframe": report_dataframe,
                                    "interface_name": interface_name
                                })
                        else:
                            # Insert the IPv6 Membership Report into the IPOP Tap
                            report_dataframe = self.buildmembershipreportv1_2(multicast_address_list, interface_name, "6")
                            self.registerCBT("TincanInterface", "DO_INSERT_DATA_PACKET", {
                                "dataframe": report_dataframe,
                                "interface_name": interface_name
                            })
                else:
                    dst_uid = cbt.data.get("init_uid")
                    # Check whether message is a general membership query or not
                    if multicast_address in ["0" * bytelength] + self.multicast_details[interface_name].keys() and \
                                    self.multicast_details[interface_name]["mac"] not in [None, ""]:
                        # Check whether message is a general membership query or not
                        if multicast_address == "0" * bytelength:
                            multicast_address_list = self.multicast_details[interface_name].keys()
                        else:
                            multicast_address_list = [multicast_address]

                        if ip_version == "4":
                            # Check IGMP version of Membership is it 1,2 or not
                            if len(IGMPData) == 16:
                                # Create multiple MembershipReport messages for IGMP Version 1 and 2 as it does not
                                # support multiple multicast addresses into a single MembershipReport message
                                for multicast_address in multicast_address_list:
                                    report_dataframe = self.buildmembershipreportv1_2([multicast_address], interface_name, "4")
                                    new_msg = {
                                        "msg_type": "forward",
                                        "src_uid": self.multicast_details[interface_name]["uid"],
                                        "dst_uid": dst_uid,
                                        "interface_name": interface_name,
                                        "datagram": report_dataframe
                                    }
                                    self.registerCBT("BaseTopologyManager", "ICC_CONTROL", new_msg)
                            else:
                                report_dataframe = self.buildmembershipreportv3(multicast_address_list, interface_name)
                                new_msg = {
                                    "msg_type": "forward",
                                    "src_uid": self.multicast_details[interface_name]["uid"],
                                    "dst_uid": dst_uid,
                                    "interface_name": interface_name,
                                    "datagram": report_dataframe
                                }
                                # Send Membership Report as unicast to the Source Node
                                self.registerCBT("BaseTopologyManager", "ICC_CONTROL", new_msg)
                        else:
                            report_dataframe = self.buildmembershipreportv1_2(multicast_address_list, interface_name, "6")
                            new_msg = {
                                "msg_type": "forward",
                                "src_uid": self.multicast_details[interface_name]["uid"],
                                "dst_uid": dst_uid,
                                "interface_name": interface_name,
                                "datagram": report_dataframe
                            }
                            # Send Membership Report as unicast to the Source Node
                            self.registerCBT("BaseTopologyManager", "ICC_CONTROL", new_msg)
            # IGMP Membership Report/Leave Group Message
            elif operation in ["2", "6", "7", "83", "84", "8F", "8f"]:
                self.registerCBT("Logger", "info", "IGMP Membership Report packet received")
                # Check whether the data is from local tap or remote node
                if cbt.data.get("type") == "remote":
                    multicast_src_uid = cbt.data.get("init_uid")
                    # Check the IGMP Version
                    if version == "1":
                        multicast_address = dataframe[-8:]
                        # Convert IPv4 address from hex to ASCII format
                        multicast_add = '.'.join(
                            str(int(i, 16)) for i in [multicast_address[i:i + 2] for i in range(0, 8, 2)])
                        self.registerCBT("Logger", "debug", "Multicast Address::" + str(multicast_add))
                        # Check whether there exists an entry in the Table for the Multicast group address
                        if multicast_address in self.multicast_details[interface_name]["Group"].keys():
                            # IGMP Membership Report
                            if operation in ["2", "6"]:
                                # Append the UID into the subscriber list for multicast address
                                self.multicast_details[interface_name]["Group"][multicast_address]. \
                                    append(multicast_src_uid)
                                self.multicast_details[interface_name]["Group"][multicast_address] = \
                                    list(set(self.multicast_details[interface_name]["Group"][multicast_address]))
                            # IGMP Leave Group Message
                            else:
                                # Remove UID from the subscriber list of the Multicast Table
                                if multicast_src_uid in self.multicast_details[interface_name]["Group"][
                                    multicast_address]:
                                    self.multicast_details[interface_name]["Group"][multicast_address].remove(
                                        multicast_src_uid)
                        else:
                            # Create a new entry only in case of MembershipReport data packet
                            if operation in ["2", "6"]:
                                self.multicast_details[interface_name]["Group"][multicast_address] = [multicast_src_uid]
                    else:
                        # Packet corresponds to IPv4 IGMPv3 Membership Report or IPv6 IGMPv2 that contains
                        # multiple multicast address group details
                        noofgroups = int(IGMPData[12:16])  # Extract number of multicast address group in the message
                        multicastrecords = IGMPData[16:]
                        # Iterate across the multicast group to extract multicast address group record
                        i=0
                        # Iterate across the entire multicastgroup record
                        while i < len(multicastrecords):
                            # Extract the Multicast address from the record
                            recordtype = multicastrecords[i: i + 2]
                            aux_data_length = int(multicastrecords[i + 2: i + 4],16)
                            no_of_sources = int(multicastrecords[i + 4: i + 8],16)
                            multicast_address = multicastrecords[i + 8: i + 8+ bytelength]
                            i += 8 + bytelength + (no_of_sources * bytelength) + aux_data_length
                            # Check whether an entry exists for Multicast address in the Table, if NO create
                            # a new entry else append the source UID to the multicast address group subscriber list
                            if multicast_address in self.multicast_details[interface_name]["Group"].keys():
                                self.multicast_details[interface_name]["Group"][multicast_address].append(
                                    multicast_src_uid)
                                self.multicast_details[interface_name]["Group"][multicast_address] = \
                                    list(set(self.multicast_details[interface_name]["Group"][multicast_address]))
                            else:
                                self.multicast_details[interface_name]["Group"][multicast_address] = [multicast_src_uid]
                    self.registerCBT("BroadcastForwarder", "BroadcastPkt", cbt.data)
                else:
                    # MembershipReport obtained from an unmanaged node, route it to all other nodes in the network
                    msg = {
                        "interface_name": interface_name,
                        "dataframe": dataframe,
                        "type": "local"
                    }
                    # The message has originated from the local Tap interface send it to remaining nodes in the IPOP network
                    self.registerCBT("BroadcastForwarder", "BroadcastPkt", msg)
                self.registerCBT("Logger", "debug",
                                 "Multicast Table: {0}".format(str(self.multicast_details[interface_name])))
            else:
                # IP Packet is Multicast data packet send it to all the UIDs subscribed to the Multicast address
                if ip_version == "4":
                    multicast_address = IGMPData[8:16]
                else:
                    multicast_address = dataframe[76:108]
                self.sendmulticastdata(dataframe, interface_name, multicast_address)
        else:
            # IP Packet is Multicast data packet send it to all the UIDs subscribed to the Multicast address
            multicast_address = dataframe[60:68]
            self.sendmulticastdata(dataframe, interface_name, multicast_address)

    def timer_method(self):
        pass

    def terminate(self):
        pass
