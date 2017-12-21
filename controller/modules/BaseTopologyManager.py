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
from controller.framework.CFx import CFX
import time
import math


class BaseTopologyManager(ControllerModule, CFX):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(BaseTopologyManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.CFxHandle = CFxHandle
        # BTM internal Table
        self.ipop_vnets_details = {}
        # Query CFX to get properties of virtual networks configured by the user
        tincanparams = self.CFxHandle.queryParam("TincanInterface", "Vnets")
        # Iterate across the virtual networks to get XMPPModuleName and TAPName
        for k in range(len(tincanparams)):
            interface_name = tincanparams[k]["TapName"]
            self.ipop_vnets_details[interface_name] = {}
            vnet_details = self.ipop_vnets_details[interface_name]
            vnet_details["p2p_state"] = "started"
            vnet_details["GeoIP"] = ""
            vnet_details["ipop_state"] = {}
            vnet_details["discovered_nodes"] = []
            vnet_details["successor"] = {}
            vnet_details["ip_uid_table"] = {}
            vnet_details["uid_mac_table"] = {}
            vnet_details["mac_uid_table"] = {}
            vnet_details["link_type"] = {}
            vnet_details["peer_uid_sendmsgcount"] = {}
            vnet_details["xmpp_client_code"] = tincanparams[k]["XMPPModuleName"]
        tincanparams = None

    def initialize(self):
        # Iterate across different TapInterface to initialize BTM table attributes
        for interface_name in self.ipop_vnets_details.keys():
            # Invoke Tincan to get Local node state
            self.registerCBT('TincanInterface', 'DO_GET_STATE', {"interface_name": interface_name, "MAC": ""})
            self.CFxHandle.StartSubscription(self.ipop_vnets_details[interface_name]["xmpp_client_code"], "PEER_PRESENCE_NOTIFICATION")

        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))
        self.timer_method()

    def terminate(self):
        pass

    # Method to create all outbound links from the Node
    def add_outbound_link(self, link_type, uid, interface_name):
        self.registerCBT("LinkManager", "CREATE_LINK", {"uid": uid, "interface_name": interface_name})
        if uid not in self.ipop_vnets_details[interface_name]["link_type"].keys():
            self.ipop_vnets_details[interface_name]["link_type"].update({uid: link_type})

    # remove connection
    # remove a link by peer UID
    #def remove_link(self, uid, interface_name, link=None):
    #    if link is None:
    #        connection_type_list = ["successor"]
    #    else:
    #        connection_type_list = [link]

    #    for link_type in connection_type_list:
    #        if uid in self.ipop_vnets_details[interface_name][link_type].keys():
    #            self.ipop_vnets_details[interface_name][link_type].pop(uid)
    #            message = {"uid": uid, "interface_name": interface_name}
    #            self.registerCBT("LinkManager", "REMOVE_LINK", message)
    #            log = "Connection remove request for UID: {0}".format(uid)
    #            self.registerCBT('Logger', 'info', log)

    def add_successors(self, interface_name):
        vnet_details = self.ipop_vnets_details[interface_name]
        my_uid = vnet_details["ipop_state"]["_uid"]
        for node in vnet_details["discovered_nodes"]:
            if my_uid != node:
                self.add_outbound_link("successor", node, interface_name)

    # Sets GEO Location IP (needed by Visualizer Module)
    def setGeoIP(self, interface_name, cas):
        try:
            casdetails = str(cas).split(":")
            for i, ele in enumerate(casdetails):
                if str(ele).count(".") == 3:
                    if casdetails[i - 1] == "udp" and casdetails[i + 5] == "stun":
                        ip_octet = str(ele).split(".")
                        if ip_octet[0] == "10":
                            pass
                        elif ip_octet[0] == "172" and ip_octet[1] in range(16, 32, 1):
                            pass
                        elif ip_octet[0] == "192" and ip_octet == "168":
                            pass
                        else:
                            self.ipop_vnets_details[interface_name]["GeoIP"] = ele
        except Exception as err:
            self.registerCBT("Logger", "error", "Error while Setting GeoIP:{0}".format(err))

    def processCBT(self, cbt):
        msg = cbt.data
        msg_type = msg.get("type", None)
        interface_name = msg["interface_name"]
        vnet_details = self.ipop_vnets_details[interface_name]

        if cbt.action == "PEER_PRESENCE_NOTIFICATION":
            self.registerCBT('Logger', 'debug', "RECEIVED PEER NOTIFICATION FROM XMPP")
            peer_uid = msg["uid_notification"]
            interface_name = msg["interface_name"]
            self.add_outbound_link("successor", peer_uid, interface_name)
            self.registerCBT('Logger', 'debug', "attempting to create outbound link to {}".format(peer_uid))

        # CBT to process peerlist from Signal module
        elif cbt.action == "UPDATE_XMPP_PEERLIST":
            xmpp_peer_list = msg.get("peer_list")
            if len(xmpp_peer_list) > 0:
                vnet_details["discovered_nodes"] += xmpp_peer_list
                vnet_details["discovered_nodes"] = list(set(vnet_details["discovered_nodes"]))
            else:
                vnet_details["discovered_nodes"] = []
            self.registerCBT(vnet_details["xmpp_client_code"], "GET_XMPP_PEERLIST", {"interface_name": interface_name})
        elif cbt.action == "FORWARD_MSG":
            #pass
            self.forward_msg(msg["fwd_type"], msg["dst_uid"], msg["data"], interface_name)
        # CBT to process p2p link state details from LinkManager
        elif cbt.action == "RETRIEVE_LINK_DETAILS":
            data = msg.get("data")
            current_links = vnet_details["link_type"].keys()
            updated_links = data.keys()
            # If length of current_links is equal to updated_links it means no link got dropped
            if len(current_links) == len(updated_links):
                # Update Link details (E.g TTL, Status)
                for peeruid in current_links:
                    vnet_details[vnet_details["link_type"][peeruid]].update({peeruid: data[peeruid]})
            else:
                # Extract nodes in current_links not present in the updated_links. These are the deleted links
                deleted_links = set(current_links) - set(updated_links)
                for peeruid in deleted_links:
                    # Deleted the Peer UID from BTM's link table
                    if peeruid in vnet_details[vnet_details["link_type"][peeruid]]:
                        del vnet_details[vnet_details["link_type"][peeruid]][peeruid]
                    if peeruid in vnet_details["uid_mac_table"]:
                        # Extract unmanaged nodes behind the Peer UID
                        unmanaged_node_mac_list = vnet_details["uid_mac_table"][peeruid]
                        # Deleted the Peer UID entry from the UID_MAC_TABLE
                        del vnet_details["uid_mac_table"][peeruid]
                        # Iterate across the unmanaged node mac list and remove it from MAC_UID Table
                        for node_mac in unmanaged_node_mac_list:
                            del vnet_details["mac_uid_table"][node_mac]
                    # Iterate across IP_UID Table and remove all keys whose value is the Peer UID
                    for ip, uid in list(vnet_details["ip_uid_table"].items()):
                        if uid == peeruid:
                            del vnet_details["ip_uid_table"][ip]
                    # Delete the entry from Peer UID sent msg table
                    if peeruid in vnet_details["peer_uid_sendmsgcount"]:
                        del vnet_details["peer_uid_sendmsgcount"][peeruid]
        elif cbt.action == "XMPP_MSG":
            # Remove Offline peer node from Discovered node List
            if msg_type == "offline_peer":
                if msg["uid"] in vnet_details["discovered_nodes"]:
                    vnet_details["discovered_nodes"].remove(msg["uid"])
                log = "Removed peer from discovered node list {0}".format(msg["uid"])
                self.registerCBT('Logger', 'debug', log)
            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                self.registerCBT('Logger', 'warning', log)
        elif cbt.action == "TINCAN_RESPONSE":
            # update local state into BTM table
            if msg_type == "local_state":
                vnet_details["ipop_state"] = msg
                vnet_details["mac"] = msg["mac"]
                vnet_details["mac_uid_table"][msg["mac"]] = msg["_uid"]
                if msg["_uid"] not in vnet_details["uid_mac_table"].keys():
                    vnet_details["uid_mac_table"][msg["_uid"]] = [msg["mac"]]
            else:
                self.setGeoIP(interface_name, msg["cas"])
        elif cbt.action == "UPDATE_MAC_UID_IP_TABLES":
            location = msg.get("location")
            uid = msg["uid"]
            localuid = vnet_details["ipop_state"]["_uid"]

            # check whether an entry exists for UID, if NOT create an entry in UID_MAC Table
            if uid not in list(vnet_details["uid_mac_table"].keys()):
                vnet_details["uid_mac_table"][uid] = []

            self.registerCBT('Logger', 'debug', 'UpdateMACUIDMessage:::' + str(msg))
            # Update the IP_UID and MAC_UID Table with the Unmanaged node details
            for mac, ip in msg["mac_ip_table"].items():
                if mac not in vnet_details["uid_mac_table"][uid]:
                    vnet_details["uid_mac_table"][uid].append(mac)
                    vnet_details["ip_uid_table"].update({ip: uid})
                vnet_details["mac_uid_table"].update({mac: uid})
        elif cbt.action == "ICC_CONTROL":
            msg_type = msg.get("msg_type", None)
            # advertisement of nearby nodes
            if msg_type == "advertise":
                vnet_details["discovered_nodes"] = list(set(vnet_details["discovered_nodes"] + msg["peer_list"]))
                localuid = vnet_details["ipop_state"]["_uid"]
                if localuid in vnet_details["discovered_nodes"]:
                    vnet_details["discovered_nodes"].remove(localuid)
                log = "Received p2p link advertisement from node UID: {0}".format(msg["src_uid"])
                self.registerCBT('Logger', 'info', log)
            # handle forward packet
            elif msg_type == "forward":
                dst_uid = msg["dst_uid"]
                # Check whether the current node is the intended recipient of the message
                if dst_uid != vnet_details["ipop_state"]["_uid"]:
                    self.forward_msg("exact", msg["dst_uid"], msg, interface_name)
                else:
                    msg["interface_name"] = interface_name
                    # Check whether the forwarded message is a network packet,
                    # If YES insert it into the local network interface
                    if "datagram" in msg.keys():
                        data = msg.pop("datagram")
                        msg["dataframe"] = data
                        self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', msg)
            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                self.registerCBT('Logger', 'warning', log)
        elif cbt.action == "GET_VISUALIZER_DATA":
            for interface_name in self.ipop_vnets_details.keys():
                vnet_details = self.ipop_vnets_details[interface_name]
                local_uid, local_ip = "", ""
                if vnet_details["p2p_state"] == "connected" and "ipop_state" in vnet_details.keys():
                    local_uid = vnet_details["ipop_state"]["uid"]
                    local_ip = vnet_details["ipop_state"]["ip4"]
                successors = []

                # Iterate over the IP-UID Table to retrieve Unmanaged node IP list
                for ip, uid in list(vnet_details["ip_uid_table"].items()):
                    # check whether the IP is that of the local node
                    if ip != local_ip and uid == local_uid and ip != "0.0.0.0":
                        unmanaged_node_list.append(ip)
                # Extract the online successor list from the BTM Table
                for successor in list(vnet_details["successor"].keys()):
                    if "status" in vnet_details["successor"][successor].keys():
                        if vnet_details["successor"][successor]["status"] == "online":
                            successors.append(successor)

                # Check if GEO IP exists else invoke the function to retrieve the details from Public Stun server
                if vnet_details["GeoIP"] in ["", None]:
                    geoip = ""
                    #vnet_details["GeoIP"] = geoip
                else:
                    geoip = vnet_details["GeoIP"]

                # Message for Overlay visualizer
                new_msg = {
                    "interface_name": interface_name,
                    "uid": local_uid,
                    "ip4": local_ip,
                    "GeoIP": geoip,
                    "mac": vnet_details["mac"],
                    "state": vnet_details["p2p_state"],
                    "macuidmapping": vnet_details["uid_mac_table"],
                    #"unmanagednodelist": unmanaged_node_list,
                    "links": {
                        "successor": successors,
                        "chord": [],
                        "on_demand": []
                    }
                }
                self.registerCBT("OverlayVisualizer", "TOPOLOGY_DETAILS", new_msg)
        # handle and forward tincan data packets
        elif cbt.action == "TINCAN_PACKET":
            reqdata = cbt.data
            data = reqdata["dataframe"]
            m_type = reqdata["m_type"]
            # ignore packets when not connected to the overlay
            if vnet_details["p2p_state"] != "connected":
                return
            # Check the Packet type whether it is an ARP or IP packet and extract destination IP and MAC for routing
            if m_type == "ARP":
                maclen = int(data[36:38], 16)
                iplen = int(data[38:40], 16)
                srcmacindex = 44 + 2 * maclen
                srcmac = data[44:srcmacindex]
                srcipindex = srcmacindex + 2 * iplen
                destmacindex = srcipindex + 2 * maclen
                destmac = data[srcipindex:destmacindex]
                destipindex = destmacindex + 2 * iplen
                dst_ip = '.'.join(str(int(i, 16))
                                  for i in [data[destmacindex:destipindex][i:i + 2] for i in range(0, 8, 2)])
            else:
                # Check whether the packet is IPv4 or IPv6
                if data[24:28] == "0800":
                    dst_ip = '.'.join(str(int(i, 16)) for i in [data[60:68][i:i + 2] for i in range(0, 8, 2)])
                else:
                    dst_ip = data[76: 108]
                destmac, srcmac = data[0:12], data[12:24]

            ip4_uid_table = vnet_details["ip_uid_table"]
            # If the destination IP exists in IP_UID_Table, if YES get the UID and send the message to the Peer
            if dst_ip in list(ip4_uid_table.keys()):
                dst_uid = ip4_uid_table[dst_ip]
            # If the destination MAC exists in MAC_UID_Table, if YES get the UID and send the message to the Peer
            elif destmac in vnet_details["mac_uid_table"].keys():
                dst_uid = vnet_details["mac_uid_table"][destmac]
            # Check if it is an IPv4 Multicast packet
            elif destmac[0:6] == "01005E":
                self.registerCBT("BroadcastForwarder", "BroadcastPkt", {"dataframe": data, "interface_name": interface_name,
                                                                   "type": "local"})
                return
            # Check if it is an IPv6 Multicast packet
            elif destmac[0:4] == "3333":
                self.registerCBT("BroadcastForwarder", "BroadcastPkt", {"dataframe": data, "interface_name": interface_name,
                                                                   "type": "local"})
                return
            # Packet is broadcast packet send it to Broadcast module
            elif destmac == "FFFFFFFFFFFF":
                datapacket = {
                    "dataframe": data,
                    "interface_name": interface_name,
                }
                # Check whether Packet has been generated from the local network interface
                if reqdata.get("type") == "remote":
                    datapacket["type"] = "remote"
                else:
                    datapacket["type"] = "local"
                # Route the packet to Broadcast module for broadcasting
                self.registerCBT("BroadcastForwarder", "BroadcastPkt", datapacket)
                return
            else:
                log = "recv illegal tincan_packet: src={0} dst={1}".format(srcmac, destmac)
                self.registerCBT('Logger', 'info', log)
                return
            # Message routing to one of the local node attached to this UID
            if dst_uid == vnet_details["ipop_state"]["_uid"]:
                network_inject_message = {
                    "dataframe": data,
                    "interface_name": interface_name
                }
                self.registerCBT("TincanInterface", "DO_INSERT_DATA_PACKET", network_inject_message)
                return
            ## send forwarded message
            new_msg = {
                "msg_type": "forward",
                "src_uid": vnet_details["ipop_state"]["_uid"],
                "dst_uid": dst_uid,
                "datagram": data
            }
            self.forward_msg("exact", dst_uid, new_msg, interface_name)


            log = "sent tincan_packet (exact): {0}. Message: {1}".format(dst_uid, data)
            self.registerCBT('Logger', 'info', log)
        else:
            log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
            self.registerCBT('Logger', 'warning', log)

############################################################################
            # packet forwarding policy #
############################################################################
    # closer function
    # tests if uid is successively closer to uid_B than uid_A
    def closer(self, uid_A, uid, uid_B):
        if (uid_A < uid_B) and ((uid_A < uid) and (uid <= uid_B)):
            return True  # 0---A===B---N
        elif (uid_A > uid_B) and ((uid_A < uid) or (uid <= uid_B)):
            return True  # 0===B---A===N
        return False

    # forward packet
    #   forward a packet across ICC
    #   - fwd_type = {
    #       exact   = intended specifically to the destination node,
    #       closest = intended to the node closest to the designated node
    #     }
    #   - dst_uid  = UID of the destination or designated node
    #   - msg      = message in transit
    #   returns true if this packet is intended for the calling node

    def forward_msg(self, fwd_type, dst_uid, msg, interface_name):
        # find peer that is successively closest to and less-than-or-equal-to the designated UID
        vnet_details = self.ipop_vnets_details[interface_name]
        uid = vnet_details["ipop_state"]["_uid"]
        nxt_uid = uid
        online_peer_list = list(vnet_details["successor"].keys())# + list(vnet_details["chord"].keys()) +\
            #list(vnet_details["on_demand"].keys())
        # Iterate across the Peer List
        for peer in sorted(online_peer_list):
            # Check if the link to Peer is Online else dont forward the message to the Peer UID
            if self.is_link_connected(peer, interface_name):
                # Check whether the Peer is the dst_uid of the message, If YES terminate the loop
                # and set the UID as the next_uid
                if peer == dst_uid:
                    nxt_uid = peer
                    break
                # Check whether the UID is closest to the destination node
                if self.closer(uid, peer, dst_uid):
                    nxt_uid = peer

        # packet is intended specifically to the destination node
        if fwd_type == "exact":
            # this is the destination uid
            if dst_uid == uid:
                return True
            # this is the closest node but not the destination; drop packet
            elif nxt_uid == uid:
                # check if atleast one online peer exists
                if len(online_peer_list) > 0:
                    nxt_uid = max(online_peer_list)
                else:
                    return False
        # packet is intended to the node closest to the designated node
        elif fwd_type == "closest":
            if nxt_uid == uid:
                return True
        # Send the message to LinkManager to update message with Peer MAC Address from its tables
        self.registerCBT("LinkManager", "SEND_ICC_MSG", {
                         "dst_uid": nxt_uid, "msg": msg, "interface_name": interface_name})
        return False

    # Checks if the link to Peer UID is connected
    def is_link_connected(self, uid, interface_name):
        # Checks whether the Peer UID exists in link_type Table
        if uid in self.ipop_vnets_details[interface_name]["link_type"].keys():
            # Extract the link type for the UID (Successor, Chord, On-Demand)
            link_type = self.ipop_vnets_details[interface_name]["link_type"][uid]
            if uid in self.ipop_vnets_details[interface_name][link_type].keys():
                if "status" in self.ipop_vnets_details[interface_name][link_type][uid].keys():
                    if self.ipop_vnets_details[interface_name][link_type][uid]["status"] == "online":
                        return True
        return False

############################################################################
    # manage topology #
############################################################################

    def manage_topology(self, interface_name):
        vnet_details = self.ipop_vnets_details[interface_name]
        # Extract all the peer UIDs seen by the node
        online_peer_list = list(vnet_details["successor"].keys())

        # connecting or connected to the IPOP peer-to-peer network
        if vnet_details["p2p_state"] == "connected":
            # manage successors
            self.add_successors(interface_name)
            # self.remove_successors(interface_name)
            # Iterate across all the p2p links created by the node
            for peer in sorted(online_peer_list):
                # Check if atleast a link is in Online State
                if self.is_link_connected(peer, interface_name):
                    vnet_details["p2p_state"] = "connected"
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: CONNECTED")
                    return
            vnet_details["p2p_state"] = "connecting"
            self.registerCBT('Logger', 'info', interface_name + " p2p state: RECONNECTING")

        if vnet_details["p2p_state"] == "started":
            if not vnet_details["ipop_state"]:
                self.registerCBT('Logger', 'info', interface_name + " P2P State: STARTED")
                return
            else:
                vnet_details["p2p_state"] = "searching"
                log = "IPOP local state: {0}".format(vnet_details["ipop_state"]["_uid"])
                self.registerCBT('Logger', 'info', log)
        # Check whether the Local Node details exists in BTM Table If YES set the Node state to Connecting
        if vnet_details["p2p_state"] == "searching":
            if not vnet_details["discovered_nodes"]:
                # Get Peer Nodes from the XMPP server
                self.registerCBT('Logger', 'info', interface_name + " P2P State: SEARCHING")
                return
            else:
                vnet_details["p2p_state"] = "connecting"
        # connecting to the peer-to-peer network
        if vnet_details["p2p_state"] == "connecting":
            self.registerCBT('Logger', 'info', interface_name + " P2P State: CONNECTING")
            self.add_successors(interface_name)
            # wait until atleast one successor, chord or on-demand links are created
            for peer in sorted(online_peer_list):
                # Check if at least a link is in Online State
                if self.is_link_connected(peer, interface_name):
                    vnet_details["p2p_state"] = "connected"
                    self.registerCBT('Logger', 'info', interface_name + " P2P State: CONNECTED")
                    linktype = vnet_details["link_type"][peer]
                    self.registerCBT('TincanInterface', 'DO_QUERY_ADDRESS_SET',
                                     {"interface_name": interface_name,
                                      "MAC": vnet_details[linktype][peer]["mac"], "uid": peer})
                    return

    def timer_method(self):
        try:
            for interface_name in self.ipop_vnets_details.keys():
                self.registerCBT("Logger", "debug", "BTM Table::" + str(self.ipop_vnets_details[interface_name]))
                # Invoke class method to create the topology
                self.manage_topology(interface_name)
                # Periodically query LinkManager for Peer2Peer Link Details
                self.registerCBT("LinkManager", "GET_LINK_DETAILS", {"interface_name": interface_name})
                if self.ipop_vnets_details[interface_name]["p2p_state"] == "started":
                    self.registerCBT('TincanInterface', 'DO_GET_STATE', {"interface_name": interface_name, "MAC": ""})
        except Exception as err:
            self.registerCBT('Logger', 'error', "Exception in BTM timer:" + str(err))
