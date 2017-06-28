from controller.framework.ControllerModule import ControllerModule
from controller.framework.CFx import CFX
import time
import math


class BaseTopologyManager(ControllerModule, CFX):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(BaseTopologyManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.CFxHandle = CFxHandle
        # Variable to store default BaseTopology parameters
        self.CMConfig = paramDict
        # BTM internal Table
        self.ipop_vnets_details = {}
        # Limit for links that can be created by a node
        self.max_num_links = self.CMConfig["NumberOfSuccessors"] + self.CMConfig["NumberOfChords"] + \
                             self.CMConfig["NumberOfOnDemand"] + self.CMConfig["NumberOfInbound"]
        # Query CFX to get properties of virtual networks configured by the user
        tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Vnets")
        # Iterate across the virtual networks to get XMPPModuleName and TAPName
        for k in range(len(tincanparams)):
            interface_name = tincanparams[k]["TapName"]
            self.ipop_vnets_details[interface_name] = {}
            virtual_net_details = self.ipop_vnets_details[interface_name]
            virtual_net_details["p2p_state"] = "started"
            virtual_net_details["GeoIP"] = ""
            virtual_net_details["ipop_state"] = {}
            virtual_net_details["discovered_nodes"] = []
            virtual_net_details["log_chords"] = []
            virtual_net_details["successor"] = {}
            virtual_net_details["chord"] = {}
            virtual_net_details["on_demand"] = {}
            virtual_net_details["ip_uid_table"] = {}
            virtual_net_details["uid_mac_table"] = {}
            virtual_net_details["mac_uid_table"] = {}
            virtual_net_details["link_type"] = {}
            virtual_net_details["peer_uid_sendmsgcount"] = {}
            virtual_net_details["xmpp_client_code"] = tincanparams[k]["XMPPModuleName"]
        tincanparams = None

    def initialize(self):
        # Iterate across different TapInterface to initialize BTM table attributes
        for interface_name in self.ipop_vnets_details.keys():
            # Invoke Tincan to get Local node state
            self.registerCBT('TincanInterface', 'DO_GET_STATE', {"interface_name": interface_name, "MAC": ""})
            # Get Peer Nodes from XMPP server
            self.registerCBT(self.ipop_vnets_details[interface_name]["xmpp_client_code"], "GET_XMPP_PEERLIST",
                             {"interface_name": interface_name})
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def terminate(self):
        pass

    # Method to create all outbound links from the Node
    def add_outbound_link(self, link_type, uid, interface_name):
        self.registerCBT("LinkManager", "CREATE_LINK", {"uid": uid, "interface_name": interface_name})
        if uid not in self.ipop_vnets_details[interface_name]["link_type"].keys():
            self.ipop_vnets_details[interface_name]["link_type"].update({uid: link_type})

    # remove connection
    # remove a link by peer UID
    def remove_link(self, uid, interface_name, link=None):
        if link is None:
            connection_type_list = ["successor", "chord", "on_demand"]
        else:
            connection_type_list = [link]

        for link_type in connection_type_list:
            if uid in self.ipop_vnets_details[interface_name][link_type].keys():
                self.ipop_vnets_details[interface_name][link_type].pop(uid)
                message = {"uid": uid, "interface_name": interface_name}
                self.registerCBT("LinkManager", "REMOVE_LINK", message)
                log = "Connection remove request for UID: {0}".format(uid)
                self.registerCBT('Logger', 'info', log)

############################################################################
        # successors policy                                                        #
############################################################################
        # [1] A discovers nodes in the network
        #     A requests to link to the closest successive node B as A's successor
        # [2] B accepts A's link request, with A as B's inbound link
        #     B responds to link to A
        # [3] A and B are connected
        # [*] the link is terminated when A discovers and links to closer successive
        #     nodes, or the link disconnects

    def add_successors(self, interface_name):
        # sort nodes into rotary, unique list with respect to this UID
        virtual_net_details = self.ipop_vnets_details[interface_name]
        nodeuid = virtual_net_details["ipop_state"]["_uid"]
        nodes = list(sorted(virtual_net_details["discovered_nodes"]))

        if nodeuid in nodes:
            nodes.remove(nodeuid)
        if max([nodeuid] + nodes) != nodeuid:
            while nodes[0] < nodeuid:
                nodes.append(nodes.pop(0))

        requested_nodes = []
        # link to the closest <num_successors> nodes (if not already linked)
        for node in nodes[0:min(len(nodes), self.CMConfig["NumberOfSuccessors"])]:
            if node not in virtual_net_details["successor"].keys():
                self.add_outbound_link("successor", node, interface_name)
                requested_nodes.append(node)

        # establishing link from the smallest UID node in the network to the biggest UID in the network
        if min([nodeuid] + nodes) == nodeuid and len(nodes) > 1:
            for node in list(reversed(nodes))[0:self.CMConfig["NumberOfSuccessors"]]:
                if node not in virtual_net_details["successor"].keys():
                    self.add_outbound_link("successor", node, interface_name)
                    requested_nodes.append(node)

    def remove_successors(self, interface_name):
        # sort nodes into rotary, unique list with respect to this UID
        virtual_net_details = self.ipop_vnets_details[interface_name]
        successors = list(sorted(virtual_net_details["successor"].keys()))
        local_uid = virtual_net_details["ipop_state"]["_uid"]

        # Allow the least node in the network to have the as many connections as required to maintain a fully connected
        # Ring Topology
        if max([local_uid] + successors) != local_uid:
            while successors[0] < local_uid:
                successors.append(successors.pop(0))

        # remove all linked successors not within the closest <num_successors> linked nodes
        # remove all unlinked successors not within the closest <num_successors> nodes
        num_linked_successors = 0

        if len(successors) % 2 == 0:
            loop_counter = len(successors) / 2
        elif len(successors) == 1:
            loop_counter = 0
        else:
            loop_counter = len(successors) / 2 + 1

        i = 0
        while i < loop_counter:
            if successors[i] not in successors:
                num_linked_successors += 1
                if num_linked_successors > (2 * int(self.CMConfig["num_successors"])):
                    self.remove_link(successors[i], interface_name, link="successor")
            if successors[-(i + 1)] not in successors:
                num_linked_successors += 1
                if num_linked_successors > (2 * int(self.CMConfig["num_successors"])):
                    self.remove_link(successors[-(i + 1)], interface_name, link="successor")
            i += 1

############################################################################
            # chords policy                                                            #
############################################################################
    # [1] A forwards a headless find_chord message approximated by a designated UID
    # [2] B discovers that it is the closest node to the designated UID
    #     B responds with a found_chord message to A
    # [3] A requests to link to B as A's chord
    # [4] B accepts A's link request, with A as B's inbound link
    #     B responds to link to A
    # [5] A and B are connected
    # [*] the link is terminated when the chord time-to-live attribute expires and
    #     a better chord was found or the link disconnects

    def find_chords(self, interface_name):
        # find chords closest to the approximate logarithmic nodes
        link_details = self.ipop_vnets_details[interface_name]
        current_node_uid = link_details["ipop_state"]["_uid"]
        if len(link_details["log_chords"]) == 0:
            for i in reversed(range(self.CMConfig["NumberOfChords"])):
                log_num = (int(current_node_uid, 16) + int(math.pow(2, 160 - 1 - i))) % int(math.pow(2, 160))
                log_uid = "{0:040x}".format(log_num)
                link_details["log_chords"].append(log_uid)

        # determine list of designated UIDs
        log_chords = link_details["log_chords"]
        for chord in link_details["chord"].values():
            if "log_uid" in chord.keys():
                if chord["log_uid"] in log_chords:
                    log_chords.remove(chord["log_uid"])

        # forward find_chord messages to the nodes closest to the designated UID
        for log_uid in log_chords:
            # forward find_chord message
            new_msg = {
                        "fwd_type": "closest",
                        "dst_uid": log_uid,
                        "interface_name": interface_name,
                        "data": {
                            "msg_type": "find_chord",
                            "src_uid": current_node_uid,
                            "dst_uid": log_uid,
                            "log_uid": log_uid
                        }
            }

            self.registerCBT("BaseTopologyManager", "FORWARD_MSG", new_msg)

    # Sets GEO Location IP (needed by Visualizer Module)
    def setGeoIP(self, interface_name, cas):
        try:
            casdetails = str(cas).split(":")
            for i,ele in enumerate(casdetails):
                if str(ele).count(".") == 3 :
                    if casdetails[i-1] == "udp" and casdetails[i+5] == "stun":
                        ip_octet = str(ele).split(".")
                        if ip_octet[0] == "10":
                            pass
                        elif ip_octet[0] == "172" and ip_octet[1] in range(16,32,1):
                            pass
                        elif ip_octet[0] == "192" and ip_octet == "168":
                            pass
                        else:
                            self.ipop_vnets_details[interface_name]["GeoIP"] = ele
        except Exception as err:
            self.registerCBT("Logger","error","Error while Setting GeoIP:{0}".format(err))

    # Method to trim stale chord connections and initiate better chord connections
    def clean_chord(self, interface_name):
        links = self.ipop_vnets_details[interface_name]
        # Check whether p2p chord links exists for the node, If NOT return
        if not links["chord"].keys():
            return
        # find chord with the oldest time-to-live attribute
        uid = min(links["chord"].keys(), key=lambda u: (links["chord"][u]["ttl"]))
        # time-to-live attribute has expired: determine if a better chord exists
        if time.time() > links["chord"][uid]["ttl"]:
            # forward find_chord message
            if "log_uid" in links["chord"][uid].keys():
                new_msg = {
                    "msg_type": "find_chord",
                    "src_uid": self.ipop_vnets_details[interface_name]["uid"],
                    "dst_uid": links["chord"][uid]["log_uid"],
                    "log_uid": links["chord"][uid]["log_uid"]
                }
                forward_message = {
                    "fwd_type": "closest",
                    "dst_uid": links["chord"][uid]["log_uid"],
                    "interface_name": interface_name,
                    "data": new_msg
                }
                # Forward the find_chord as an ICC message
                self.registerCBT("BaseTopologyManager", "FORWARD_MSG", forward_message)
            # Remove the stale chord link
            self.remove_link(uid, interface_name, link="chord")

    #Method to clean on-demand links
    def clean_on_demand(self, interface_name):
        on_demand_link_details = self.ipop_vnets_details[interface_name]["on_demand"]
        # check whether on-demand links exists if NO return
        if not on_demand_link_details.keys():
            return
        # Iterate across the On-demand Table to determine stale links
        for peeruid, link_details in list(on_demand_link_details.items()):
            # check whether the link is in Offline state or the data transfer is below the threshold,
            # if YES remove the link
            if link_details["status"] != "online" or (link_details["stats"][0]["sent_bytes_second"] + \
                link_details["stats"][0]["recv_bytes_second"] < self.CMConfig["OndemandDataTransferRate"]):
                self.remove_link(peeruid, interface_name, link="on_demand")


    def processCBT(self, cbt):
        msg = cbt.data
        msg_type = msg.get("type", None)
        interface_name = msg["interface_name"]
        virtual_net_details = self.ipop_vnets_details[interface_name]
        # CBT to process peerlist from XMPPClient module
        if cbt.action == "UPDATE_XMPP_PEERLIST":
            xmpp_peer_list = msg.get("peer_list")
            if len(xmpp_peer_list) > 0:
                virtual_net_details["discovered_nodes"] += xmpp_peer_list
                virtual_net_details["discovered_nodes"] = list(set(virtual_net_details["discovered_nodes"]))
            else:
                virtual_net_details["discovered_nodes"] = []
            self.registerCBT(virtual_net_details["xmpp_client_code"], "GET_XMPP_PEERLIST", {"interface_name": interface_name})
        elif cbt.action == "FORWARD_MSG":
            self.forward_msg(msg["fwd_type"], msg["dst_uid"], msg["data"], interface_name)
        # CBT to process p2p link state details from LinkManager
        elif cbt.action == "RETRIEVE_LINK_DETAILS":
            data = msg.get("data")
            current_links = virtual_net_details["link_type"].keys()
            updated_links = data.keys()
            # If length of current_links is equal to updated_links it means no link got dropped
            if len(current_links) == len(updated_links):
                # Update Link details (E.g TTL, Status)
                for peeruid in current_links:
                    virtual_net_details[virtual_net_details["link_type"][peeruid]].update({peeruid: data[peeruid]})
            else:
                # Extract nodes in current_links not present in the updated_links. These are the deleted links
                deleted_links = set(current_links) - set(updated_links)
                for peeruid in deleted_links:
                    # Deleted the Peer UID from BTM's link table
                    if peeruid in virtual_net_details[virtual_net_details["link_type"][peeruid]]:
                        del virtual_net_details[virtual_net_details["link_type"][peeruid]][peeruid]
                    if peeruid in virtual_net_details["uid_mac_table"]:
                        # Extract unmanaged nodes behind the Peer UID
                        unmanaged_node_mac_list = virtual_net_details["uid_mac_table"][peeruid]
                        # Deleted the Peer UID entry from the UID_MAC_TABLE
                        del virtual_net_details["uid_mac_table"][peeruid]
                        # Iterate across the unmanaged node mac list and remove it from MAC_UID Table
                        for node_mac in unmanaged_node_mac_list:
                            del virtual_net_details["mac_uid_table"][node_mac]
                    # Iterate across IP_UID Table and remove all keys whose value is the Peer UID
                    for ip, uid in list(virtual_net_details["ip_uid_table"].items()):
                        if uid == peeruid:
                            del virtual_net_details["ip_uid_table"][ip]
                    # Delete the entry from Peer UID sent msg table
                    if peeruid in virtual_net_details["peer_uid_sendmsgcount"]:
                        del virtual_net_details["peer_uid_sendmsgcount"][peeruid]
        elif cbt.action == "XMPP_MSG":
            # Remove Offline peer node from Discovered node List
            if msg_type == "offline_peer":
                if msg["uid"] in virtual_net_details["discovered_nodes"]:
                        virtual_net_details["discovered_nodes"].remove(msg["uid"])
                log = "Removed peer from discovered node list {0}".format(msg["uid"])
                self.registerCBT('Logger', 'debug', log)
            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                        .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                self.registerCBT('Logger', 'warning', log)
        elif cbt.action == "TINCAN_RESPONSE":
            # update local state into BTM table
            if msg_type == "local_state":
                virtual_net_details["ipop_state"] = msg
                virtual_net_details["mac"] = msg["mac"]
                virtual_net_details["mac_uid_table"][msg["mac"]] = msg["_uid"]
                if msg["_uid"] not in virtual_net_details["uid_mac_table"].keys():
                    virtual_net_details["uid_mac_table"][msg["_uid"]] = [msg["mac"]]
                self.registerCBT("Logger", "info", "Local Node Info UID:{0} MAC:{1} IP4: {2}".format(msg["_uid"],
                                msg["mac"], msg["_ip4"]))
            else:
                self.setGeoIP(interface_name, msg["cas"])
        elif cbt.action == "UPDATE_MAC_UID_IP_TABLES":
            location = msg.get("location")
            uid = msg["uid"]
            localuid = virtual_net_details["ipop_state"]["_uid"]

            # check whether an entry exists for UID, if NOT create an entry in UID_MAC Table
            if uid not in list(virtual_net_details["uid_mac_table"].keys()):
                virtual_net_details["uid_mac_table"][uid] = []

            self.registerCBT('Logger', 'debug', 'UpdateMACUIDMessage:::' + str(msg))
            '''
            if uid not in virtual_net_details["online_peer_uid"] and uid != localuid:
                 nextuid = self.getnearestnode(uid, interface_name)
                 nextnodemac = virtual_net_details["peers"][nextuid]["mac"]
                 for destmac in list(msg["mac_ip_table"].keys()):
                      self.registerCBT('Logger', 'info', 'MAC_UID Table:::' + str(virtual_net_details["mac_uid_table"]))
                      if destmac not in list(virtual_net_details["mac_uid_table"].keys()):
                           message = {
                                    "interface_name": interface_name,
                                    "sourcemac": nextnodemac,
                                    "destmac": [destmac]
                           }
                           self.registerCBT("TincanInterface", "DO_INSERT_FORWARDING_RULES", message)
                      else:
                           olduid = virtual_net_details["mac_uid_table"][destmac]
                           if olduid != uid:
                                message = {
                                        "interface_name": interface_name,
                                        "sourcemac": nextnodemac,
                                        "destmac": [destmac]
                                }
                                self.registerCBT("TincanInterface", "DO_INSERT_FORWARDING_RULES", message)
            '''
            # Update the IP_UID and MAC_UID Table with the Unmanaged node details
            for mac, ip in msg["mac_ip_table"].items():
                if mac not in virtual_net_details["uid_mac_table"][uid]:
                    virtual_net_details["uid_mac_table"][uid].append(mac)
                    virtual_net_details["ip_uid_table"].update({ip: uid})
                virtual_net_details["mac_uid_table"].update({mac: uid})
        elif cbt.action == "ICC_CONTROL":
            msg_type = msg.get("msg_type", None)
            # advertisement of nearby nodes
            if msg_type == "advertise":
                virtual_net_details["discovered_nodes"] = list(set(virtual_net_details["discovered_nodes"] + msg["peer_list"]))
                localuid = virtual_net_details["ipop_state"]["_uid"]
                if localuid in virtual_net_details["discovered_nodes"]:
                    virtual_net_details["discovered_nodes"].remove(localuid)
                log = "Received p2p link advertisement from node UID: {0}".format(msg["src_uid"])
                self.registerCBT('Logger', 'info', log)
            # handle forward packet
            elif msg_type == "forward":
                dst_uid = msg["dst_uid"]
                # Check whether the current node is the intended recipient of the message
                if dst_uid != virtual_net_details["ipop_state"]["_uid"]:
                    self.forward_msg("exact", msg["dst_uid"], msg, interface_name)
                else:
                    msg["interface_name"] = interface_name
                    # Check whether the forwarded message is a network packet,
                    # If YES insert it into the local network interface
                    if "datagram" in msg.keys():
                        data = msg.pop("datagram")
                        msg["dataframe"] = data
                        self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', msg)
            # handle find chord
            elif msg_type == "find_chord":
                if self.forward_msg("closest", msg["dst_uid"], msg, interface_name):
                    # Check whether the current node UID is bigger than the Chord UID
                    if msg["src_uid"] > self.ipop_vnets_details[interface_name]["ipop_state"]["_uid"]:
                        self.add_outbound_link("chord", msg["src_uid"], interface_name)
                    else:
                        # forward found_chord message
                        new_msg = {
                                "msg_type": "found_chord",
                                "src_uid": self.ipop_vnets_details[interface_name]["ipop_state"]["_uid"],
                                "dst_uid": msg["src_uid"],
                                "log_uid": msg["log_uid"]
                        }
                        self.forward_msg("exact", msg["src_uid"], new_msg, interface_name)
            # handle found chord
            elif msg_type == "found_chord":
                if self.forward_msg("exact", msg["dst_uid"], msg, interface_name):
                    if msg["src_uid"] > self.ipop_vnets_details[interface_name]["ipop_state"]["_uid"]:
                        self.add_outbound_link("chord", msg["src_uid"], interface_name)
            elif msg_type == "add_on_demand":
                self.add_outbound_link("on_demand", msg["uid"], msg["interface_name"])
            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                        .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                self.registerCBT('Logger', 'warning', log)
        elif cbt.action == "GET_VISUALIZER_DATA":
            for interface_name in self.ipop_vnets_details.keys():
                virtual_net_details = self.ipop_vnets_details[interface_name]
                local_uid, local_ip = "", ""
                if virtual_net_details["p2p_state"] == "connected" and "ipop_state" in virtual_net_details.keys():
                    local_uid = virtual_net_details["ipop_state"]["_uid"]
                    local_ip = virtual_net_details["ipop_state"]["_ip4"]
                unmanaged_node_list, successors, chords, on_demands = [], [], [], []

                # Iterate over the IP-UID Table to retrieve Unmanaged node IP list
                for ip, uid in list(virtual_net_details["ip_uid_table"].items()):
                    # check whether the IP is that of the local node
                    if ip != local_ip and uid == local_uid and ip != "0.0.0.0":
                        unmanaged_node_list.append(ip)
                # Extract the online successor list from the BTM Table
                for successor in list(virtual_net_details["successor"].keys()):
                    if "status" in virtual_net_details["successor"][successor].keys():
                        if virtual_net_details["successor"][successor]["status"] == "online":
                            successors.append(successor)
                # Extract the online chord list from the BTM Table
                for chord in list(virtual_net_details["chord"].keys()):
                    if "status" in virtual_net_details["chord"][chord].keys():
                        if virtual_net_details["chord"][chord]["status"] == "online":
                            chords.append(chord)
                # Extract the online on_demand list from the BTM Table
                for ondemand in list(virtual_net_details["on_demand"].keys()):
                    if "status" in virtual_net_details["on_demand"][ondemand].keys():
                        if virtual_net_details["on_demand"][ondemand]["status"] == "online":
                            on_demands.append(ondemand)
                # Check if GEO IP exists else invoke the function to retrieve the details from Public Stun server
                if virtual_net_details["GeoIP"] in ["", None]:
                    geoip = ""
                    #virtual_net_details["GeoIP"] = geoip
                else:
                    geoip = virtual_net_details["GeoIP"]

                # Message for Overlay visualizer
                new_msg = {
                    "interface_name": interface_name,
                    "uid": local_uid,
                    "ip4": local_ip,
                    "GeoIP": geoip,
                    "mac": virtual_net_details["mac"],
                    "state": virtual_net_details["p2p_state"],
                    "macuidmapping": virtual_net_details["uid_mac_table"],
                    "unmanagednodelist": unmanaged_node_list,
                    "links": {
                        "successor": successors,
                        "chord": chords,
                        "on_demand": on_demands
                    }
                }
                self.registerCBT("OverlayVisualizer", "TOPOLOGY_DETAILS", new_msg)
        # handle and forward tincan data packets
        elif cbt.action == "TINCAN_PACKET":
            reqdata = cbt.data
            data = reqdata["dataframe"]
            m_type = reqdata["m_type"]
            # ignore packets when not connected to the overlay
            if virtual_net_details["p2p_state"] != "connected":
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
                dst_ip = '.'.join(str(int(i, 16)) for i in [data[destmacindex:destipindex][i:i + 2] for i in range(0, 8, 2)])
            else:
                # Check whether the packet is IPv4 or IPv6
                if data[24:28] == "0800":
                    dst_ip = '.'.join(str(int(i, 16)) for i in [data[60:68][i:i + 2] for i in range(0, 8, 2)])
                else:
                    dst_ip = data[76: 108]
                destmac, srcmac = data[0:12], data[12:24]

            ip4_uid_table = virtual_net_details["ip_uid_table"]
            # If the destination IP exists in IP_UID_Table, if YES get the UID and send the message to the Peer
            if dst_ip in list(ip4_uid_table.keys()):
                dst_uid = ip4_uid_table[dst_ip]
            # If the destination MAC exists in MAC_UID_Table, if YES get the UID and send the message to the Peer
            elif destmac in virtual_net_details["mac_uid_table"].keys():
                dst_uid = virtual_net_details["mac_uid_table"][destmac]
            # Check if it is an IPv4 Multicast packet
            elif destmac[0:6] == "01005E":
                self.registerCBT("IPMulticast", "IPv4_MULTICAST", {"dataframe": data, "interface_name": interface_name,
                                                                   "type": "local"})
                return
            # Check if it is an IPv6 Multicast packet
            elif destmac[0:4] == "3333":
                self.registerCBT("IPMulticast", "IPv6_MULTICAST", {"dataframe": data, "interface_name": interface_name,
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
                self.registerCBT("BroadCastForwarder", "BroadcastPkt", datapacket)
                return
            else:
                log = "recv illegal tincan_packet: src={0} dst={1}".format(srcmac, destmac)
                self.registerCBT('Logger', 'info', log)
                return
            # Message routing to one of the local node attached to this UID
            if dst_uid == virtual_net_details["ipop_state"]["_uid"]:
                    network_inject_message = {
                        "dataframe": data,
                        "interface_name": interface_name
                    }
                    self.registerCBT("TincanInterface", "DO_INSERT_DATA_PACKET", network_inject_message)
                    return
            # send forwarded message
            new_msg = {
                    "msg_type": "forward",
                    "src_uid": virtual_net_details["ipop_state"]["_uid"],
                    "dst_uid": dst_uid,
                    "datagram": data
            }
            self.forward_msg("exact", dst_uid, new_msg, interface_name)

            # Check whether the UID entry exists in the msgcount table
            if dst_uid not in list(virtual_net_details["peer_uid_sendmsgcount"].keys()):
                virtual_net_details["peer_uid_sendmsgcount"][dst_uid] = {"count": 1}
            else:
                virtual_net_details["peer_uid_sendmsgcount"][dst_uid]["count"] += 1
                # Check whether number of messages sent to the Peer UID has exceeded max value if YES create an ondemand link
                if virtual_net_details["peer_uid_sendmsgcount"][dst_uid]["count"] > self.CMConfig["OndemandThreshold"]:
                    # Check whether the connection to Peer already exists
                    if self.linked(dst_uid, interface_name):
                        # Connection already exists no need to create an On-demand link reset the msg counter
                        virtual_net_details["peer_uid_sendmsgcount"][dst_uid] = {"count": 0}
                    # First-Time on-demand link creation, record the time it has initated
                    elif "conn_init_time" not in list(virtual_net_details["peer_uid_sendmsgcount"][dst_uid].keys()):
                        virtual_net_details["peer_uid_sendmsgcount"][dst_uid]["conn_init_time"] = time.time()
                        # add on-demand link
                        self.add_outbound_link("on_demand", dst_uid, interface_name)
                    # If On-demand connection not established beyond the wait time, initiate a new request
                    elif time.time() - virtual_net_details["peer_uid_sendmsgcount"][dst_uid]["conn_init_time"] > \
                        self.CMConfig["OndemandConnectionWaitTime"] and dst_uid not in virtual_net_details["on_demand"]:
                        # Update connection initiation time
                        virtual_net_details["peer_uid_sendmsgcount"][dst_uid]["conn_init_time"] = time.time()
                        # add on-demand link
                        self.add_outbound_link("on_demand", dst_uid, interface_name)

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
        virtual_net_details = self.ipop_vnets_details[interface_name]
        uid = virtual_net_details["ipop_state"]["_uid"]
        nxt_uid = uid
        online_peer_list = list(virtual_net_details["successor"].keys())+list(virtual_net_details["chord"].keys()) +\
                           list(virtual_net_details["on_demand"].keys())
        # Iterate across the Peer List
        for peer in sorted(online_peer_list):
            # Check if the link to Peer is Online else dont forward the message to the Peer UID
            if self.linked(peer, interface_name):
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
        self.registerCBT("LinkManager", "SEND_ICC_MSG", {"dst_uid": nxt_uid, "msg": msg, "interface_name": interface_name})
        return False

    # Method checks if the link to Peer UID is Online(Connected)
    def linked(self, uid, interface_name):
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
        self.registerCBT('Logger', 'debug', "Inside Topology Manager")
        virtual_net_details = self.ipop_vnets_details[interface_name]
        # Extract all the peer UIDs seen by the node
        online_peer_list = list(virtual_net_details["successor"].keys()) + list(virtual_net_details["chord"].keys()) + \
                           list(virtual_net_details["on_demand"].keys())

        if virtual_net_details["p2p_state"] == "started":
            if not virtual_net_details["ipop_state"]:
                self.registerCBT('Logger', 'info', interface_name + " p2p state: started")
                return
            else:
                virtual_net_details["p2p_state"] = "searching"
                log = "identified local state: {0}".format(virtual_net_details["ipop_state"]["_uid"])
                self.registerCBT('Logger', 'info', log)
        # Check whether the Local Node details exists in BTM Table If YES set the Node state to Connecting
        if virtual_net_details["p2p_state"] == "searching":
            if not virtual_net_details["discovered_nodes"]:
                # Get Peer Nodes from the XMPP server
                self.registerCBT('Logger', 'info', interface_name + " p2p state: searching")
                return
            else:
                virtual_net_details["p2p_state"] = "connecting"
        # connecting to the peer-to-peer network
        if virtual_net_details["p2p_state"] == "connecting":
            self.registerCBT('Logger', 'debug', "discovered nodes: {0}".format(virtual_net_details["discovered_nodes"]))
            self.registerCBT('Logger', 'info', interface_name + " p2p state: connecting")
            self.add_successors(interface_name)
            # wait until atleast one successor, chord or on-demand links are created
            for peer in sorted(online_peer_list):
                # Check if atleast a link is in Online State
                if self.linked(peer, interface_name):
                    virtual_net_details["p2p_state"] = "connected"
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: CONNECTED")
                    linktype = virtual_net_details["link_type"][peer]
                    self.registerCBT('TincanInterface', 'DO_QUERY_ADDRESS_SET',
                                     {"interface_name": interface_name,
                                      "MAC": virtual_net_details[linktype][peer]["mac"], "uid": peer})
                    return
        # connecting or connected to the IPOP peer-to-peer network
        if virtual_net_details["p2p_state"] == "connected":
            # manage successors
            self.add_successors(interface_name)
            self.remove_successors(interface_name)
            # periodically call policy to clean Chords and On-Demand Links
            self.clean_chord(interface_name)
            #self.clean_on_demand(interface_name)
            # manage chords
            self.find_chords(interface_name)
            # Iterate across all the p2p links created by the node
            for peer in sorted(online_peer_list):
                # Check if atleast a link is in Online State
                if self.linked(peer, interface_name):
                    virtual_net_details["p2p_state"] = "connected"
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: CONNECTED")
                    return
            virtual_net_details["p2p_state"] = "connecting"
            self.registerCBT('Logger', 'info', interface_name + " p2p state: DISCONNECTED")

    def timer_method(self):
        try:
            for interface_name in self.ipop_vnets_details.keys():
                self.registerCBT("Logger","debug","BTM Table::"+str(self.ipop_vnets_details[interface_name]))
                # Invoke class method to create the topology
                self.manage_topology(interface_name)
                # Periodically query LinkManager for Peer2Peer Link Details
                self.registerCBT("LinkManager", "GET_LINK_DETAILS", {"interface_name": interface_name})
                if self.ipop_vnets_details[interface_name]["p2p_state"] == "started":
                    self.registerCBT('TincanInterface', 'DO_GET_STATE', {"interface_name": interface_name, "MAC": ""})
                if len(self.ipop_vnets_details[interface_name]["on_demand"].keys()) != 0:
                    for peeruid, linktype in list(self.ipop_vnets_details[interface_name]["on_demand"].item()):
                        self.registerCBT('TincanInterface', 'DO_QUERY_TUNNEL_STATS',
                                         {"interface_name": interface_name, "MAC": linktype["mac"], "uid": peeruid})
        except Exception as err:
            self.registerCBT('Logger', 'error', "Exception in BTM timer:" + str(err))
