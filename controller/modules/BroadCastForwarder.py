#!/usr/bin/env/ python
import sys,time
from controller.framework.ControllerModule import ControllerModule


py_ver = sys.version_info[0]
class BroadCastForwarder(ControllerModule):
    def __init__(self ,CFxHandle, paramDict, ModuleName):
        super(BroadCastForwarder,self).__init__(CFxHandle,  paramDict,ModuleName)
        # Table to store VNet specific network information
        self.ipop_vnets_details = {}
        # Query CFX to get properties of virtual networks configured by the user
        tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Vnets")
        # Iterate across the virtual networks to get UID and TAPName
        for k in range(len(tincanparams)):
            interface_name = tincanparams[k]["TapName"]
            self.ipop_vnets_details[interface_name] = {}
            self.ipop_vnets_details[interface_name]["uid"] = tincanparams[k]["uid"]
            self.ipop_vnets_details[interface_name]["mac"] = ""
            # Stores local node's mac address obtained from LinkManager
            self.ipop_vnets_details[interface_name]["peerlist"] = []
        tincanparams = None
        # List to store timestamp of all messages seen by the node and drop any duplicate messages
        self.prevtimestamp = []
     
    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    # Method to store timestamp of messages processed to avoid duplicates
    def inserttimestamp(self, msgtime):
        # Check whether the length has exceeded the max value, if Yes, clean the list
        if len(self.prevtimestamp) < 10000:
            self.prevtimestamp.append(msgtime)
        else:
            self.prevtimestamp = []
            self.prevtimestamp.append(msgtime)

    def processCBT(self, cbt):
        # CBT gets Online Peerlist and MAC from BTM
        if cbt.action == 'ONLINE_PEERLIST':
            interface_name = cbt.data.get("interface_name")
            self.ipop_vnets_details[interface_name]["peerlist"] = list(sorted(cbt.data['peerlist']))
            self.ipop_vnets_details[interface_name]["mac"] = cbt.data.get("mac", None)
        # CBT to process network packets broadcasted over p2plink
        elif cbt.action == 'BroadcastPkt':
            self.sendtopeer(cbt.data, "BroadcastPkt")
        # CBT to process JSON data broadcasted over p2plink
        elif cbt.action == 'BroadcastData':
            self.sendtopeer(cbt.data, "BroadcastData")
        else:
            log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
            self.registerCBT('Logger', 'warning', log)

    # Sends data to the appropriate peer
    def sendtopeer(self, data, datype):
        interface_name = data.get("interface_name")
        # Check whether peer exists in the network, if not drop the message
        if self.ipop_vnets_details[interface_name]["peerlist"]:
            # Check the source of Broadcast message whether it is current node (in which case type='local')
            if data["type"] == "local":
                # Message originated at this node. Pass to all the Peers (with uid greater than itself).
                self.registerCBT('Logger', 'debug', "Broadcast message obtained from the local Tap interface")
                self.sendto_all_peers(sorted(self.ipop_vnets_details[interface_name]["peerlist"]),
                                       data["dataframe"], datype, data["interface_name"])
            else:
                messagetime = data["put_time"]
                # Check for duplicate broadcast message from different sources
                if self.prevtimestamp.count(messagetime) == 0:
                    self.inserttimestamp(messagetime)
                    # Message originated at some other node. Pass to peers upto the incoming successor uid.
                    self.registerCBT('Logger', 'debug', "Broadcast message received from peer node.")
                    self.sendto_peer(data["dataframe"], data["init_uid"], data["peer_list"], messagetime, datype,
                                 data["interface_name"])
                    # Passing the message to itself.
                    self.insertnetworkpacket(data, data["message_type"])
        else:
            self.registerCBT('Logger', 'info', "No online peers available for broadcast.")
            # if no online peers exists in the Forwarder table then send request to LinkManager to get the list
            self.registerCBT('LinkManager', 'GET_ONLINE_PEERLIST', {"interface_name": data["interface_name"]})

    def forwardmessage(self, msg_frame, init_id, suc_id, peer, peer_list, puttime, datype, interface_name):
        # ICC Message structure for broadcasting data over p2plink
        cbtdata = {
                    "msg_type": "forward",
                    "src_uid": suc_id,
                    "dst_uid": peer,
                    "interface_name": interface_name,
                    "msg": {
                            "dataframe": str(msg_frame),
                            "init_uid": init_id,
                            "peer_list": peer_list,
                            "put_time":  puttime,
                            "message_type": datype
                    }
        }
        # Register CBT to send message to the node whose UID is mentioned as destination UID
        self.registerCBT('BaseTopologyManager', 'ICC_CONTROL', cbtdata)

    # Method to forward message to peers from the Initiating node.
    def sendto_all_peers(self, plist, data, datype, interface_name):
        # Considering the node with the highest uid.
        self.registerCBT('Logger', 'info', 'Sending broadcast packet to all online peers'+str(plist))
        uid = self.ipop_vnets_details[interface_name]["uid"]
        messageputtime = int(round(time.time()*1000))
        # Case when the initiator is the last node in the network
        if uid > max(plist):
            self.registerCBT('Logger', 'info', 'Broadcast message sent to peer: '+str(plist[0]))
            self.forwardmessage(data, uid, uid, plist[0], [plist[0], uid], messageputtime, datype, interface_name)
        else:
            for ind, peer in enumerate(plist):
                if ind == len(plist)-1:
                    suc_id = plist[0]
                else:
                    suc_id = plist[ind+1]
                # Appending the message with the next succesor and the initiator
                self.registerCBT('Logger', 'debug', 'Broadcast message sent to Successor uid: {0}'.format(peer))
                self.forwardmessage(data, uid, uid, plist[ind], [peer, suc_id], messageputtime, datype, interface_name)

    # Method to forward packets when the initiator is elsewhere
    def sendto_peer(self, data_frame, init_id, in_plist, messagetime, datype, interface_name):
        self.registerCBT('Logger', 'info', 'Sending broadcast data to suitable peers.')
        uid = self.ipop_vnets_details[interface_name]["uid"]
        plist = sorted(self.ipop_vnets_details[interface_name]["peerlist"])
        # Case when next node is larger than initiator and current node UID
        if uid >= max(in_plist) and uid > init_id:
            for peer in plist:
                if peer != init_id and peer > uid:
                    self.registerCBT('Logger', 'debug', 'Broadcast message sent to UID: {0}'.format(peer))
                    self.forwardmessage(data_frame, init_id, uid, peer, in_plist, messagetime, datype, interface_name)
        # Case when next node is smaller than initiator and current node UID
        elif uid <= min(in_plist) and uid < init_id:
            for peer in plist:
                if init_id >= max(in_plist):
                    if uid < peer and in_plist.count(peer) == 0 and peer != init_id:
                        self.registerCBT('Logger', 'debug', 'Broadcast message sent to UID: {0}'.format(peer))
                        self.forwardmessage(data_frame, init_id, uid, peer, in_plist, messagetime, datype, interface_name)
                else:
                    if uid > peer and in_plist.count(peer) == 0 and peer != init_id:
                        self.registerCBT('Logger', 'debug', 'Broadcast message sent to UID: {0}'.format(peer))
                        self.forwardmessage(data_frame, init_id, uid, peer, in_plist, messagetime, datype, interface_name)
        else:
            for peer in plist:
                if uid < peer and in_plist.count(peer) == 0 and peer != init_id and peer < max(in_plist):
                    self.registerCBT('Logger', 'debug', 'Broadcast message sent to UID: {0}'.format(peer))
                    self.forwardmessage(data_frame, init_id, uid, peer, in_plist, messagetime, datype, interface_name)

    # Method to insert received packet into the local network stack
    def insertnetworkpacket(self, data, messagetype):
        # self.registerCBT('BaseTopologyManager', 'Send_Receive_Details', messagedetails)
        if messagetype != "BroadcastData" and data["type"] == "remote":
            self.registerCBT('Logger', 'info', 'Going to insert Broadcast Packet to Tap interface')
            self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', data)

    def terminate(self):
        pass

    def timer_method(self):
        # Refresh the Online Peer list on every timer thread invocation
        for interface_name in self.ipop_vnets_details.keys():
            self.registerCBT('LinkManager', 'GET_ONLINE_PEERLIST', {"interface_name": interface_name})
        pass
