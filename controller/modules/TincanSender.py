#!/usr/bin/env python
import json,sys
import socket,base64
from binascii import unhexlify, b2a_base64
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule
py_ver = sys.version_info[0]

class TincanSender(ControllerModule):

    def __init__(self, sock_list, CFxHandle, paramDict, ModuleName):
        super(TincanSender, self).__init__(CFxHandle, paramDict, ModuleName)
        self.sock = sock_list[0]
        self.dest = ()
        self.trans_counter= 0

        if socket.has_ipv6:
            self.dest = (self.CMConfig["localhost6"], self.CMConfig["svpn_port"])
        else:
            self.dest = (self.CMConfig["localhost"], self.CMConfig["svpn_port"])

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        if cbt.action == 'DO_CREATE_LINK':
            uid = cbt.data.get('uid')
            msg = cbt.data.get("data")
            log = "Creating conn to : {0}".format(uid)
            self.registerCBT('Logger', 'info', log)

            connection_details = ipoplib.CONCT
            conn_details = connection_details["IPOP"]["Request"]
            conn_details["InterfaceName"]                 = cbt.data.get("interface_name")
            connection_details["IPOP"]["TransactionId"]   = self.trans_counter
            self.trans_counter +=1
            conn_details["PeerInfo"]["VIP4"]      = msg.get('ip4')
            #conn_details["PeerInfo"]["VIP6"]      = msg.get('ip6')
            conn_details["PeerInfo"]["UID"]       = uid
            conn_details["PeerInfo"]["MAC"]       = msg.get('mac')
            conn_details["PeerInfo"]["CAS"]       = msg.get('cas')
            conn_details["PeerInfo"]["Fingerprint"] = msg.get('fpr')
            conn_details["PeerInfo"]["con_type"] = msg.get("con_type")

            log = "Connection Details : {0}".format(str(conn_details))
            self.registerCBT('Logger', 'debug', log)
            self.send_msg(json.dumps(connection_details))

        elif cbt.action == 'DO_TRIM_LINK':
            uid = cbt.data.get("uid")
            log = "removing conn to : {0}".format(uid)
            self.registerCBT('Logger', 'info', log)

            remove_node_details = ipoplib.REMOVE
            remove_node_details["IPOP"]["Request"]["InterfaceName"] = cbt.data.get("interface_name")
            remove_node_details["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter+=1
            #remove_node_details["IPOP"]["Request"]["UID"]           = uid
            remove_node_details["IPOP"]["Request"]["MAC"] = cbt.data.get("MAC")

            log = "Tincan Request : {0}".format(str(remove_node_details["IPOP"]))
            self.registerCBT('Logger', 'debug', log)
            self.send_msg(json.dumps(remove_node_details))

        elif cbt.action == 'DO_GET_STATE':
            get_state_request = ipoplib.LSTATE
            get_state_request["IPOP"]["TransactionId"]              = self.trans_counter
            self.trans_counter+=1
            get_state_request["IPOP"]["Request"]["ProtocolVersion"] = 4
            get_state_request["IPOP"]["Request"]["InterfaceName"] = cbt.data.get("interface_name")
            get_state_request["IPOP"]["Request"]["UID"]           = cbt.data.get("uid")
            get_state_request["IPOP"]["Request"]["MAC"] = cbt.data.get("MAC")
            log = "Tincan Request : {0}".format(str(get_state_request["IPOP"]))
            #self.registerCBT('Logger', 'debug', log)

            self.send_msg(json.dumps(get_state_request))

        elif cbt.action  == 'DO_GET_CAS':
            lcas = ipoplib.LCAS
            data = cbt.data
            uid  = data["uid"]
            lcas["IPOP"]["TransactionId"]                      = self.trans_counter
            self.trans_counter  +=1
            lcas["IPOP"]["Request"]["InterfaceName"]           = data["interface_name"]
            lcas["IPOP"]["Request"]["PeerInfo"]["VIP4"]        = data["data"]["ip4"]
            #lcas["IPOP"]["Request"]["PeerInfo"]["VIP6"]        = data["data"]["ip6"]
            lcas["IPOP"]["Request"]["PeerInfo"]["Fingerprint"] = data["data"]["fpr"]
            lcas["IPOP"]["Request"]["PeerInfo"]["UID"]         = uid
            lcas["IPOP"]["Request"]["PeerInfo"]["MAC"]         = data["data"]["mac"]
            lcas["IPOP"]["Request"]["PeerInfo"]["con_type"]    = data["data"]["con_type"]

            log = "Get CAS Request :: {0}".format(str(lcas["IPOP"]))
            self.registerCBT('Logger', 'debug', log)
            self.send_msg(json.dumps(lcas))

        elif cbt.action == 'DO_ECHO':
            ec = ipoplib.ECHO
            ec["IPOP"]["InterfaceName"] = cbt.data.get("interface_name")
            ec["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter+=1
            self.send_msg(json.dumps(ec))

        elif cbt.action == 'DO_SEND_ICC_MSG':
            icc_message_details = ipoplib.ICC
            icc_message_details["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter += 1
            icc_message_details["IPOP"]["Request"]["InterfaceName"]   = cbt.data.get("interface_name")
            icc_message_details["IPOP"]["Request"]["Recipient"]       = cbt.data.get('dst_uid')
            icc_message_details["IPOP"]["Request"]["RecipientMac"]    = cbt.data.get('dst_mac')
            msg  = cbt.data.get("msg")
            icc_message_details["IPOP"]["Request"]["Data"]      = json.dumps(msg)

            log = "ICC Message : {0}".format(str(icc_message_details["IPOP"]))
            self.registerCBT('Logger', 'debug', log)

            self.send_msg(json.dumps(icc_message_details))


        elif cbt.action == 'DO_INSERT_DATA_PACKET':
            packet = ipoplib.INSERT_TAP_PACKET
            packet["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter +=1
            packet["IPOP"]["Request"]["InterfaceName"]   = cbt.data["interface_name"]
            packet["IPOP"]["Request"]["Data"]      = cbt.data["dataframe"]

            log = "Network Packet Inserted: {0}".format(str(packet["IPOP"]))
            self.registerCBT('Logger', 'info', log)

            self.send_msg(json.dumps(packet))

        elif cbt.action == 'DO_RETRY':
            self.send_msg(json.dumps(cbt.data))

        elif cbt.action == "DO_INSERT_ROUTING_RULES":
            add_routing = ipoplib.ADD_ROUTING
            add_routing["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter += 1
            add_routing["IPOP"]["Request"]["InterfaceName"] = cbt.data["interface_name"]
            add_routing["IPOP"]["Request"]["Routes"]        = []
            sourcemac = cbt.data["sourcemac"]
            for mac in cbt.data.get("destmac"):
                if mac != "0"*12 and mac != sourcemac:
                    add_routing["IPOP"]["Request"]["Routes"]= [mac+":"+sourcemac]

                    log = "Routing Rule Inserted: {0}".format(str(add_routing["IPOP"]))
                    self.registerCBT('Logger', 'debug', log)

                    self.send_msg(json.dumps(add_routing))
        elif cbt.action == "DO_REMOVE_ROUTING_RULES":
            remove_routing = ipoplib.DELETE_ROUTING
            remove_routing["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter += 1
            remove_routing["IPOP"]["Request"]["InterfaceName"] = cbt.data["interface_name"]
            remove_routing["IPOP"]["Request"]["Routes"]        = [ cbt.data["mac"] ]

            log = "Routing Rule Removed: {0}".format(str(remove_routing["IPOP"]))
            self.registerCBT('Logger', 'debug', log)
            self.send_msg(json.dumps(remove_routing))
        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def send_msg(self, msg):
        return self.sock.sendto(bytes((msg).encode('utf-8')), self.dest)

    def timer_method(self):
        pass

    def terminate(self):
        pass
