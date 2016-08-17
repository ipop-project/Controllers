#!/usr/bin/env python
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule


class BaseTopologyManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(BaseTopologyManager, self).__init__(CFxHandle, paramDict, ModuleName)

        self.ipop_state = None

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))
        
    # send message (through XMPP service)
    #   - msg_type = message type attribute
    #   - uid      = UID of the destination node
    #   - msg      = message
    def send_msg_srv(self, msg_type, uid, msg):
        cbtdata = {"method": msg_type, "overlay_id": 0, "uid": uid, "data": msg} #TODO overlay_id
        self.registerCBT('XmppClient', 'DO_SEND_MSG', cbtdata)
        
    def log(self,msg,severity='info'):
        self.registerCBT('Logger',severity,msg)

    def processCBT(self, cbt):
        # new CBTs request for services from other modules by issuing CBTs; if no
        # services are required, the CBT is processed here.
        # con_ack packages and moves cas over XMPP/Overlay
        # con_resp is cas received from tin-can after transport allocation is done.
        if not self.checkMapping(cbt):
            if cbt.action in ["TINCAN_MSG", "XMPP_MSG"]:
                msg = cbt.data
                msg_type = msg.get("type", None)

                if msg_type in ["con_req","con_resp","con_ack"]:
                    stateCBT = self.registerCBT('Watchdog', 'QUERY_IPOP_STATE')
                    self.CBTMappings[cbt.uid] = [stateCBT.uid]

                    mapCBT = self.registerCBT('AddressMapper', 'RESOLVE', msg['uid'])
                    self.CBTMappings[cbt.uid].append(mapCBT.uid)

                    ip_mapCBT = self.registerCBT('AddressMapper', 'QUERY_IP_MAP')
                    self.CBTMappings[cbt.uid].append(ip_mapCBT.uid)

                    conn_stat_CBT = self.registerCBT('Monitor', 'QUERY_CONN_STAT', msg['uid'])
                    self.CBTMappings[cbt.uid].append(conn_stat_CBT.uid)

                    peer_list_CBT = self.registerCBT('Monitor', 'QUERY_PEER_LIST')
                    self.CBTMappings[cbt.uid].append(peer_list_CBT.uid)

                    self.pendingCBT[cbt.uid] = cbt
                    self.log("put in cbt queue {0}".format(msg_type))

            elif cbt.action == "QUERY_PEER_LIST_RESP":
                # cbt.data contains a dict of peers
                self.__link_trimmer(cbt.data)

            else:
                log = '{0}: unrecognized CBT {1} received from {2}'\
                        .format(cbt.recipient, cbt.action, cbt.initiator)
                self.registerCBT('Logger', 'warning', log)

        # CBTs that required servicing by other modules are processed here
        else:
            # get the source CBT of this request
            sourceCBT_uid = self.checkMapping(cbt)
            self.pendingCBT[cbt.uid] = cbt

            # wait until all requested services are complete
            if self.allServicesCompleted(sourceCBT_uid):
                if self.pendingCBT[sourceCBT_uid].action in ['TINCAN_MSG', 'XMPP_MSG']:
                    msg = self.pendingCBT[sourceCBT_uid].data
                    msg_type = msg.get("type", None)
                    if msg_type in ["con_req","con_resp","con_ack"]:
                        for key in self.CBTMappings[sourceCBT_uid]:
                            if self.pendingCBT[key].action == 'QUERY_IPOP_STATE_RESP':
                                self.ipop_state = self.pendingCBT[key].data
                            elif self.pendingCBT[key].action == 'RESOLVE_RESP':
                                ip4 = self.pendingCBT[key].data
                            elif self.pendingCBT[key].action == 'QUERY_CONN_STAT_RESP':
                                conn_stat = self.pendingCBT[key].data
                            elif self.pendingCBT[key].action == 'QUERY_PEER_LIST_RESP':
                                peer_list = self.pendingCBT[key].data
                            elif self.pendingCBT[key].action == 'QUERY_IP_MAP_RESP':
                                ip_map = self.pendingCBT[key].data

                        # process the original CBT when all values have been received
                        self.log("Received {0} action of type {1}".format(self.pendingCBT[sourceCBT_uid].action,msg_type))

                        if self.CMConfig["multihop"]:
                            conn_cnt = 0
                            for k, v in peer_list.items():
                                if "fpr" in v and v["status"] == "online":
                                    conn_cnt += 1
                            if conn_cnt >= self.CMConfig["multihop_cl"]:
                                return
                        if self.check_collision(msg_type, msg["uid"], conn_stat):
                            return
                        fpr_len = len(self.ipop_state["_fpr"])
                        fpr = msg["data"][:fpr_len]
                        cas = msg["data"][fpr_len + 1:]
                        ip4 = ipoplib.gen_ip4(msg["uid"],
                                ip_map,
                                self.ipop_state["_ip4"])

                        if (msg_type == "con_resp"):
                            # My cas recvd from tin-can, send it wrapped in a 
                            # con_ack to peer
                            target_uid = msg["uid"]
                            data = msg["data"]
                            self.send_msg_srv("con_ack",target_uid,data)
                            log = "recv con_resp from Tincan for {0}".format(msg["uid"])
                            self.registerCBT('Logger', 'info', log)
                        else:
                            self.create_connection(msg["uid"], fpr, 0,
                                    self.CMConfig["sec"], cas, ip4)


    def create_connection(self, uid, data, nid, sec, cas, ip4):
        # nid needs to be 0, to program tin-can to send con_resp to controller.
        conn_dict = {'uid': uid, 'fpr': data, 'nid': 0, 'sec': sec, 'cas': cas}
        self.registerCBT('LinkManager', 'CREATE_LINK', conn_dict)
        cbtdata = {"uid": uid, "ip4": ip4}
        self.registerCBT('TincanSender', 'DO_SET_REMOTE_IP', cbtdata)
        self.log("{0} Initiated connection to {1}".format(self.ipop_state["_uid"],uid))

    # TODO appears to always return False
    def check_collision(self, msg_type, uid, conn_stat):
        if msg_type == "con_req" and conn_stat == "req_sent":
            if uid > self.ipop_state["_uid"]:
                self.registerCBT('LinkManager', 'TRIM_LINK', uid)
                self.registerCBT('Monitor', 'DELETE_CONN_STAT', uid)
            return False
        elif msg_type in ["con_resp","con_ack"]:
            cbtdata = {
                'uid': uid,
                'status': "resp_recv"
            }

            self.registerCBT('Monitor', 'STORE_CONN_STAT', cbtdata)
            return False
        else:
            return True

    def __link_trimmer(self, peer_list):
        for k, v in peer_list.items():
            # trim the links of offline peers
            if "fpr" in v and v["status"] == "offline":
                if v["last_time"] > self.CMConfig["link_trimmer_wait_time"]:
                    self.registerCBT('LinkManager', 'TRIM_LINK', k)

            if self.CMConfig["multihop"]:
                connection_count = 0
                for k, v in peer_list.items():
                    if "fpr" in v and v["status"] == "online":
                        connection_count += 1
                        if connection_count > self.CMConfig["multihop_cl"]:
                            self.registerCBT('LinkManager', 'TRIM_LINK', k)

    def timer_method(self):
        self.registerCBT('Monitor', 'QUERY_PEER_LIST')

    def terminate(self):
        pass
