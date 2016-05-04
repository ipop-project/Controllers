import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule


class BaseTopologyManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict):

        super(BaseTopologyManager, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.ipop_state = None

    def initialize(self):

        logCBT = self.CFxHandle.createCBT(initiator='BaseTopologyManager',
                                          recipient='Logger',
                                          action='info',
                                          data="BaseTopologyManager Loaded")
        self.CFxHandle.submitCBT(logCBT)

    def processCBT(self, cbt):

        # In case of a fresh CBT, request the required services
        # from the other modules, by issuing CBTs. If no services
        # from other modules required, process the CBT here only
        if(not self.checkMapping(cbt)):
            if(cbt.action == "TINCAN_MSG"):
                msg = cbt.data
                msg_type = msg.get("type", None)

                # we ignore connection status notification for now
                if msg_type == "con_stat":
                    pass

                elif msg_type == "con_req" or msg_type == "con_resp":
                    stateCBT = self.CFxHandle.createCBT(initiator='Base'
                                                        'TopologyManager',
                                                        recipient='Watchdog',
                                                        action='QUERY_IPOP'
                                                        '_STATE',
                                                        data="")
                    self.CFxHandle.submitCBT(stateCBT)
                    self.CBTMappings[cbt.uid] = [stateCBT.uid]

                    mapCBT = self.CFxHandle.createCBT(initiator='BaseTopology'
                                                      'Manager',
                                                      recipient='Address'
                                                      'Mapper',
                                                      action='RESOLVE',
                                                      data=msg['uid'])
                    self.CFxHandle.submitCBT(mapCBT)
                    self.CBTMappings[cbt.uid].append(mapCBT.uid)

                    ip_mapCBT = self.CFxHandle.createCBT(initiator='Base'
                                                         'TopologyManager',
                                                         recipient='Address'
                                                         'Mapper',
                                                         action='QUERY_IP_MAP',
                                                         data="")
                    self.CFxHandle.submitCBT(ip_mapCBT)
                    self.CBTMappings[cbt.uid].append(ip_mapCBT.uid)

                    conn_stat_CBT = self.CFxHandle.createCBT(initiator='Base'
                                                             'TopologyManager',
                                                             recipient='Monitor',
                                                             action='QUERY_'
                                                             'CONN_STAT',
                                                             data=msg['uid'])
                    self.CFxHandle.submitCBT(conn_stat_CBT)
                    self.CBTMappings[cbt.uid].append(conn_stat_CBT.uid)

                    peer_list_CBT = self.CFxHandle.createCBT(initiator='Base'
                                                             'TopologyManager',
                                                             recipient='Monitor',
                                                             action='QUERY_'
                                                             'PEER_LIST',
                                                             data='')
                    self.CFxHandle.submitCBT(peer_list_CBT)
                    self.CBTMappings[cbt.uid].append(peer_list_CBT.uid)

                    # Add CBT to pendingCBT dictionary
                    self.pendingCBT[cbt.uid] = cbt

                # Pass for now
                elif msg_type == "send_msg":
                    pass

            elif(cbt.action == "QUERY_PEER_LIST_RESP"):

                # cbt.data is a dict of peers
                self.__link_trimmer(cbt.data)

            else:
                logCBT = self.CFxHandle.createCBT(initiator='BaseTopology'
                                                  'Manager',
                                                  recipient='Logger',
                                                  action='warning',
                                                  data="BaseTopologyManager:"
                                                  " Unrecognized CBT "
                                                  "from " + cbt.initiator)
                self.CFxHandle.submitCBT(logCBT)

        # Case when one of the requested service CBT comes back

        else:
            # Get the source CBT of this request
            sourceCBT_uid = self.checkMapping(cbt)
            self.pendingCBT[cbt.uid] = cbt
            # If all the other services of this sourceCBT are also completed,
            # process CBT here. Else wait for other CBTs to arrive
            if(self.allServicesCompleted(sourceCBT_uid)):
                if(self.pendingCBT[sourceCBT_uid].action == 'TINCAN_MSG'):
                    msg = self.pendingCBT[sourceCBT_uid].data
                    msg_type = msg.get("type", None)
                    if msg_type == "con_req" or msg_type == "con_resp":
                        for key in self.CBTMappings[sourceCBT_uid]:
                            if(self.pendingCBT[key].action ==
                                    'QUERY_IPOP_STATE_RESP'):
                                self.ipop_state = self.pendingCBT[key].data
                            elif(self.pendingCBT[key].action ==
                                    'RESOLVE_RESP'):
                                ip4 = self.pendingCBT[key].data
                            elif(self.pendingCBT[key].action ==
                                    'QUERY_CONN_STAT_RESP'):
                                conn_stat = self.pendingCBT[key].data
                            elif(self.pendingCBT[key].action ==
                                    'QUERY_PEER_LIST_RESP'):
                                peer_list = self.pendingCBT[key].data
                            elif(self.pendingCBT[key].action ==
                                    'QUERY_IP_MAP_RESP'):
                                ip_map = self.pendingCBT[key].data

                        logCBT = self.CFxHandle.createCBT(initiator='Base'
                                                          'TopologyManager',
                                                          recipient='Logger',
                                                          action='info',
                                                          data="Received"
                                                          " connection request"
                                                          "/response")
                        self.CFxHandle.submitCBT(logCBT)

                        if self.CMConfig["multihop"]:
                            conn_cnt = 0
                            for k, v in peer_list.items(): #XXX iteritems
                                if "fpr" in v and v["status"] == "online":
                                    conn_cnt += 1
                            if conn_cnt >= self.CMConfig["multihop_cl"]:
                                return
                        if self.check_collision(msg_type, msg["uid"],
                                                conn_stat):
                            return
                        fpr_len = len(self.ipop_state["_fpr"])
                        fpr = msg["data"][:fpr_len]
                        cas = msg["data"][fpr_len + 1:]
                        ip4 = ipoplib.gen_ip4(msg["uid"],
                                              ip_map,
                                              self.ipop_state["_ip4"])
                        self.create_connection(msg["uid"], fpr, 1,
                                               self.CMConfig["sec"], cas, ip4)

    def create_connection(self, uid, data, nid, sec, cas, ip4):

        conn_dict = {'uid': uid, 'fpr': data, 'nid': nid,
                     'sec': sec, 'cas': cas}
        createLinkCBT = self.CFxHandle.createCBT(initiator='BaseTopology'
                                                 'Manager',
                                                 recipient='LinkManager',
                                                 action='CREATE_LINK',
                                                 data=conn_dict)
        self.CFxHandle.submitCBT(createLinkCBT)

        cbtdata = {
            "uid": uid,
            "ip4": ip4,
        }

        TincanCBT = self.CFxHandle.createCBT(initiator='BaseTopologyManager',
                                             recipient='TincanSender',
                                             action='DO_SET_REMOTE_IP',
                                             data=cbtdata)
        self.CFxHandle.submitCBT(TincanCBT)

    def check_collision(self, msg_type, uid, conn_stat):
        if msg_type == "con_req" and \
           conn_stat == "req_sent":
            if uid > self.ipop_state["_uid"]:
                trimCBT = self.CFxHandle.createCBT(initiator='Base'
                                                   'TopologyManager',
                                                   recipient='LinkManager',
                                                   action='TRIM_LINK',
                                                   data=uid)
                self.CFxHandle.submitCBT(trimCBT)

                conn_stat_pop_CBT = self.CFxHandle.createCBT(initiator='Base'
                                                             'TopologyManager',
                                                             recipient='Monitor',
                                                             action='DELETE_'
                                                             'CONN_STAT',
                                                             data=uid)
                self.CFxHandle.submitCBT(conn_stat_pop_CBT)
            return False
        elif msg_type == "con_resp":
            conn_stat_CBT = self.CFxHandle.createCBT(initiator='Base'
                                                     'TopologyManager',
                                                     recipient='Monitor',
                                                     action='STORE_CONN_STAT',
                                                     data={
                                                          'uid': uid,
                                                          'status': "resp_recv"
                                                           })
            self.CFxHandle.submitCBT(conn_stat_CBT)
            return False
        else:
            return True

    def __link_trimmer(self, peer_list):
        for k, v in peer_list.items(): #XXX iteritems
            # Trim TinCan link if the peer is offline
            if "fpr" in v and v["status"] == "offline":
                if v["last_time"] > self.CMConfig["link_trimmer_wait_time"]:
                    trimCBT = self.CFxHandle.createCBT(initiator='Base'
                                                       'TopologyManager',
                                                       recipient='LinkManager',
                                                       action='TRIM_LINK',
                                                       data=k)
                    self.CFxHandle.submitCBT(trimCBT)

            if self.CMConfig["multihop"]:
                connection_count = 0
                for k, v in peer_list.items(): #XXX iteritems
                    if "fpr" in v and v["status"] == "online":
                        connection_count += 1
                        if connection_count > self.CMConfig["multihop_cl"]:
                            trimCBT = self.CFxHandle.createCBT(initiator='Base'
                                                               'Topology'
                                                               'Manager',
                                                               recipient='Link'
                                                               'Manager',
                                                               action='TRIM_'
                                                               'LINK',
                                                               data=k)
                            self.CFxHandle.submitCBT(trimCBT)

    def timer_method(self):
        peersCBT = self.CFxHandle.createCBT(initiator='BaseTopology'
                                            'Manager',
                                            recipient='Monitor',
                                            action='QUERY_PEER_LIST',
                                            data='')
        self.CFxHandle.submitCBT(peersCBT)

    def terminate(self):
        pass
