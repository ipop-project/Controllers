#!/usr/bin/env python

from ipoplib import *

class SvpnUdpServer(UdpServer):
    def __init__(self, user, password, host, ip4, uid):
        UdpServer.__init__(self, user, password, host, ip4)
        self.uid = uid
        self.ip4 = ip4
        self.ip6 = gen_ip6(uid)
        self.vpn_type = "SocialVPN"
        self.peerlist = set()
        self.ip_map = dict(IP_MAP)
        do_set_logging(self.sock, CONFIG["tincan_logging"])
        do_set_translation(self.sock, 1)
        do_set_cb_endpoint(self.sock, self.sock.getsockname())
        do_set_local_ip(self.sock, uid, ip4, gen_ip6(uid), CONFIG["ip4_mask"],
                        CONFIG["ip6_mask"], CONFIG["subnet_mask"], 0)
        do_register_service(self.sock, user, password, host)
        do_set_trimpolicy(self.sock, CONFIG["trim_enabled"])
        do_get_state(self.sock, stats=False)
        if CONFIG["icc"]:
            self.inter_controller_conn()
            self.lookup_req = {}
        if "network_ignore_list" in CONFIG:
            logging.debug("network ignore list")
            make_call(self.sock, m="set_network_ignore_list",\
                             network_ignore_list=CONFIG["network_ignore_list"])

    def create_connection(self, uid, data, overlay_id, sec, cas, ip4):
        self.peerlist.add(uid)
        do_create_link(self.sock, uid, data, overlay_id, sec, cas)
        do_set_remote_ip(self.sock, uid, ip4, gen_ip6(uid))

    def trim_connections(self):
        for k, v in self.peers.iteritems():
            if "fpr" in v and v["status"] == "offline":
                if v["last_time"] > CONFIG["wait_time"] * 2:
                    do_trim_link(self.sock, k)
        if CONFIG["multihop"]: 
            connection_count = 0 
            for k, v in self.peers.iteritems():
                if "fpr" in v and v["status"] == "online":
                    connection_count += 1
                    if connection_count > CONFIG["multihop_cl"]:
                        do_trim_link(self.sock, k)
        

    def serve(self):
        socks, _, _ = select.select(self.sock_list, [], [], CONFIG["wait_time"])
        for sock in socks:
            if sock == self.sock or sock == self.sock_svr:
                data, addr = sock.recvfrom(CONFIG["buf_size"])
                #---------------------------------------------------------------
                #| offset(byte) |                                              |
                #---------------------------------------------------------------
                #|      0       | ipop version                                 |
                #|      1       | message type                                 |
                #|      2       | Payload (JSON formatted control message)     |
                #---------------------------------------------------------------
                if data[0] != ipop_ver:
                    logging.debug("ipop version mismatch: tincan:{0} controller"
                                  ":{1}".format(data[0].encode("hex"), \
                                       ipop_ver.encode("hex")))
                    sys.exit()
                if data[1] == tincan_control:
                    msg = json.loads(data[2:])
                    logging.debug("recv %s %s" % (addr, data[2:]))
                    msg_type = msg.get("type", None)
                    if msg_type == "echo_request":
                        make_remote_call(self.sock_svr, m_type=tincan_control,\
                          dest_addr=addr[0], dest_port=addr[1], payload=None,\
                          type="echo_reply")
                    if msg_type == "local_state":
                        self.ipop_state = msg
                    elif msg_type == "peer_state":
                        uid = msg["uid"]
                        if msg["status"] == "online": 
                            self.peers_ip4[msg["ip4"]] = msg
                            self.peers_ip6[msg["ip6"]] = msg
                        else:
                            if uid in self.peers and\
                              self.peers[uid]["status"]=="online":
                                del self.peers_ip4[self.peers[uid]["ip4"]]
                                del self.peers_ip6[self.peers[uid]["ip6"]]
                        self.peers[uid] = msg
                        self.trigger_conn_request(msg)
                    # we ignore connection status notification for now
                    elif msg_type == "con_stat": pass
                    elif msg_type == "con_req" or msg_type == "con_resp":
                        if CONFIG["multihop"]:
                            conn_cnt = 0
                            for k, v in self.peers.iteritems():
                                if "fpr" in v and v["status"] == "online":
                                    conn_cnt += 1
                            if conn_cnt >= CONFIG["multihop_cl"]:
                                continue
                        if self.check_collision(msg_type, msg["uid"]): continue
                        fpr_len = len(self.ipop_state["_fpr"])
                        fpr = msg["data"][:fpr_len]
                        cas = msg["data"][fpr_len + 1:]
                        ip4 = gen_ip4(msg["uid"],self.ip_map,self.ipop_state["_ip4"])
                        self.create_connection(msg["uid"], fpr, 1,\
                                               CONFIG["sec"], cas, ip4)
                    return

                #|-------------------------------------------------------------|
                #| offset(byte) |                                              |
                #|-------------------------------------------------------------|
                #|      0       | ipop version                                 |
                #|      1       | message type                                 |
                #|      2       | source uid                                   |
                #|     22       | destination uid                              |
                #|     42       | Payload (Ethernet frame)                     |
                #|-------------------------------------------------------------|
                elif data[1] == tincan_packet:
                    # At this point, we only handle ipv6 packet
                    if data[54:56] == "\x86\xdd" and data[80:82] != "\xff\x02"\
                       and CONFIG["multihop"]:
                        logging.pktdump("Destination unknown packet", dump=data)
                        dest_ip6=ip6_b2a(data[80:96])
                        if dest_ip6 in self.far_peers:
                            logging.pktdump("Destination({0}) packet is in far" 
                                  "peers({1})".format(dest_ip6, self.far_peers))
                            next_hop_addr = self.far_peers[dest_ip6]["via"][1] 
                            if not next_hop_addr in self.peers_ip6:
                                del self.far_peers[dest_ip6]
                                self.lookup(dest_ip6)
                                return
                            if CONFIG["multihop_sr"]: #Source routing
                                # Attach all the ipv6 address of hop in the 
                                payload = tincan_sr6 # Multihop packet
                                payload = "\x01" # Hop Index
                                payload += chr(self.far_peers[dest_ip6]\
                                                    ["hop_count"]+1) # Hop Count
                                for hop in self.far_peers[dest_ip6]["via"]:
                                    payload += ip6_a2b(hop)
                                payload += data[80:96] 
                                payload += data[42:]
                                #send packet to the next hop
                                logging.pktdump("sending", dump=payload)
                                make_remote_call(sock=self.cc_sock,\
                                  dest_addr=self.far_peers[dest_ip6]["via"][1],\
                                  dest_port=CONFIG["icc_port"],\
                                  m_type=tincan_sr6, payload=payload)
                            else:
                                # Non source route mode
                                make_remote_call(sock=self.cc_sock, \
                                  dest_addr=self.far_peers[dest_ip6]["via"],\
                                  dest_port=CONFIG["icc_port"],\
                                  m_type=tincan_packet, payload=data[42:])
                        else:
                            # Destination is not known, we flood lookup_req msg
                            self.lookup(dest_ip6)
                    return

            elif sock == self.cc_sock and CONFIG["multihop"]:
                data, addr = sock.recvfrom(CONFIG["buf_size"])
                logging.pktdump("Packet received from {0}".format(addr))
                self.multihop_handle(data)

    
def main():
    parse_config()
    server = SvpnUdpServer(CONFIG["xmpp_username"], CONFIG["xmpp_password"],
                       CONFIG["xmpp_host"], CONFIG["ip4"], CONFIG["local_uid"])
    set_global_variable_server(server)
    if CONFIG["stat_report"]:
        server.report()
    last_time = time.time()
    while True:
        server.serve()
        time_diff = time.time() - last_time
        if time_diff > CONFIG["wait_time"]:
            server.trim_connections()
            do_get_state(server.sock, stats=False)
            last_time = time.time()

if __name__ == "__main__":
    main()

