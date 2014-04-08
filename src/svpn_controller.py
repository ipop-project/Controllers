#!/usr/bin/env python

from ipoplib import *

class SvpnUdpServer(UdpServer):
    def __init__(self, user, password, host, ip4, uid):
        UdpServer.__init__(self, user, password, host, ip4)
        self.peerlist = set()
        self.ip_map = dict(IP_MAP)
        do_set_logging(self.sock, CONFIG["tincan_logging"])
        do_set_translation(self.sock, 1)
        do_set_cb_endpoint(self.sock, self.sock.getsockname())
        do_set_local_ip(self.sock, uid, ip4, gen_ip6(uid), CONFIG["ip4_mask"],
                        CONFIG["ip6_mask"], CONFIG["subnet_mask"])
        do_register_service(self.sock, user, password, host)
        do_get_state(self.sock)

    def create_connection(self, uid, data, overlay_id, sec, cas, ip4):
        self.peerlist.add(uid)
        do_create_link(self.sock, uid, data, overlay_id, sec, cas)
        do_set_remote_ip(self.sock, uid, ip4, gen_ip6(uid))

    def trim_connections(self):
        for k, v in self.peers.iteritems():
            if "fpr" in v and v["status"] == "offline":
                if v["last_time"] > CONFIG["wait_time"] * 2:
                    do_trim_link(self.sock, k)

    def serve(self):
        socks = select.select(sock_list, [], [], CONFIG["wait_time"])
        for sock in socks[0]:
            data, addr = sock.recvfrom(CONFIG["buf_size"])
            #---------------------------------------------------------------
            #| offset(byte) |                                              |
            #---------------------------------------------------------------
            #|      0       | ipop version                                 |
            #|      1       | message type                                 |
            #|      2       | Payload (JSON formatted control message)     |
            #---------------------------------------------------------------
            if data[0] != ipop_ver:
                logging.debug("ipop version mismatch: tincan:{0} controller:{1}"
                              "".format(data[0].encode("hex"), \
                                   ipop_ver.encode("hex")))
                sys.exit()
            if data[1] == tincan_control:
                msg = json.loads(data[2:])
                logging.debug("recv %s %s" % (addr, data[2:]))
                msg_type = msg.get("type", None)

                if msg_type == "local_state":
                    self.state = msg
                elif msg_type == "peer_state":
                    self.peers[msg["uid"]] = msg
                    self.trigger_conn_request(msg)
                # we ignore connection status notification for now
                elif msg_type == "con_stat": pass
                elif msg_type == "con_req" or msg_type == "con_resp":
                    if self.check_collision(msg_type, msg["uid"]): continue
                    fpr_len = len(self.state["_fpr"])
                    fpr = msg["data"][:fpr_len]
                    cas = msg["data"][fpr_len + 1:]
                    ip4 = gen_ip4(msg["uid"], self.ip_map, self.state["_ip4"])
                    self.create_connection(msg["uid"], fpr, 1, CONFIG["sec"],
                                           cas, ip4)

def main():
    parse_config()
    server = SvpnUdpServer(CONFIG["xmpp_username"], CONFIG["xmpp_password"],
                       CONFIG["xmpp_host"], CONFIG["ip4"], CONFIG["local_uid"])
    last_time = time.time()
    while True:
        server.serve()
        time_diff = time.time() - last_time
        if time_diff > CONFIG["wait_time"]:
            server.trim_connections()
            do_get_state(server.sock)
            last_time = time.time()

if __name__ == "__main__":
    main()

