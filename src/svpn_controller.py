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
        socks = select.select([self.sock], [], [], CONFIG["wait_time"])
        for sock in socks[0]:
            data, addr = sock.recvfrom(CONFIG["buf_size"])
            if data[0] == "{":
                msg = json.loads(data)
                logging.debug("recv %s %s" % (addr, data))
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

def setup_config(config):
    """Validate config and set default value here. Return ``True`` if config is
    changed.
    """
    if not config["local_uid"]:
        uid = binascii.b2a_hex(os.urandom(CONFIG["uid_size"] / 2))
        config["local_uid"] = uid
        return True # modified
    return False

def load_peer_ip_config(ip_config):
    with open(ip_config) as f:
        ip_cfg = json.load(f)

    for peer_ip in ip_cfg:
        uid = peer_ip["uid"]
        ip = peer_ip["ipv4"]
        IP_MAP[uid] = ip
        logging.debug("MAP %s -> %s" % (ip, uid))

def parse_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", help="load configuration from a file",
                        dest="config_file", metavar="config_file")
    parser.add_argument("-u", help="update configuration file if needed",
                        dest="update_config", action="store_true")
    parser.add_argument("-p", help="load remote ip configuration file",
                        dest="ip_config", metavar="ip_config")

    args = parser.parse_args()

    if args.config_file:
        # Load the config file
        with open(args.config_file) as f:
            loaded_config = json.load(f)
        CONFIG.update(loaded_config)

    need_save = setup_config(CONFIG)
    if need_save and args.config_file and args.update_config:
        with open(args.config_file, "w") as f:
            json.dump(CONFIG, f, indent=4, sort_keys=True)

    if not ("xmpp_username" in CONFIG and "xmpp_host" in CONFIG):
        raise ValueError("At least 'xmpp_username' and 'xmpp_host' must be "
                         "specified in config file")

    if "xmpp_password" not in CONFIG:
        prompt = "\nPassword for %s: " % CONFIG["xmpp_username"]
        CONFIG["xmpp_password"] = getpass.getpass(prompt)

    if "controller_logging" in CONFIG:
        level = getattr(logging, CONFIG["controller_logging"])
        logging.basicConfig(level=level)

    if args.ip_config:
        load_peer_ip_config(args.ip_config)


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

