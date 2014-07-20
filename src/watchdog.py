#!/usr/bin/env python

import gvpn_controller as gvpn
import json
import logging
import os
import select
import signal
import socket
import subprocess
import sys
import threading
import time

CONFIG = gvpn.CONFIG

# thread signal
run_event = threading.Event()

tincan_bin = None
tincan_process = None

# exit handler and register this process
def exit_handler(signum, frame):
    if tincan_process is not None:
        tincan_process.send_signal(signal.SIGINT)
    run_event.clear()

class TinCanException(Exception):
    def __init__(self):
        Exception.__init__(self)
    def __str__(self):
        return "TinCan not running properly."

class UdpServer(gvpn.UdpServer):

    def run_server(self):
        last_time = time.time()
        count = 0
        while run_event.is_set():
            self.serve()
            time_diff = time.time() - last_time
            if time_diff > CONFIG["wait_time"]:
                count += 1
                self.trim_connections()
                logging.debug("send_get_state")
                gvpn.do_get_state(self.sock)
                last_time = time.time()

class WatchDog:
    def __init__(self):
        if socket.has_ipv6:
            self.sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", 0))
        self.inactive_time = time.time()

    def watch(self):
        socks = select.select([self.sock], [], [], CONFIG["wait_time"])
        if not socks[0]: # Check if the socks is empty
            if time.time() - self.inactive_time > 60:
                logging.debug("TinCan is inactive for 60s")
                raise TinCanException

        for sock in socks[0]:
            data, addr = sock.recvfrom(CONFIG["buf_size"])
            msg = json.loads(data)
            logging.debug("watchdog recv %s %s" % (addr, data))
            msg_type = msg.get("type", None)
            if msg_type == "local_state":
                self.inactive_time = time.time()


def main():

    signal.signal(signal.SIGINT, exit_handler)

    watchdog = WatchDog()

    tincan_path = CONFIG["tincan_path"]

    if os.path.exists(tincan_path):
        tincan_bin = os.path.abspath(tincan_path)

        with open("core_log", "w+") as core_log:
            logging.debug("Starting ipop-tincan");
            tincan_process = subprocess.Popen([tincan_bin], 
                    stdout=subprocess.PIPE, stderr=core_log)
            time.sleep(1)

    else:
        logging.debug("TinCan binary doesn't exist at specified directory")
        sys.exit(0)

    server = UdpServer(CONFIG["xmpp_username"], CONFIG["xmpp_password"],
                     CONFIG["xmpp_host"], CONFIG["ip4"])

    
    run_event.set()
    t = threading.Thread(target=server.run_server)

    logging.debug("Starting Server");
    t.daemon = True
    t.start()
    last_time = time.time()
    watchdog.inactive_time = time.time()
    tincan_attempt = 0

    logging.debug("Starting WatchDog");
    while run_event.is_set():
        try:
            watchdog.watch()
            time_diff = time.time() - last_time
            if time_diff > CONFIG["wait_time"]:
                logging.debug("watchdog send_get_state")
                gvpn.do_get_state(watchdog.sock)
                last_time = time.time()
        except TinCanException:
            tincan_attempt += 1
            logging.error("TinCan Failed {0} times".format(tincan_attempt));
            os.kill(tincan_process.pid, signal.SIGTERM)
            time.sleep(1)
            if tincan_attempt > 3:
                logging.critical("TinCan Failed beyond threshold point");
                run_event.clear()
                break
            with open("core_log", "wb+") as core_log:
                tincan_process = subprocess.Popen([tincan_bin], 
                        stdout=subprocess.PIPE, stderr=core_log)
                time.sleep(1)
            server.ctrl_conn_init()
            watchdog.inactive_time = time.time()

if __name__  == "__main__":
    gvpn.parse_config() 
    main()
