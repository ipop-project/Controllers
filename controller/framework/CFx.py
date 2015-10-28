#!/usr/bin/env python

import os
import sys
import json
import time
import signal
import socket
import ipoplib
import argparse
import binascii
import threading
import importlib
from getpass import getpass
from collections import OrderedDict
from CBT import CBT as _CBT
from CFxHandle import CFxHandle


class CFX(object):

    def __init__(self):

        self.parse_config()
        ipoplib.CONFIG = self.CONFIG

        # CFxHandleDict is a dict containing the references to
        # CFxHandles of all CMs with key as the module name and
        # value as the CFxHandle reference
        self.CFxHandleDict = {}

        self.vpn_type = self.CONFIG['CFx']['vpn_type']
        self.user = self.CONFIG['CFx']["xmpp_username"]
        self.password = self.CONFIG['CFx']["xmpp_password"]
        self.host = self.CONFIG['CFx']["xmpp_host"]
        
        if(self.vpn_type == 'GroupVPN'):
            self.ip4 = self.CONFIG['BaseTopologyManager']["ip4"]
            self.uid = ipoplib.gen_uid(self.ip4)  # SHA-1 Hash
        elif(self.vpn_type == 'SocialVPN'):
            self.ip4 = self.CONFIG['AddressMapper']["ip4"]
            self.uid = self.CONFIG['CFx']['local_uid']
        self.ip6 = ipoplib.gen_ip6(self.uid)

        if socket.has_ipv6:
            self.sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self.sock_svr = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self.sock_svr.bind((self.CONFIG['TincanSender']["localhost6"],
                                self.CONFIG['CFx']["contr_port"]))
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_svr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_svr.bind((self.CONFIG['TincanSender']["localhost"],
                                self.CONFIG['CFx']["contr_port"]))
        self.sock.bind(("", 0))
        self.sock_list = [self.sock, self.sock_svr]

    def submitCBT(self, CBT):

        recipient = CBT.recipient
        self.CFxHandleDict[recipient].CMQueue.put(CBT)

    def createCBT(self, initiator='', recipient='', action='', data=''):

        # Create and return an empty CBT. The variables of the CBT
        # will be assigned by the CM
        cbt = _CBT(initiator, recipient, action, data)
        return cbt

    def freeCBT(self):

        # Deallocate the CBT here
        # Python automatic garbage collector handles it anyway
        pass

    def initialize(self,):

        # Make Tincan API calls to initialize the controller

        # Set logging level
        ipoplib.do_set_logging(self.sock, self.CONFIG["CFx"]["tincan_logging"])

        if(self.vpn_type == "GroupVPN"):
            ipoplib.do_set_translation(self.sock, 0)
            ipoplib.do_set_switchmode(self.sock,
                                      self.CONFIG["TincanSender"]
                                      ["switchmode"])
        elif(self.vpn_type == "SocialVPN"):
            ipoplib.do_set_translation(self.sock, 1)

        # Callback endpoint to receive notifications
        ipoplib.do_set_cb_endpoint(self.sock, self.sock.getsockname())

        # Configure the local node
        if not self.CONFIG["CFx"]["router_mode"]:
            ipoplib.do_set_local_ip(self.sock, self.uid, self.ip4,
                                    self.ip6,
                                    self.CONFIG["CFx"]["ip4_mask"],
                                    self.CONFIG["CFx"]["ip6_mask"],
                                    self.CONFIG["CFx"]["subnet_mask"],
                                    self.CONFIG["TincanSender"]["switchmode"])

        else:
            ipoplib.do_set_local_ip(self.sock, self.uid,
                                    self.CONFIG["CFx"]["router_ip"],
                                    self.ip6,
                                    self.CONFIG["CFx"]["router_ip4_mask"],
                                    self.CONFIG["CFx"]["router_ip6_mask"],
                                    self.CONFIG["CFx"]["subnet_mask"],
                                    self.CONFIG["TincanSender"]["switchmode"])

        # Create ICC socket
        if self.CONFIG['CFx']["icc"]:
            if socket.has_ipv6:
                self.sock_icc = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
                while True:
                    try:
                        time.sleep(3)
                        self.sock_icc.bind((self.ip6, self.CONFIG['CFx']["icc_port"]))
                    except KeyboardInterrupt:
                        self.terminate()
                    except Exception as e:
                        print("Wait until IPOP Tap is available")
                        continue
                    else:
                        break

                self.sock_list.append(self.sock_icc)

            else:
                print "ICC is enabled but IPv6 is not supported. Exiting"
                sys.exit()

        # Register to the XMPP server
        ipoplib.do_register_service(self.sock, self.user,
                                    self.password, self.host)
        ipoplib.do_set_trimpolicy(self.sock,
                                  self.CONFIG["CFx"]["trim_enabled"])

        # Retrieve the state of the local node
        ipoplib.do_get_state(self.sock)

        # Ignore the network interfaces in the list
        if "network_ignore_list" in self.CONFIG["CFx"]:
            ipoplib.make_call(self.sock, m="set_network_ignore_list",
                              network_ignore_list=CONFIG["CFx"]
                              ["network_ignore_list"])

        print "CFx initialized. Loading Controller Modules\n"

        self.loaded_modules = ['CFx']  # List of modules already loaded

        # Check for circular dependencies in config.json
        dependency_graph = {}
        for key in self.json_data:
            if(key != 'CFx'):
                try:
                    dependency_graph[key] = self.json_data[key]['dependencies']
                except:
                    pass

        if(self.detect_cyclic_dependency(dependency_graph)):
            print "Circular dependency detected in config.json. Exiting"
            sys.exit()

        # Iterate through the modules mentioned in config.json
        # and load them.
        for key in self.json_data:
            if (key not in self.loaded_modules):
                self.load_module(key)

        # Start all the worker and timer threads
        for handle in self.CFxHandleDict:
            self.CFxHandleDict[handle].CMThread.start()
            if(self.CFxHandleDict[handle].timer_thread):
                self.CFxHandleDict[handle].timer_thread.start()

    def load_module(self, module_name):

        if(module_name not in self.loaded_modules):

            # Load dependencies of the module
            self.load_dependencies(module_name)

            # Dynamically importing the modules
            try:
                module = importlib.import_module("controller.modules."+module_name)
            except:
                if(self.vpn_type == "GroupVPN"):
                    module = importlib.import_module("controller.modules.gvpn."+module_name)
                elif(self.vpn_type == "SocialVPN"):
                    module = importlib.import_module("controller.modules.svpn."+module_name)

            # Get the class with name key from module
            module_class = getattr(module, module_name)

            # Create a CFxHandle object for each module
            handle = CFxHandle(self)

            # Instantiate the class, with CFxHandle reference and
            # configuration parameters

            # Append the icc flag and icc port to the modules config and
            # pass the sock_list as parameter to TincanListener
            # and TincanSender modules
            if(module_name in ['TincanListener', 'TincanSender']):
                self.CONFIG[module_name]["icc"] = self.CONFIG['CFx']["icc"]
                self.CONFIG[module_name]["icc_port"] = self.CONFIG['CFx']["icc_port"]

                instance = module_class(self.sock_list,
                                        handle,
                                        self.CONFIG[module_name])
            else:
                instance = module_class(handle, self.CONFIG[module_name])

            handle.CMInstance = instance
            handle.CMConfig = self.CONFIG[module_name]

            # Store the CFxHandle object references in the
            # dict with module name as the key
            self.CFxHandleDict[module_name] = handle

            # Intialize all the CFxHandles which in turn initialize the CMs
            handle.initialize()

            self.loaded_modules.append(module_name)

    def load_dependencies(self, module_name):

        # Load dependencies of the module, if specified in config.json
        try:
            dependencies = self.json_data[module_name]['dependencies']
            for module in dependencies:
                if(module not in self.loaded_modules):
                    self.load_module(module)
        except KeyError:
            pass

    def detect_cyclic_dependency(self, g):

        # Return True if the directed graph g has a cycle.
        path = set()

        def visit(vertex):
            path.add(vertex)
            for neighbour in g.get(vertex, ()):
                if neighbour in path or visit(neighbour):
                    return True
            path.remove(vertex)
            return False

        return any(visit(v) for v in g)

    def __handler(self, signum=None, frame=None):

        print 'Signal handler called with signal ' + str(signum)

    def parse_config(self):

        self.CONFIG = ipoplib.CONFIG

        parser = argparse.ArgumentParser()
        parser.add_argument("-c", help="load configuration from a file",
                            dest="config_file", metavar="config_file")
        parser.add_argument("-u", help="update configuration file if needed",
                            dest="update_config", action="store_true")
        parser.add_argument("-p", help="load remote ip configuration file",
                            dest="ip_config", metavar="ip_config")
        parser.add_argument("-s", help="configuration as json string"
                            " (overrides configuration from file)",
                            dest="config_string", metavar="config_string")
        parser.add_argument("--pwdstdout", help="use stdout as "
                            "password stream",
                            dest="pwdstdout", action="store_true")

        args = parser.parse_args()

        if args.config_file:
            # Load the config file
            with open(args.config_file) as f:

                # Read config file into an OrderedDict, to load the
                # modules in the order in which they appear in config.json
                self.json_data = json.load(f, object_pairs_hook=OrderedDict)
                for key in self.json_data:
                    if(self.CONFIG.get(key, None)):
                        self.CONFIG[key].update(self.json_data[key])

        if args.config_string:
            # Load the config string
            loaded_config = json.loads(args.config_string)
            for key in loaded_config:
                if(self.CONFIG.get(key, None)):
                    self.CONFIG[key].update(loaded_config[key])

        need_save = self.setup_config(self.CONFIG)
        if need_save and args.config_file and args.update_config:
            with open(args.config_file, "w") as f:
                json.dump(self.CONFIG, f, indent=4, sort_keys=True)

        if not ("xmpp_username" in self.CONFIG["CFx"] and
                "xmpp_host" in self.CONFIG["CFx"]):
            raise ValueError("At least 'xmpp_username' and 'xmpp_host' "
                             "must be specified in config file or string")

        if "xmpp_password" not in self.CONFIG["CFx"]:
            prompt = "\nPassword for %s:" % self.CONFIG["CFx"]["xmpp_username"]
            if args.pwdstdout:
                self.CONFIG["CFx"]["xmpp_password"] = getpass(prompt,
                                                              stream=sys.stdout)
            else:
                self.CONFIG["CFx"]["xmpp_password"] = getpass(prompt)

        if args.ip_config:
            ipoplib.load_peer_ip_config(args.ip_config)

    def setup_config(self, config):
        """Validate config and set default value here. Return ``True`` if config is
        changed.
        """
        if not config['CFx']['local_uid']:
            uid = binascii.b2a_hex(os.urandom(self.CONFIG['CFx']
                                              ['uid_size'] / 2))
            self.CONFIG['CFx']["local_uid"] = uid
            return True  # modified
        return False

    def waitForShutdownEvent(self):

        self.event = threading.Event()

        # Since signal.pause() is not avaialble on windows, use event.wait()
        # with a timeout to catch KeyboardInterrupt. Without timeout, it's
        # not possible to catch KeyboardInterrupt because event.wait() is
        # a blocking call without timeout. The if condition checks if the os
        # is windows.
        if(os.name == 'nt'):

            while(True):
                try:
                    self.event.wait(1)
                except KeyboardInterrupt, SystemExit:
                    break

        else:

            for sig in [signal.SIGINT]:
                signal.signal(sig, self.__handler)

            # signal.pause() sleeps until SIGINT is received
            signal.pause()

    def terminate(self):

        for key in self.CFxHandleDict:

            # Create a special terminate CBT to terminate all the CMs
            terminateCBT = self.createCBT('CFx', key, 'TERMINATE', '')

            # Clear all the queues and put the terminate CBT in all the queues
            self.CFxHandleDict[key].CMQueue.queue.clear()

            self.submitCBT(terminateCBT)

        # Wait for the threads to process their current CBTs and exit
        print "Waiting for timer threads to exit gracefully..."
        for handle in self.CFxHandleDict:
            if(self.CFxHandleDict[handle].joinEnabled):
                self.CFxHandleDict[handle].CMThread.join()
                self.CFxHandleDict[handle].timer_thread.join()

        sys.exit(0)


def main():

    CFx = CFX()
    CFx.initialize()
    CFx.waitForShutdownEvent()
    CFx.terminate()

if __name__ == "__main__":
    main()
