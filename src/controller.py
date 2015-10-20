#!/usr/bin/env python

import argparse
import binascii
import code
import ipoplib as i
import observer
import os
import json
import keyring
#import logging
import readline
import sys
import threading

CONFIG = i.CONFIG
logging = None

def setup_config(config):
    """Validate config and set default value here. Return ``True`` if config is
    changed.
    """
    if not config["local_uid"]:
        uid = binascii.b2a_hex(os.urandom(CONFIG["uid_size"] / 2))
        config["local_uid"] = uid
        return True # modified
    return False

class IpopController(observer.Observer):
    def __init__(self, argv, logger):

        super(IpopController, self).__init__("controller")
        self.observable = observer.Observable()
        self.observable.register(self)

        logger.info("say something i'm ipop")
        self.logging = logger;
        global logging
     
        logging = self.logging

        # Parsing stdin arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", help="load configuration from a file",
                        dest="config_file", metavar="config_file")
        parser.add_argument("-u", help="update configuration file if needed",
                        dest="update_config", action="store_true")
        parser.add_argument("-p", help="load remote ip configuration file",
                        dest="ip_config", metavar="ip_config")
        parser.add_argument("-s", help="configuration as json string (overrides "
                        "configuration from file)", dest="config_string", 
                        metavar="config_string")
        parser.add_argument("--pwdstdout", help="use stdout as password stream",
                        dest="pwdstdout", action="store_true")
        parser.add_argument("-i", help="Interactive mode",
                        dest="interactive", action="store_true")

        self.args = parser.parse_args(argv)

        # Take configuration file
        if self.args.config_file:
            # Load the config file
            with open(self.args.config_file) as f:
                loaded_config = json.load(f)
            CONFIG.update(loaded_config)
    
        if self.args.config_string:
            # Load the config string
            loaded_config = json.loads(args.config_string)
            CONFIG.update(loaded_config)
    
        need_save = setup_config(CONFIG)
        if need_save and self.args.config_file and self.args.update_config:
            with open(self.args.config_file, "w") as f:
                json.dump(CONFIG, f, indent=4, sort_keys=True)
    
        if not ("xmpp_username" in CONFIG and "xmpp_host" in CONFIG):
            raise ValueError("At least 'xmpp_username' and 'xmpp_host' must be "
                             "specified in config file or string")
    
        if not self.args.update_config:
            temp = keyring.get_password("ipop", CONFIG["xmpp_username"])
        if temp == None and "xmpp_password" not in CONFIG:
            prompt = "\nPassword for %s: " % CONFIG["xmpp_username"]
            if self.args.pwdstdout:
              CONFIG["xmpp_password"] = getpass.getpass(prompt, stream=sys.stdout)
            else:
              CONFIG["xmpp_password"] = getpass.getpass(prompt)
        if temp != None:
            CONFIG["xmpp_password"] = temp
        try:
            keyring.set_password("ipop", CONFIG["xmpp_username"],CONFIG["xmpp_password"])
        except:
            raise RuntimeError("Unable to store password in keyring")
    
        if "controller_logging" in CONFIG:
            level = getattr(logging, CONFIG["controller_logging"].lower())
            #try:
                #getattr(logging, "basicConfig")
                #logging.basicConfig(level=level)
            #except:
                #pass
    
        if self.args.ip_config:
            load_peer_ip_config(self.args.ip_config)
    
    def run(self):
        # Start controller thread
        if CONFIG["controller_type"] == "group_vpn":
            import groupvpn as vpn
        elif CONFIG["controller_type"] == "social_vpn":
            import socialvpn as vpn
    
        print logging
        controller = vpn.Controller(CONFIG, self.logging, self.observable)
        t = controller.run()
    
        #if CONFIG["icc"]:
        #    import icc
            #icc.start()
    
        # Start interactive mode
        if self.args.interactive:
            vars = globals().copy()
            vars.update(locals())
            shell = code.InteractiveConsole(vars)
            shell.interact()
        #else:
        #    t.join()

    def on_message(self, msg_type, msg):
        print("on_message type:{0} message:{1}".format(msg_type, msg))


    
if __name__ == "__main__":
    IpopController(sys.argv[1:]).run()

