#!/usr/bin/env python

import datetime
import hashlib
import json
import logging
import urllib2

import controller.framework.ipoplib as il
from controller.framework.ControllerModule import ControllerModule

class StatReport(ControllerModule):

    def __init__(self, CFxHandle, paramDict):

        super(StatReport, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict

    def initialize(self):

        logCBT = self.CFxHandle.createCBT(initiator='StatReport',
                                          recipient='Logger',
                                          action='info',
                                          data="StatReport Loaded")
        self.CFxHandle.submitCBT(logCBT)
        self.report()

    def processCBT():
        pass

    def timer_method(self):
        self.report()
        pass

    def terminate(self):
        self.report()
        pass

    def report(self):
        if il.CONFIG["uid"] == None: return
        data = json.dumps({
                "xmpp_host" : hashlib.sha1(il.CONFIG["CFx"]["xmpp_host"]).hexdigest(),\
                "uid": hashlib.sha1(il.CONFIG["uid"]).hexdigest(), "xmpp_username":\
                hashlib.sha1(il.CONFIG["CFx"]["xmpp_username"]).hexdigest(),\
                "time": str(datetime.datetime.now()),\
                "controller": il.CONFIG["CFx"]["vpn_type"],\
                "version": ord(il.CONFIG["ipop_ver"])})

        try:
            url="http://" + il.CONFIG["CFx"]["stat_server"] + ":" +\
                str(il.CONFIG["CFx"]["stat_server_port"]) + "/api/submit"
            req = urllib2.Request(url=url, data=data)
            req.add_header("Content-Type", "application/json")
            res = urllib2.urlopen(req)
            logging.debug("Succesfully reported status to the stat-server({0})."
              ".\nHTTP response code:{1}, msg:{2}".format(url, res.getcode(),\
              res.read()))
            if res.getcode() != 200:
                raise
        except:
            logging.debug("Status report failed.")




