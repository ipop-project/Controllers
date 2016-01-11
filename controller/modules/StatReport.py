﻿#!/usr/bin/env python

import datetime
import hashlib
import json
import logging
import urllib2

import controller.framework.ipoplib as il
#from controller.framework.CFxHandle import CFxHandle
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

    def processCBT():
        pass

    def timer_method(self):
        if self.CMConfig["stat_report"]:
            self.report()

    def terminate(self):
        if self.CMConfig["stat_report"]:
            self.report()

    def report(self):
        uid = self.CFxHandle.queryParam("local_uid")
        if uid == None: return
        xmpp_host = self.CFxHandle.queryParam("xmpp_host")
        xmpp_username = self.CFxHandle.queryParam("xmpp_username")
        controller = self.CFxHandle.queryParam("vpn_type")
        version = il.ipop_ver
        res = None

        data = json.dumps({
                "xmpp_host" : hashlib.sha1(xmpp_host).hexdigest(),\
                "uid": hashlib.sha1(uid).hexdigest(), "xmpp_username":\
                hashlib.sha1(xmpp_username).hexdigest(),\
                "time": str(datetime.datetime.now()),\
                "controller": controller,\
                "version": ord(version)})

        try:
            url="http://" + self.CMConfig["stat_server"] + ":" +\
                str(self.CMConfig["stat_server_port"]) + "/api/submit"
            req = urllib2.Request(url=url, data=data)
            req.add_header("Content-Type", "application/json")
            res = urllib2.urlopen(req)

            if res.getcode() == 200:
                logCBT = self.CFxHandle.createCBT(initiator='StatReport',
                                    recipient='Logger',
                                    action='info',
                                    data="Succesfully reported status to the "
                                    " stat-server({0}). \nHTTP response"
                                    " code:{1}, msg:{2}".format(url,\
                                    res.getcode(), res.read()))
                self.CFxHandle.submitCBT(logCBT)
            else:
                raise

        except:
            logmsg = ""
            if res != None:
                logmsg = "Statistics report failed to the stat-server({0})."\
                         ".\nHTTP response code:{1}, msg:{2}".format(url,\
                         res.getcode(), res.read())
            else:
                logmsg = "Stat server report fails. HTTP Times out"
            logCBT = self.CFxHandle.createCBT(initiator='StatReport',
                                    recipient='Logger',
                                    action='warning',
                                    data=logmsg)
            self.CFxHandle.submitCBT(logCBT)




