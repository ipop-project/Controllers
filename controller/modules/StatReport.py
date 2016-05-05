#!/usr/bin/env python
import sys
import datetime
import hashlib
import json
import logging
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule

py_ver = sys.version_info[0]

if py_ver == 3:
    import urllib.request as urllib2
else:
    import urllib2


class StatReport(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(StatReport, self).__init__(CFxHandle, paramDict, ModuleName)

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT():
        pass

    def timer_method(self):
        self.report()

    def terminate(self):
        pass

    def report(self):
        uid = self.CFxHandle.queryParam("local_uid")
        if uid == None: return
        xmpp_host = self.CFxHandle.queryParam("xmpp_host")
        xmpp_username = self.CFxHandle.queryParam("xmpp_username")
        controller = self.CFxHandle.queryParam("vpn_type")
        version = ipoplib.ipop_ver

        stat = {
            "xmpp_host" : hashlib.sha1(xmpp_host.encode('utf-8')).hexdigest(),
            "uid": hashlib.sha1(uid.encode('utf-8')).hexdigest(),
            "xmpp_username": hashlib.sha1(xmpp_username.encode('utf-8')).hexdigest(),
            "time": str(datetime.datetime.now()),
            "controller": controller,
            "version": ord(version)
        }

        data = json.dumps(stat)

        try:
            url="http://" + self.CMConfig["stat_server"] + ":" +\
                str(self.CMConfig["stat_server_port"]) + "/api/submit"
            req = urllib2.Request(url=url, data=data)
            req.add_header("Content-Type", "application/json")
            res = urllib2.urlopen(req)

            if res.getcode() == 200:
                log = "succesfully reported status to the stat-server {0}\n"\
                        "HTTP response code:{1}, msg:{2}"\
                        .format(url, res.getcode(), res.read())
                self.registerCBT('Logger', 'info', log)
            else:
                raise
        except:
            log = "statistics report failed to the stat-server ({0})".format(url)
            self.registerCBT('Logger', 'warning', log)
