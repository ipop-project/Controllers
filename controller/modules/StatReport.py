#!/usr/bin/env python
import sys
import datetime
import hashlib
import json
import controller.framework.fxlib as fxlib
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

    def processCBT(self, cbt):
        pass

    def timer_method(self):
        self.report()

    def terminate(self):
        pass

    def report(self):
        uid = self.CFxHandle.queryParam("CFx", "local_uid")
        if uid is None:
            return
        xmpp_host = self.CFxHandle.queryParam("XmppClient", "AddressHost")
        xmpp_username = self.CFxHandle.queryParam("XmppClient", "Username")
        controller = self.CFxHandle.queryParam("CFx", "Model")
        version = fxlib.ipopVerRel

        stat = {
            "xmpp_host": hashlib.sha1(xmpp_host.encode('utf-8')).hexdigest(),
            "uid": hashlib.sha1(uid.encode('utf-8')).hexdigest(),
            "xmpp_username": hashlib.sha1(xmpp_username.encode('utf-8')).hexdigest(),
            "time": str(datetime.datetime.now()),
            "controller": controller,
            "version": ord(version)
        }
        data = json.dumps(stat)
        url = None
        try:
            url = "http://" + self.CMConfig["StatServerAddress"] + ":" +\
                str(self.CMConfig["StatServerPort"]) + "/api/submit"
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
        except Exception as error:
            log = "statistics report failed to the stat-server ({0}).Error: {1}".format(url, error)
            self.registerCBT('Logger', 'warning', log)
