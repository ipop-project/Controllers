# ipop-project
# Copyright 2016, University of Florida
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

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
    def __init__(self, cfx_handle, module_config, module_name):
        super(StatReport, self).__init__(cfx_handle, module_config, module_name)
        self._stat_data = {"ready": False}
        self.submit_time = 0

    def initialize(self):
        self.register_cbt('Logger', 'info', "{0} Loaded".format(self._module_name))

    def process_cbt(self, cbt):
        if cbt.OpType == "Response":
            if (cbt.Response.Status == False):
                self.registerCBT("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.Response.Message))
                self.FreeCBT(cbt)
                return
            else:
                self.create_report(cbt)
        else:
            log = "Unsupported CBT OpType {0}".format(cbt)
            self.registerCBT('Logger', 'LOG_WARNING', log)


    def timer_method(self):
        cur_time = datetime.datetime.now()
        if self._stat_data["data_ready"]:
            self.submit_report()
            print(self._stat_data)
            self.submit_time = datetime.datetime.now()
            self._stat_data = {"ready": False}
        elif cur_time > self.submit_time:
            self.request_report()

    def terminate(self):
        pass

    def request_report(self):
        self.registerCBT("Signal","QUERY_REPORTING_DATA")
        


    def create_report(self, cbt):
        nid = self.CMConfig["NodeId"]
        report_data = cbt.Response.Data
        for overlay_id in report_data:
            report_data[overlay_id] = {
                "xmpp_host": hashlib.sha1(report_data[overlay_id]["xmpp_host"].encode('utf-8')).hexdigest(),
                "xmpp_username": hashlib.sha1(report_data[overlay_id]["xmpp_username"].encode('utf-8')).hexdigest(),
            }
        
        stat = {
            "NodeId": hashlib.sha1(nid.encode('utf-8')).hexdigest(),
            "Time": str(datetime.datetime.now()),
            "Model": self.CMConfig["Model"]
            "Version": self.CFxHandle.queryparam("CFx","Version"), # currently not supported
            report_data
        }
        self._stat_data["data"] = stat
        self._stat_data["ready"] = True

        
        
    def submit_report(self,):
        data = json.dumps(_stat_data["data"])
        url = None
        try:
            url = "http://" + self._cm_config["StatServerAddress"] + ":" +\
                str(self._cm_config["StatServerPort"]) + "/api/submit"
            req = urllib2.Request(url=url, data=data)
            req.add_header("Content-Type", "application/json")
            res = urllib2.urlopen(req)

            if res.getcode() == 200:
                log = "succesfully reported status to the stat-server {0}\n"\
                        "HTTP response code:{1}, msg:{2}"\
                        .format(url, res.getcode(), res.read())
                self.register_cbt('Logger', 'info', log)
            else:
                raise
        except Exception as error:
            log = "statistics report failed to the stat-server ({0}).Error: {1}".format(url, error)
            self.register_cbt('Logger', 'warning', log)
