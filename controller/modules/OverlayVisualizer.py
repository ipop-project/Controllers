#!/usr/bin/env python
import json,time,sys
from controller.framework.ControllerModule import ControllerModule
py_ver = sys.version_info[0]
if py_ver == 3:
    import urllib.request as urllib2
else:
    import urllib2


class OverlayVisualizer(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(OverlayVisualizer, self).__init__(CFxHandle, paramDict, ModuleName)
        self.interval_counter = 0
        self.vis_address = "http://"+self.CMConfig["WebServiceAddress"]
        self.CMConfig = paramDict
        self.ipop_interface_details = {}

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        msg  = cbt.data
        interface_name =  msg.pop("interface_name")
        if interface_name not in self.ipop_interface_details.keys():
            self.ipop_interface_details[interface_name] = {}
        self.ipop_interface_details[interface_name].update(msg)
        #self.registerCBT("Logger", "info", "IPOP_UI_DETAILS" + str(self.ipop_interface_details))

    def timer_method(self):

        self.interval_counter +=1
        if self.interval_counter % self.CMConfig["TopologyDataQueryInterval"] == 0 :
            self.registerCBT("BaseTopologyManager", "get_visualizer_data","")
            self.registerCBT("ConnectionManager", "get_visualizer_data", "")

        if self.interval_counter % self.CMConfig["WebServiceDataPostInterval"] == 0 :
            try:
                for interface_name in self.ipop_interface_details.keys():
                    vis_req_msg = self.ipop_interface_details[interface_name]
                    vis_req_msg["node_name"] = self.CMConfig["NodeName"]
                    vis_req_msg["name"] = vis_req_msg["uid"]
                    vis_req_msg["uptime"] = int(time.time())
                    message = json.dumps(vis_req_msg).encode("utf8")

                    req = urllib2.Request(url=self.vis_address, data=message)
                    req.add_header("Content-Type", "application/json")
                    res = urllib2.urlopen(req)


                    if res.getcode() != 200:
                        raise
            except Exception as err:
                log = "Failed to send data to the IPOP Visualizer webservice({0}). Exception: {1}".format(self.vis_address, str(err))
                self.registerCBT('Logger', 'error', log)

    def terminate(self):
        pass
