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

import json
import time
import sys
from controller.framework.ControllerModule import ControllerModule
py_ver = sys.version_info[0]
# Check Python version and load appropriate urllib modules
if py_ver == 3:
    import urllib.request as urllib2
else:
    import urllib2


class OverlayVisualizer(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(OverlayVisualizer, self).__init__(CFxHandle, paramDict, ModuleName)
        # Counter to keep track of time lapsed
        self.interval_counter = 0
        # Visualizer webservice URL
        self.vis_address = "http://"+self.CMConfig["WebServiceAddress"]
        # Datastructure to store Node network details
        self.ipop_interface_details = {}

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))
        # Query VirtualNetwork Interface details from TincanInterface module
        ipop_interfaces = self.CFxHandle.queryParam("TincanInterface", "Vnets")
        # Create a dict of available net interfaces for collecting visualizer data
        for interface_details in ipop_interfaces:
          interface_name = interface_details["TapName"]
          self.ipop_interface_details[interface_name] = {}

    def processCBT(self, cbt):
        msg = cbt.data
        interface_name = msg.pop("interface_name")
        # Check whether TapName exists in the internal table, if not create the entry
        if interface_name not in self.ipop_interface_details.keys():
            self.ipop_interface_details[interface_name] = {}
        self.ipop_interface_details[interface_name].update(msg)

    def timer_method(self):
        # Increment the counter with every timer thread invocation
        #self.interval_counter += 1
        #if self.interval_counter % self.CMConfig["TopologyDataQueryInterval"] == 0:
        for interface_name in self.ipop_interface_details.keys():
          self.registerCBT("BaseTopologyManager", "GET_VISUALIZER_DATA", {"interface_name": interface_name})
        #if self.interval_counter % self.CMConfig["WebServiceDataPostInterval"] == 0:
        try:
            # Iterate across the IPOP interface details table to send Node network details
            for interface_name in self.ipop_interface_details.keys():
                vis_req_msg = self.ipop_interface_details[interface_name]
                if vis_req_msg:
                  vis_req_msg["node_name"] = self.CMConfig["NodeName"]
                  vis_req_msg["name"] = vis_req_msg["uid"]
                  vis_req_msg["uptime"] = int(time.time())
                  message = json.dumps(vis_req_msg).encode("utf8")
                  req = urllib2.Request(url=self.vis_address, data=message)
                  req.add_header("Content-Type", "application/json")
                  res = urllib2.urlopen(req)
                  # Check whether data has been successfully sent to the Visualizer
                  if res.getcode() != 200:
                      raise
        except Exception as err:
            log = "Failed to send data to the IPOP Visualizer webservice({0}). Exception: {1}".\
                format(self.vis_address, str(err))
            self.registerCBT('Logger', 'error', log)

    def terminate(self):
        pass
