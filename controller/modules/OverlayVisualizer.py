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

try:
    import simplejson as json
except ImportError:
    import json
import time
import sys
import threading
from collections import defaultdict

from controller.framework.ControllerModule import ControllerModule

import requests

py_ver = sys.version_info[0]
# Check Python version and load appropriate urllib modules
if py_ver == 3:
    import urllib.request as urllib2
else:
    import urllib2


class OverlayVisualizer(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(OverlayVisualizer, self).__init__(cfx_handle, module_config, module_name)
        # Counter to keep track of time lapsed
        self.interval_counter = 0
        # Visualizer webservice URL
        self.vis_address = "http://"+self._cm_config["WebServiceAddress"]
        # Datastructure to store Node network details
        self.ipop_interface_details = {}

        self.node_id = str(self._cfx_handle.query_param("CFx", "NodeId"))
        # The visualizer dataset which is forwarded to the collector service
        self.vis_ds = dict(NodeId=self.node_id, Data=defaultdict(dict))
        # Its lock
        self.vis_ds_lock = threading.Lock()

    def initialize(self):
        self.register_cbt('Logger', 'info', "{0} Loaded".format(self._module_name))
        # Query VirtualNetwork Interface details from TincanInterface module

        ipop_interfaces = self._cfx_handle.query_param("TincanInterface", "Vnets")
        # Create a dict of available net interfaces for collecting visualizer data
        for interface_details in ipop_interfaces:
          interface_name = interface_details["TapName"]
          self.ipop_interface_details[interface_name] = {}

        # We're using the pub-sub model here to gather data for the visualizer
        # from other modules
        # Using this publisher, the OverlayVisualizer publishes events in the
        # timer_method() and all subscribing modules are expected to reply
        # with the data they want to forward to the visualiser
        self.vis_req_publisher = self._cfx_handle.publish_subscription("VIS_DATA_REQ")

    def process_cbt(self, cbt):
        msg = cbt.response.data

        # self.vis_ds belongs to the critical section as
        # it may be updated in timer_method concurrently
        self.vis_ds_lock.acquire()
        for mod_name in msg:
            for ovrl_id in msg[mod_name]:
                self.vis_ds["Data"][ovrl_id][mod_name] \
                        = msg[mod_name][ovrl_id]
        self.vis_ds_lock.release()

    def timer_method(self):
        # to keep track of requests which failed
        failed_reqs = dict()

        try:
            self.vis_ds_lock.acquire()
            print "Visualizer is going to send" \
                    " {}".format(json.dumps(self.vis_ds))
            req_url = "{}/IPOP/nodes/{}".format(self.vis_address, self.node_id)
            resp = requests.put(req_url, data=json.dumps(self.vis_ds),
                          headers={"Content-Type": "application/json"})
            resp.raise_for_status()

            # flush old data if request was successful
            self.vis_ds = dict(NodeId=self.node_id, Data=defaultdict(dict))
        except requests.exceptions.RequestException as err:
            # collect failed request
            # failed_reqs[ovrl_id] = ovrl_data
            log = "Failed to send data to the IPOP Visualizer" \
                    " webservice({0}). Exception: {1}" \
                            .format(self.vis_address, str(err))
            self.registerCBT('Logger', 'error', log)
        finally:
            self.vis_ds_lock.release()

        # Now that all the accumulated data has been dealth with, we request
        # more data
        self.vis_req_publisher.post_update(None)

    def terminate(self):
        pass
