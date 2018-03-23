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
import threading
from collections import defaultdict
from controller.framework.ControllerModule import ControllerModule
import requests


class OverlayVisualizer(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(OverlayVisualizer, self).__init__(cfx_handle,
                                                module_config, module_name)
        # Visualizer webservice URL
        self.vis_address = "http://" + self._cm_config["WebServiceAddress"]

        self.node_id = str(self._cm_config["NodeId"])

        # The visualizer dataset which is forwarded to the collector service
        self._vis_ds = dict(NodeId=self.node_id, Data=defaultdict(dict))
        # Its lock
        self._vis_ds_lock = threading.Lock()

    def initialize(self):
        # We're using the pub-sub model here to gather data for the visualizer
        # from other modules
        # Using this publisher, the OverlayVisualizer publishes events in the
        # timer_method() and all subscribing modules are expected to reply
        # with the data they want to forward to the visualiser
        self._vis_req_publisher = \
            self._cfx_handle.publish_subscription("VIS_DATA_REQ")

        self.register_cbt("Logger", "LOG_INFO",
                          "{0} Module loaded".format(self._module_name))

    def process_cbt(self, cbt):
        if cbt.op_type == "Response":
            if cbt.request.action == "VIS_DATA_REQ":
                msg = cbt.response.data

                if msg:
                    # self._vis_ds belongs to the critical section as
                    # it may be updated in timer_method concurrently
                    self._vis_ds_lock.acquire()
                    for mod_name in msg:
                        for ovrl_id in msg[mod_name]:
                            self._vis_ds["Data"][ovrl_id][mod_name] \
                                = msg[mod_name][ovrl_id]
                    self._vis_ds_lock.release()
                else:
                    warn_msg = "Got no data in CBT response from module" \
                        " {}".format(cbt.request.recipient)
                    self.register_cbt("Logger", "LOG_WARNING", warn_msg)
                self.free_cbt(cbt)
        else:
                self.req_handler_default(cbt)

    def timer_method(self):
        with self._vis_ds_lock:
            vis_ds = self._vis_ds
            # flush old data, next itr provides new data
            self._vis_ds = dict(NodeId=self.node_id,
                                Data=defaultdict(dict))

        collector_msg = dict(Data=dict())

        # Filter out overlays for which we do not have LinkManager data
        for overlay_id in vis_ds["Data"]:
            overlay_data = vis_ds["Data"][overlay_id]
            if "LinkManager" in overlay_data and overlay_data["LinkManager"]:
                collector_msg["Data"][overlay_id] = overlay_data

        if collector_msg["Data"]:

            # Read the optional human-readable node name specified in the
            # configuration and pass it along to the collector
            if "NodeName" in self._cm_config:
                collector_msg["NodeName"] = self._cm_config["NodeName"]

            data_log = "Visualizer is going to send" \
                " {}".format(collector_msg)
            self.register_cbt("Logger", "LOG_DEBUG", data_log)

            req_url = "{}/IPOP/nodes/{}".format(self.vis_address, self.node_id)

            try:
                resp = requests.put(req_url,
                                    data=json.dumps(collector_msg),
                                    headers={"Content-Type":
                                             "application/json"},
                                    timeout=3)
                resp.raise_for_status()

            except requests.exceptions.RequestException as err:
                err_msg = "Failed to send data to the IPOP Visualizer" \
                    " webservice({0}). Exception: {1}" \
                    .format(self.vis_address, str(err))
                self.register_cbt("Logger", "LOG_ERROR", err_msg)
        """
        #else:
        #    warn_msg = "Don't have enough data to send. Not forwarding" \
        #            " anything to the collector service. Data:" \
        #            " {}".format(collector_msg)
        #    self.register_cbt("Logger", "LOG_WARNING", warn_msg)

        # Now that all the accumulated data has been dealth with, we request
        # more data
        self._vis_req_publisher.post_update(None)
        """

    def terminate(self):
        pass
