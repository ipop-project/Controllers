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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
from controller.framework.ControllerModule import ControllerModule
import json

class Icc(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Icc, self).__init__(cfx_handle, module_config, module_name)
        # Dictionary to hold data about overlayID->peerID->linkID mappings
        self._links = {}
        # Dictionary to hold CBTs created by Icc for remote action requests
        self._remote_acts = {}

    def initialize(self):
        self.register_cbt("Logger", "LOG_INFO", 
                            "Module loaded")
        
       # Subscribe for link updates from LinkManager
        self._cfx_handle.start_subscription("LinkManager",
                    "LNK_DATA_UPDATES")

        # Subscribe for messages from TincanInterface
        self._cfx_handle.start_subscription("TincanInterface",
                    "TCI_TINCAN_MSG_NOTIFY")

    def update_links(self,cbt):
        if cbt.request.params["UpdateType"] == "ADDED":
            overlayid = cbt.request.params["OverlayId"]
            peerid = cbt.request.params["PeerId"]
            linkid = cbt.request.params["LinkId"]
            if overlayid in self._links:
                self._links[overlayid]["Peers"][peerid] = linkid
            else:
                self._links[overlayid] = {}
                self._links[overlayid]["Peers"] = {}
                self._links[overlayid]["Peers"][peerid] = linkid

        elif cbt.request.params["UpdateType"] == "REMOVED":
            overlayid = cbt.request.params["OverlayId"]
            linkid = cbt.request.params["LinkId"]
            for peerid in self._links[overlayid]["Peers"]:
                if self._links[overlayid]["Peers"][peerid] == linkid:
                    del self._links[overlayid]["Peers"][peerid]
                    if len(self._links[overlayid]["Peers"]) == 0:
                        del self._links[overlayid]
                    break
        self.register_cbt("Logger","LOG_INFO","Received Link Updates")
        cbt.set_response(None, True)
        self.complete_cbt(cbt)

    def send_icc_data(self,cbt):
        """
            rem_data  = dict(OverlayId = "",
                             RecipientId = "",
                             RecipientCM = "",
                             Params = opaque_msg,
                             # added by sending Icc
                             InitiatorId="",
                             InitiatorCM="",
                             ActionTag="") 
        """
        self.register_cbt("Logger", "LOG_DEBUG", "Send data request {0} from {1}"
                          .format(cbt.tag, cbt.request.initiator))
        rem_data = cbt.request.params
        peerid = rem_data["RecipientId"]
        overlayid = rem_data["OverlayId"]

        linkid = self._links[overlayid]["Peers"][peerid]
        rem_data["InitiatorId"] = self._cm_config["NodeId"]
        rem_data["InitiatorCM"] = cbt.request.initiator
        rem_data["ActionTag"] = cbt.tag
        rem_data = json.dumps(rem_act)
        icc_msg = {
            "OverlayId": overlayid,
            "LinkId": linkid,
            "Data": rem_act
            }

        self.register_cbt("TincanInterface", "TCI_ICC", icc_msg)

    def broadcast_icc_data(self,cbt):
        peer_list = cbt.request.params["RecipientId"]
        overlayid = cbt.request.params["OverlayId"]
        for peerid in peer_list:
            param = {}
            param["OverlayId"] = overlayid
            param["LinkId"] = self._links[overlayid]["Peers"][peerid]
                
            lcbt = self.create_linked_cbt(cbt)
            lcbt.set_request("TincanInterface", "TCI_ICC", param)
            self.submit_cbt(lcbt)

    def send_icc_remote_action(self,cbt):
        """
        rem_act = dict(OverlayId="",
                          RecipientId="",
                          RecipientCM="",
                          Action="",
                          Params=json.dumps(opaque_msg),
                          # added by sending Icc
                          InitiatorId="",
                          InitiatorCM="",
                          ActionTag="",
                          # added by responding Icc
                          Data="",
                          Status="")
        """
        self.register_cbt("Logger", "LOG_DEBUG", "Send remote action {0} from {1}"
                          .format(cbt.tag, cbt.request.initiator))
        rem_act = cbt.request.params
        peerid = rem_act["RecipientId"]
        overlayid = rem_act["OverlayId"]
        if self._links.get(overlayid) is None:
            cbt.set_response("Invalid overlay id for send remote action", False)
            self.complete_cbt(cbt)
            return
        if self._links[overlayid]["Peers"].get(peerid) is None:
            cbt.set_response("Invalid peer id for send remote action", False)
            self.complete_cbt(cbt)
            return
        linkid = self._links[overlayid]["Peers"][peerid]
        rem_act["InitiatorId"] = self._cm_config["NodeId"]
        rem_act["InitiatorCM"] = cbt.request.initiator
        rem_act["ActionTag"] = cbt.tag
        rem_act = json.dumps(rem_act)
        icc_msg = {
            "OverlayId": overlayid,
            "LinkId": linkid,
            "Data": rem_act
            }
        self.register_cbt("TincanInterface", "TCI_ICC", icc_msg)

    def recieve_icc(self,cbt):
        if (cbt.request.params["Command"] != "ICC"):
            cbt.set_response(None, False)
            self.complete_cbt(cbt)
            return

        rem_act = json.loads(cbt.request.params["Data"])
        # Handling incoming Data Delivery requests
        # The field "Action" will not be present in rem_act
        # to differentiate Data Delivery & Remote action requests
        if "Action" not in rem_act:
            self.register_cbt("Logger", "LOG_DEBUG", "Incoming remote data {0}"
                                            .format(rem_act["ActionTag"]))
            target_module_name = rem_act["RecipientCM"]
            opaque_msg = rem_act["Params"]
            self.register_cbt(target_module_name, "ICC_DELIVER_DATA", opaque_msg)

        # New incoming Remote action requests received via Tincan
        elif rem_act["ActionTag"] not in self._cfx_handle._pending_cbts:
            self.register_cbt("Logger", "LOG_DEBUG", "Incoming remote action {0}"
                                        .format(rem_act["ActionTag"]))
            target_module_name = rem_act["RecipientCM"]
            remote_action_code = rem_act["Action"]
            opaque_msg = rem_act["Params"]
            rcbt = self.create_cbt(self._module_name, 
                                    target_module_name, 
                                    remote_action_code, 
                                    opaque_msg)
            self._remote_acts[rcbt.tag] = rem_act
            self.submit_cbt(rcbt)

        # Handle response to the remote action
        else:
            self.register_cbt("Logger", "LOG_DEBUG", "Remote action response {0}"
                                    .format(rem_act["ActionTag"]))
            rcbt = self._cfx_handle._pending_cbts[rem_act["ActionTag"]]
            rem_act = json.loads(cbt.request.params["Data"])
            resp_data = rem_act["Data"]
            status = rem_act["Status"]
            rcbt.set_response(resp_data, status)
            self.complete_cbt(rcbt)
        # Complete notification
        cbt.set_response(None, True)
        self.complete_cbt(cbt)

    # Complete the remote action by sending the response from the modules
    def complete_remote_action(self,cbt):
        if cbt.tag in self._remote_acts:
            rem_act = self._remote_acts[cbt.tag]
            self.register_cbt("Logger", "LOG_DEBUG", "Remote action complete"
                                " {0}".format(rem_act["ActionTag"]))
            overlayid = rem_act["OverlayId"]
            peerid = rem_act["InitiatorId"]
            if peerid in self._links[overlayid]["Peers"]:
                linkid = self._links[overlayid]["Peers"][peerid]
                rem_act["Data"] = cbt.response.data
                rem_act["Status"] = cbt.response.status
                rem_act = json.dumps(rem_act)
                icc_msg = {
                    "OverlayId": overlayid,
                    "LinkId": linkid,
                    "Data": rem_act
                    }
                self.register_cbt("TincanInterface", "TCI_ICC", icc_msg)
        self.free_cbt(cbt)

    # Handling responses for CBTs sent to TCI
    def resp_handler_tc_icc(self, cbt):
        cbt_data = json.loads(cbt.request.params["Data"])
        # Failure responses from TincanInterface
        # Common for both Data delivery & Remote Action requests
        if not cbt.response.status:
            pcbt = self._cfx_handle._pending_cbts[cbt_data["ActionTag"]]
            pcbt.set_response("Failed to send ICC", False)
            self.complete_cbt(pcbt)
        
        # Successful responses from TincanInterface
        # for Data delivery Requests
        if "Action" not in cbt_data:
            rcbt = self._cfx_handle._pending_cbts[cbt_data["ActionTag"]]
            rcbt.set_response("Icc Send Data Successful", True)
            self.complete_cbt(rcbt)

        self.free_cbt(cbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "LNK_DATA_UPDATES":
                self.update_links(cbt)
                
            elif cbt.request.action == "ICC_SEND_DATA":
                self.send_icc_data(cbt)

            elif cbt.request.action == "ICC_BROADCAST_DATA":
                self.broadcast_icc_data(cbt)

            elif cbt.request.action == "ICC_REMOTE_ACTION":
                self.send_icc_remote_action(cbt)

            elif cbt.request.action == "TCI_TINCAN_MSG_NOTIFY":
                self.recieve_icc(cbt)
            else:
                self.req_handler_default(cbt)
        elif cbt.op_type == "Response":
            if cbt.request.action == "TCI_ICC":
                self.resp_handler_tc_icc(cbt)

            elif cbt.request.action == "ICC_DELIVER_DATA":
                self.free_cbt(cbt)

            else:
                self.complete_remote_action(cbt)

    def terminate(self):
        pass

    def timer_method(self):
       pass
