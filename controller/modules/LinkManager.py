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

import time
import threading
import uuid
import copy
try:
    import simplejson as json
except ImportError:
    import json
from controller.framework.ControllerModule import ControllerModule


class LinkManager(ControllerModule):

    def __init__(self, cfx_handle, module_config, module_name):
        super(LinkManager, self).__init__(cfx_handle, module_config, module_name)
        self.maxretries = self._cm_config["MaxConnRetry"]
        self._peers = {}
        self._overlays = {}  # indexed by overlay id
        self._links = {}     # indexed by link id
        self._lock = threading.Lock() # serializes access to _overlays, _links

    def initialize(self):
        self._link_updates_publisher = \
                self._cfx_handle.publish_subscription("LNK_DATA_UPDATES")
        try:
            # Subscribe for data request notifications from OverlayVisualizer
            self._cfx_handle.start_subscription("OverlayVisualizer",
                                                "VIS_DATA_REQ")
        except NameError as e:
            if "OverlayVisualizer" in str(e):
                self.register_cbt("Logger", "LOG_WARNING",
                                  "OverlayVisualizer module not loaded."
                                  " Visualization data will not be sent.")
        overlay_ids = self._cfx_handle.query_param("Overlays")
        for olid in overlay_ids:
            self._overlays[olid] = dict(Peers=dict())

        self.register_cbt("Logger", "LOG_INFO", "Module Loaded")

    def req_handler_remove_link(self, cbt):
        olid = cbt.request.params["OverlayId"]
        lid = cbt.request.params["LinkId"]
        self.create_linked_cbt(cbt)
        rl_cbt = self.create_cbt(self._module_name, "TincanInterface",
                                 "TCI_REMOVE_LINK", {"OverlayId": olid, "LinkId": lid})
        self.submit_cbt(rl_cbt)
        # TODO: send courtesy terminate link ICC, later.

    #def query_link_descriptor(self, cbt):
    #    pass

    def req_link_descriptors_update(self):
        params = []
        self._lock.acquire()
        for olid in self._overlays:
            params.append(olid)
        self._lock.release()
        self.register_cbt("TincanInterface", "TCI_QUERY_LINK_STATS", params)

    def req_handler_query_visualizer_data(self, cbt):
        vis_data = dict(LinkManager=dict())
        self._lock.acquire()
        for olid in self._overlays:
            for peerid in self._overlays[olid]["Peers"]:
                lnkid = self._overlays[olid]["Peers"][peerid]
                stats = self._links[lnkid]["Stats"]
                vis_data["LinkManager"][olid] = {lnkid: dict(LinkId=lnkid, PeerId=peerid, Stats=stats)}
        self._lock.release()
        cbt.set_response(data=vis_data, status=True)
        self.complete_cbt(cbt)

    def handle_link_descriptors_update(self, cbt):
        if (not cbt.response.status):
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
        else:
            data = json.loads(cbt.response.data)
            self._lock.acquire()
            for olid in cbt.request.params:
                for lnkid in data[olid]:
                    self._links[lnkid] = dict(Stats=data[olid][lnkid])
            self._lock.release()
        self.free_cbt(cbt)

    def resp_handler_remove_link(self, cbt):
        if (not cbt.response.status):
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
        else:
            parent_cbt = self.get_parent_cbt(cbt)
            olid = cbt.request.params["OverlayId"]
            lnkid = cbt.request.params["LinkId"]
            self.free_cbt(cbt)
            self._lock.acquire()
            # TODO - Remove entry from _overlays
            del(self._links[lnkid])
            self._lock.release()
            parent_cbt.set_response(data="successful", status=True)
            self.complete_cbt(parent_cbt)

            # TODO: Need to remove link from self._links
            param = {
                "UpdateType": "REMOVED", "OverlayId": olid,
                "LinkId": lnkid
                }
            self._link_updates_publisher.post_update(param)

    def req_handler_query_links(self, cbt):
        olid = cbt.request.params["OverlayId"]
        peerid = cbt.request.params["LinkId"]
        self._lock.acquire()
        lnkid = self._overlays[olid]["Peers"][peerid]
        cbt.set_response(self._overlays[lnkid]["Stats"], status=True)
        self._lock.release()
        self.complete_cbt(cbt)

    def req_handler_create_link(self, cbt):
        """
        The caller provides the overlay id which contains the link and the peer id
        which the link connects. The link id is generated here and returned to the
        caller. This is done only after the local enpoint is created, but can
        occur before the link is ready. The link status can be queried to determine
        when it is writeable.
        We request creatation of the remote endpoint first to avoid cleaning up a
        local endpoint if the peer denies our request. The link id is communicated
        in the request and will be the same at both nodes.
        """
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 1 Node A")
        olid = cbt.request.params["OverlayId"]
        peerid = cbt.request.params["PeerId"]
        self._lock.acquire()
        if peerid in self._overlays[olid]["Peers"]:
            # Link already exists, TM should clean up first
            self._lock.release()
            cbt.set_response("A link already exist or is being created for "
                             "overlay id: {0} peer id: {1}"
                             .format(olid,peerid), False)
            self.complete_cbt(cbt)
            return
        else:
            lnkid = uuid.uuid4().hex
            # index for quick peer->link lookup
            self._overlays[olid]["Peers"][peerid] = lnkid
            self._links[lnkid] = dict(Stats=dict())
            self._lock.release()

        msg = {
            "OverlayId": olid,
            "LinkId": lnkid,
            "EncryptionEnabled": cbt.request.params["EncryptionEnabled"],
            "NodeData": cbt.request.params["NodeData"],
            "TTL": time.time() + self._cm_config["InitialLinkTTL"]
        }
        # Send the message via SIG server to peer node
        remote_act = dict(OverlayId=olid,
                          RecipientId=peerid,
                          RecipientCM="LinkManager",
                          Action="LNK_REQ_LINK_ENDPT",
                          Params=json.dumps(msg))

        lcbt = self.create_linked_cbt(cbt)
        lcbt.set_request(self._module_name, "Signal", "SIG_REMOTE_ACTION", remote_act)
        self.submit_cbt(lcbt)
        return  # not returning linkid here, seems not required.
    
    def req_handler_add_peer_cas(self, cbt):
        # Create Link: Phase 7 Node B
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 7 Node B")
        params = json.loads(cbt.request.params)
        lcbt = self.create_linked_cbt(cbt)
        lcbt.set_request(self._module_name, "TincanInterface", "TCI_CREATE_LINK", params)
        self.submit_cbt(lcbt)

    def req_handler_req_link_endpt(self, cbt):
        # Create Link: Phase 2 Node B
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 2 Node B")
        params = json.loads(cbt.request.params)
        olid = params["OverlayId"]
        lnkid = params["LinkId"]
        peerid = params["NodeData"]["UID"]
        self._lock.acquire()
        self._overlays[olid]["Peers"][peerid] = lnkid  # add to index for quick peer->link lookup
        self._links[lnkid] = dict(Stats=dict())
        self._lock.release()
        self.query_local_node_data(cbt)

    def query_local_node_data(self, cbt):
        lcbt = self.create_linked_cbt(cbt)
        cbt_params = json.loads(cbt.request.params)
        params = {"OverlayId": cbt_params["OverlayId"]}
        lcbt.set_request(self._module_name, "TincanInterface", "TCI_QUERY_OVERLAY_INFO", params)
        self.submit_cbt(lcbt)

    def create_local_endpt(self, parent_cbt):
        lcbt = self.create_linked_cbt(parent_cbt)
        params = json.loads(parent_cbt.request.params)
        lcbt.set_request(self._module_name, "TincanInterface", "TCI_CREATE_LINK", params)
        self.submit_cbt(lcbt)

    def resp_handler_query_node_data(self, cbt):
        # Create Link: Phase 3 Node B
        if (not cbt.response.status):
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
            # TODO: failure response
        elif cbt.request.action == "TCI_QUERY_OVERLAY_INFO":
            """
            We now have the node data, add it to parent cbt. The next step
            to create the link endpt and retrieve its cas.
            """
            resp_data = json.loads(cbt.response.data)
            node_data = {
                "VIP4": resp_data["VIP4"],
                "MAC": resp_data["MAC"],
                "FPR": resp_data["FPR"],
                "UID": self._cm_config["NodeId"]
            }
            parent_cbt = self.get_parent_cbt(cbt)
            # store the partial data in the parent but do not send as yet
            parent_cbt.set_response(node_data, False)
            self.free_cbt(cbt)
            self.create_local_endpt(parent_cbt)

    def complete_link_endpt_request(self, cbt):
        # Create Link: Phase 4 Node B
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 4 Node B")
        parent_cbt = self.get_parent_cbt(cbt)
        parent_cbt.response.data["CAS"] = cbt.response.data
        parent_cbt.response.data = json.dumps(parent_cbt.response.data)
        parent_cbt.response.status = True
        self.free_cbt(cbt)
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 4 Node B sending node data:{}".format(parent_cbt.response.data))
        self.complete_cbt(parent_cbt)

    def send_nodea_cas_to_peer(self, cbt):
        # Create Link: Phase 6 Node A
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 6 Node A")
        local_cas = cbt.response.data
        parent_cbt = self.get_parent_cbt(cbt)
        olid = parent_cbt.request.params["OverlayId"]
        peerid = parent_cbt.request.params["PeerId"]
        params = {
            "LinkId": cbt.request.params["LinkId"],
            "OverlayId": olid,
            "EncryptionEnabled": cbt.request.params["EncryptionEnabled"],
            "NodeData": {
                "VIP4": "", "UID": "", "MAC": "", "CAS": local_cas, "FPR": ""}
            }
        remote_act = dict(OverlayId=olid, RecipientId=peerid,
                    RecipientCM="LinkManager", Action="LNK_ADD_PEER_CAS",
                    Params=json.dumps(params))
        lcbt = self.create_linked_cbt(parent_cbt)
        lcbt.set_request(self._module_name, "Signal", "SIG_REMOTE_ACTION", remote_act)
        self.submit_cbt(lcbt)
        self.free_cbt(cbt)

    def resp_handler_create_link_endpt(self, cbt):
        if (not cbt.response.status):
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
            # TODO: failure response

        parent_cbt = self.get_parent_cbt(cbt)
        if parent_cbt.request.action == "LNK_REQ_LINK_ENDPT":
            """
            To complete this request the responding node has to supply its own
            NodeData and CAS. The NodeData was previously queried and is stored
            on the parent cbt. Add the cas and send to peer.
            """
            self.complete_link_endpt_request(cbt)

        elif parent_cbt.request.action == "LNK_CREATE_LINK":
            """
            Both endpoints are created now but the peer doesn't have our cas.
            It already has the node data so no need to send that again.
            """
            self.send_nodea_cas_to_peer(cbt)

        elif parent_cbt.request.action == "LNK_ADD_PEER_CAS":
            # Create Link: Phase 8 Node B
            self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 8 Node B")
            parent_cbt.set_response(data="LNK_ADD_PEER_CAS successful", status=True)
            self.free_cbt(cbt)
            self.complete_cbt(parent_cbt)

    def create_2nd_link_endpt(self, rem_act, parent_cbt):
        # Create Link: Phase 5 Node A
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 5 Node A")
        params = json.loads(rem_act["Params"])
        node_data = json.loads(rem_act["Data"])
        olid = rem_act["OverlayId"]
        cbt_params = {"OverlayId": olid, "LinkId": params["LinkId"],
                "EncryptionEnabled": params["EncryptionEnabled"],
                "NodeData": {
                    "VIP4": node_data["VIP4"],
                    "UID": node_data["UID"],
                    "MAC": node_data["MAC"],
                    "CAS": node_data["CAS"],
                    "FPR": node_data["FPR"]}}
        lcbt = self.create_linked_cbt(parent_cbt)
        lcbt.set_request(self._module_name, "TincanInterface", "TCI_CREATE_LINK", cbt_params)
        self.submit_cbt(lcbt)

    def complete_create_link_request(self, parent_cbt):
        # Create Link: Phase 9 Node A
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: Phase 9 Node A")
        # Complete the cbt that started this all
        olid = parent_cbt.request.params["OverlayId"]
        peerid = parent_cbt.request.params["PeerId"]
        self._lock.acquire()
        lnkid = self._overlays[olid]["Peers"][peerid]
        self._lock.release()
        parent_cbt.set_response(data={"LinkId": lnkid}, status=True)
        self.complete_cbt(parent_cbt)
        # publish notification of link creation
        param = {
            "UpdateType": "ADDED", "OverlayId": olid,
            "PeerId": peerid, "LinkId": lnkid
            }
        self._link_updates_publisher.post_update(param)

    def resp_handler_remote_action(self, cbt):
        if (not cbt.response.status):
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
            self.free_cbt(cbt)
        else:
            parent_cbt = cbt.parent
            rem_act = cbt.response.data
            self.free_cbt(cbt)
            if rem_act["Action"] == "LNK_REQ_LINK_ENDPT":
                self.create_2nd_link_endpt(rem_act, parent_cbt)
            elif rem_act["Action"] == "LNK_ADD_PEER_CAS":
                self.complete_create_link_request(parent_cbt)

    def process_cbt(self, cbt):
        #if cbt.request.recipient != "Logger":
        #    self.register_cbt("Logger", "LOG_DEBUG", "Process CBT:\n{0}".format(cbt))
        if cbt.op_type == "Request":
            if cbt.request.action == "LNK_CREATE_LINK":
                # Create Link: Phase 1 Node A
                # TOP wants a new link, first SIGnal peer to create endpt
                self.req_handler_create_link(cbt)

            elif cbt.request.action == "LNK_REQ_LINK_ENDPT":
                # Create Link: Phase 2 Node B
                # Rcvd peer req to create endpt, send to TCI
                self.req_handler_req_link_endpt(cbt)

            elif cbt.request.action == "LNK_ADD_PEER_CAS":
                # Create Link: Phase 7 Node B
                # CAS rcvd from peer, sends to TCI to update link's peer CAS info
                self.req_handler_add_peer_cas(cbt)

            elif cbt.request.action == "LNK_REMOVE_LINK":
                self.req_handler_remove_link(cbt)

            elif cbt.request.action == "LNK_QUERY_LINKS":
                self.req_handler_query_links(cbt)

            elif cbt.request.action == "VIS_DATA_REQ":
                self.req_handler_query_visualizer_data(cbt)

            else:
                log = "Unsupported CBT action {0}".format(cbt)
                self.register_cbt("Logger", "LOG_WARNING", log)
        elif cbt.op_type == "Response":
            if cbt.request.action == "SIG_REMOTE_ACTION":
                # Create Link: Phase 5 Node A
                # Attempt to create our end of link
                # Create Link: Phase 9 Node A
                # Link created, notify others
                self.resp_handler_remote_action(cbt)

            elif cbt.request.action == "TCI_CREATE_LINK":
                # Create Link: Phase 4 Node B
                # Create Link: Phase 6 Node A
                # SIGnal to peer to update CAS
                # Create Link: Phase 8 Node B
                # Complete setup
                self.resp_handler_create_link_endpt(cbt)

            elif cbt.request.action == "TCI_QUERY_OVERLAY_INFO":
                # Create Link: Phase 3 Node B
                # Retrieved our node data for response
                self.resp_handler_query_node_data(cbt)

            elif cbt.request.action == "TCI_REMOVE_LINK":
                self.resp_handler_remove_link(cbt)

            elif cbt.request.action == "TCI_QUERY_LINK_STATS":
                self.handle_link_descriptors_update(cbt)
            else:
                self.free_cbt(cbt)


    def timer_method(self):
        pass #self.req_link_descriptors_update()

    def terminate(self):
        pass
