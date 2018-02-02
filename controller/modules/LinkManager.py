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

    def req_link_endpt_from_peer(self, cbt):
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

    def create_link_local_endpt(self, cbt):
        # Add to local DS (at recipient) for bookkeeping
        if cbt.request.action == "LNK_REQ_LINK_ENDPT":
            params = json.loads(cbt.request.params)
            olid = params["OverlayId"]
            lnkid = params["LinkId"]
            peerid = params["NodeData"]["UID"]
            self._lock.acquire()
            self._overlays[olid]["Peers"][peerid] = lnkid  # index for quick peer->link lookup
            self._links[lnkid] = dict(Stats=dict())
            self._lock.release()
        elif cbt.request.action == "LNK_ADD_PEER_CAS":
            params = json.loads(cbt.request.params)
        lcbt = self.create_linked_cbt(cbt)
        lcbt.set_request(self._module_name, "TincanInterface", "TCI_CREATE_LINK", params)
        self.submit_cbt(lcbt)

    def send_local_link_endpt_to_peer(self, cbt):
        if (not cbt.response.status):
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))

        local_cas = cbt.response.data
        parent_cbt = self.get_parent_cbt(cbt)
        if parent_cbt.request.action == "LNK_REQ_LINK_ENDPT":
            self.free_cbt(cbt)
            parent_cbt.set_response(local_cas, True)
            self.complete_cbt(parent_cbt)  # goes back to signal module implicitly, handled by signal.
        elif parent_cbt.request.action == "LNK_CREATE_LINK":
            # handling response after sending TCI_CREATE_LINK with peer_cas
            # create a wrapper method to frame SRA with A = "LNK_ADD_PEER_CAS", dont forget to link with LCL
            msg = copy.deepcopy(cbt.request.params)
            olid = parent_cbt.request.params["OverlayId"]
            peerid = parent_cbt.request.params["PeerId"]
            msg["NodeData"] = {"IP4": "", "UID": "", "MAC": "", "CAS": local_cas, "FPR": ""}
            remote_act = dict(OverlayId=olid,
                              RecipientId=peerid,
                              RecipientCM="LinkManager",
                              Action="LNK_ADD_PEER_CAS",
                              Params=json.dumps(msg))
            lcbt = self.create_linked_cbt(parent_cbt)
            lcbt.set_request(self._module_name, "Signal", "SIG_REMOTE_ACTION", remote_act)
            self.submit_cbt(lcbt)
        elif parent_cbt.request.action == "LNK_ADD_PEER_CAS":
            parent_cbt.set_response(data="successful", status=True)
            self.complete(parent_cbt)

    def remove_link(self, cbt):
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

    def update_visualizer_data(self, cbt):
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

    def remote_action_handler(self, cbt):
        if (not cbt.response.status):
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
        else:
            # look inside cbt for inner
            cbt_parent = cbt.parent
            rem_act = cbt.response.data
            self.free_cbt(cbt)
            if rem_act["Action"] == "LNK_REQ_LINK_ENDPT":
                peer_cas = rem_act["Data"]
                olid = rem_act["OverlayId"]
                params = json.loads(rem_act["Params"])
                link_id = params["LinkId"]
                cbt_params = {"OverlayId": olid, "LinkId": params["LinkId"],
                        "EncryptionEnabled": params["EncryptionEnabled"],
                        "NodeData": {
                            "IP4": params["NodeData"]["VIP4"],
                            "UID": params["NodeData"]["UID"],
                            "MAC": params["NodeData"]["MAC"],
                            "CAS": peer_cas,
                            "FPR": params["NodeData"]["FPR"]}}
                lcbt = self.create_linked_cbt(cbt_parent)
                lcbt.set_request(self._module_name, "TincanInterface", "TCI_CREATE_LINK", cbt_params)
                self.submit_cbt(lcbt)
            elif cbt_data["Action"] == "LNK_ADD_PEER_CAS":
                olid = cbt_parent.request.params["OverlayId"]
                peerid = cbt_parent.request.params["RecipientId"]
                self._lock.acquire()
                lnkid = self._overlays[olid]["Peers"][peerid]
                self._lock.release()
                cbt_parent.set_response(data={"LinkId": lnkid}, status=True)
                self.complete(cbt_parent)

                param = {
                   "UpdateType": "ADDED",
                   "OverlayId": olid,
                   "PeerId": peerid,
                   "LinkId": lnkid
                    }
                self._link_updates_publisher.post_update(param)

    def remove_link_handler(self, cbt):
        if (not cbt.response.status):
            self.register_cbt("Logger", "LOG_WARNING", "CBT failed {0}".format(cbt.response.data))
        else:
            # get parent, complete
            parent_cbt = self.get_parent_cbt(cbt)
            # is there a need to set up a response in parent cbt, when do we need to set up a response and when not?
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
                "UpdateType": "REMOVED",
                "OverlayId": olid,
                "LinkId": lnkid
                }
            self._link_updates_publisher.post_update(param)


    def query_links(self, cbt):
        # categorized by overlay ID's .
        olid = cbt.request.params["OverlayId"]
        peerid = cbt.request.params["LinkId"]
        self._lock.acquire()
        lnkid = self._overlays[olid]["Peers"][peerid]
        cbt.set_response(self._overlays[lnkid]["Stats"], status=True)
        self._lock.release()
        self.complete_cbt(cbt)


    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "LNK_CREATE_LINK":
                # Phase 1 create link
                # TOP wants a new link, first SIGnal peer to create endpt
                self.req_link_endpt_from_peer(cbt)

            elif cbt.request.action == "LNK_REQ_LINK_ENDPT":
                # Phase 2 create link
                # Rcvd peer req to create endpt, send via TCI
                self.create_link_local_endpt(cbt)

            elif cbt.request.action == "LNK_ADD_PEER_CAS":
                # Phase 4 create link
                # CAS rcvd from peer, sends to TCI to update link's peer CAS info
                self.create_link_local_endpt(cbt)

            elif cbt.request.action == "LNK_REMOVE_LINK":
                self.remove_link(cbt)

            elif cbt.request.action == "LNK_QUERY_LINKS":
                self.query_links(cbt)

            elif cbt.request.action == "SIG_PEER_PRESENCE_NOTIFY":  # probably not going to be used
                pass

            elif cbt.request.action == "VIS_DATA_REQ":
                self.update_visualizer_data(cbt)

            else:
                log = "Unsupported CBT action {0}".format(cbt)
                self.register_cbt("Logger", "LOG_WARNING", log)
        elif cbt.op_type == "Response":
            if cbt.request.action == "SIG_REMOTE_ACTION":
                self.remote_action_handler(cbt)

            elif cbt.request.action == "TCI_CREATE_LINK":
                # Phase 3 & 5 create link
                # SIGnal to peer to update CAS
                self.send_local_link_endpt_to_peer(cbt)

            elif cbt.request.action == "TCI_REMOVE_LINK":
                self.remove_link_handler(cbt)

            elif cbt.request.action == "TCI_QUERY_LINK_STATS":
                self.handle_link_descriptors_update(cbt)
            else:
                self.free_cbt(cbt)


    def timer_method(self):
        self.req_link_descriptors_update()

    def terminate(self):
        pass
