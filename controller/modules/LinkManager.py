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

import threading
import uuid
from collections import defaultdict
from controller.framework.ControllerModule import ControllerModule


class LinkManager(ControllerModule):

    def __init__(self, cfx_handle, module_config, module_name):
        super(LinkManager, self).__init__(cfx_handle, module_config, module_name)
        self._tunnels = {}   # maps tunnel(link) id to its descriptor
        self._peers = {}     # maps overlay id to peers map, which maps peer id to link id
        self._links = {}     # maps link id to stats, overlay id and peer id
        self._lock = threading.Lock() # serializes access to _overlays, _links
        self._link_updates_publisher = None
        self._ignored_net_interfaces = defaultdict(set)

    def initialize(self):
        self._link_updates_publisher = \
                self._cfx_handle.publish_subscription("LNK_DATA_UPDATES")
        self._cfx_handle.start_subscription("TincanInterface",
                                            "TCI_TINCAN_MSG_NOTIFY")
        try:
            # Subscribe for data request notifications from OverlayVisualizer
            self._cfx_handle.start_subscription("OverlayVisualizer",
                                                "VIS_DATA_REQ")
        except NameError as err:
            if "OverlayVisualizer" in str(err):
                self.register_cbt("Logger", "LOG_WARNING",
                                  "OverlayVisualizer module not loaded."
                                  " Visualization data will not be sent.")
        overlay_ids = self._cfx_handle.query_param("Overlays")
        for olid in overlay_ids:
            self._peers[olid] = dict()

        for overlay_id in self._cm_config["Overlays"]:
            ol_cfg = self._cm_config["Overlays"][overlay_id]
            if "IgnoredNetInterfaces" in ol_cfg:
                for ign_inf in ol_cfg["IgnoredNetInterfaces"]:
                    self._ignored_net_interfaces[overlay_id].add(ign_inf)

        self.register_cbt("Logger", "LOG_INFO", "Module Loaded")

    def _get_ignored_tap_names(self, overlay_id, new_inf_name=None):
        ign_tap_names = set()
        if new_inf_name:
            ign_tap_names.add(new_inf_name)

        # We need to ignore ALL the ipop tap devices (regardless
        # of their overlay id/link id)
        for olid in self._tunnels:
            ign_tap_names.add(
                self._tunnels[olid]["Descriptor"]["TapName"])

        # Please note that overlay_id is only used to selectively
        # ignore physical interfaces and bridges
        ign_tap_names \
            |= self._ignored_net_interfaces[overlay_id]
        return ign_tap_names

    def req_handler_add_ign_inf(self, cbt):
        ign_inf_details = cbt.request.params

        for olid in ign_inf_details:
            self._ignored_net_interfaces[olid].add(ign_inf_details[olid])

    def req_handler_remove_link(self, cbt):
        olid = cbt.request.params.get("OverlayId", None)
        lnkid = cbt.request.params.get("LinkId", None)
        peer_id = cbt.request.params.get("PeerId", None)
        if olid is not None and peer_id is not None:
            lnkid = self._peers[olid][peer_id]
            olid = lnkid
        elif lnkid is not None:
            oid = self._links[lnkid]["OverlayId"]
            olid = lnkid
        else:
            cbt.set_response("Insufficient parameters", False)
            self.complete_cbt(cbt)
            return

        self.create_linked_cbt(cbt)
        params = {"OID": oid, "OverlayId": olid, "LinkId": lnkid}
        rl_cbt = self.create_cbt(self._module_name, "TincanInterface",
                                 "TCI_REMOVE_LINK", params)
        self.submit_cbt(rl_cbt)

# TODO Check locks on self._tunnels
    def _update_tunnel_descriptor(self, tnl_desc, link_id):
        if not link_id in self._tunnels:
            self._tunnels[link_id] = dict(Descriptor=dict())
        if not "Descriptor" in self._tunnels[link_id]:
            self._tunnels[link_id]["Descriptor"] = dict()
        self._tunnels[link_id]["Descriptor"]["MAC"] = tnl_desc["MAC"]
        self._tunnels[link_id]["Descriptor"]["VIP4"] = tnl_desc.get("VIP4")
        self._tunnels[link_id]["Descriptor"]["TapName"] = tnl_desc["TapName"]
        self._tunnels[link_id]["Descriptor"]["FPR"] = tnl_desc["FPR"]
        self._tunnels[link_id]["Descriptor"]["IP4PrefixLen"] = tnl_desc["IP4PrefixLen"]
        self.register_cbt("Logger", "LOG_DEBUG", "_tunnels:{}".format(self._tunnels))

    def _query_link_stats(self):
        params = []
        with self._lock:
            for olid in self._peers:
                for peer_id in self._peers[olid]:
                    link_id = self._peers[olid][peer_id]
                    params.append(link_id)
        if params:
            self.register_cbt("TincanInterface", "TCI_QUERY_LINK_STATS", params)

    def resp_handler_query_link_stats(self, cbt):
        if not cbt.response.status:
            self.register_cbt("Logger", "LOG_WARNING", "Link stats update error: {0}"
                              .format(cbt.response.data))
            self.free_cbt(cbt)
            return
        if not cbt.response.data:
            self.free_cbt(cbt)
            return
        data = cbt.response.data
        with self._lock:
            for olid in data:
                for lnkid in data[olid]:
                   #! oid = self._links[lnkid]["OverlayId"]
                    if data[olid][lnkid]["Status"] == "UNKNOWN":
                        self._link_removed_cleanup(lnkid)
                    else:
                        self._links[lnkid]["Stats"] = data[olid][lnkid]["Stats"]
                        self._links[lnkid]["IceRole"] = data[olid][lnkid]["IceRole"]
                        self._links[lnkid]["Status"] = data[olid][lnkid]["Status"]
        self.free_cbt(cbt)

    def req_handler_query_viz_data(self, cbt):
        vis_data = dict(LinkManager=defaultdict(dict))
        #with self._lock:
        #    for olid in self._overlays:
        #        if "Descriptor" in self._overlays[olid]:
        #            descriptor = self._overlays[olid]["Descriptor"]
        #            node_data = {
        #                "TapName": descriptor["TapName"],
        #                "VIP4": descriptor.get("VIP4"),
        #                "IP4PrefixLen": descriptor["IP4PrefixLen"],
        #                "MAC": descriptor["MAC"]
        #            }
        #            node_id = str(self._cm_config["NodeId"])
        #            vis_data["LinkManager"][olid][node_id] = dict(NodeData=node_data,
        #                                                          Links=dict())
        #            # self._overlays[olid]["Descriptor"]["GeoIP"] # TODO: GeoIP
        #            peers = self._overlays[olid]["Peers"]
        #            for peerid in peers:
        #                lnkid = peers[peerid]
        #                stats = self._links[lnkid]["Stats"]

        #                link_data = {
        #                    "SrcNodeId": node_id,
        #                    "PeerId": peerid,
        #                    "Stats": stats
        #                }

        #                if "IceRole" in self._links[lnkid]:
        #                    link_data["IceRole"] = self._links[lnkid]["IceRole"]

        #                if "Type" in self._links[lnkid]:
        #                    link_data["Type"] = self._links[lnkid]["Type"]

        #                if "Status" in self._links[lnkid]:
        #                    link_data["Status"] = self._links[lnkid]["Status"]

        #                vis_data["LinkManager"][olid][node_id]["Links"][lnkid] = link_data

        cbt.set_response(vis_data, True if vis_data["LinkManager"] else False)
        self.complete_cbt(cbt)

    def _link_removed_cleanup(self, lnkid):
        lnk_entry = self._links.pop(lnkid, None)
        if lnk_entry:
            peerid = lnk_entry["PeerId"]
            olid = lnk_entry["OverlayId"]
            item = self._peers[olid].pop(peerid, None)
            # Notify subs of link removal
            param = {
                "UpdateType": "REMOVED", "OverlayId": olid, "LinkId": lnkid, "PeerId": peerid,
                "TapName": self._tunnels[lnkid]["Descriptor"]["TapName"]}
            self._link_updates_publisher.post_update(param)
            del lnk_entry
            del item

    def resp_handler_remove_link(self, cbt):
        parent_cbt = self.get_parent_cbt(cbt)
        oid = cbt.request.params["OID"]
        olid = cbt.request.params["OverlayId"]
        lnkid = cbt.request.params["LinkId"]
        with self._lock:
            self._link_removed_cleanup(lnkid)
        self.register_cbt("Logger", "LOG_DEBUG", "Link removed {}".format(lnkid))
        self.free_cbt(cbt)
        params = {"OID": oid, "OverlayId": olid}
        if parent_cbt is not None:
            rmv_ovl_cbt = self.create_linked_cbt(parent_cbt)
            rmv_ovl_cbt.set_request(self._module_name, "TincanInterface",
                                    "TCI_REMOVE_OVERLAY", params)
        else:
            rmv_ovl_cbt = self.create_cbt(self._module_name, "TincanInterface",
                                          "TCI_REMOVE_OVERLAY", params)
        self.submit_cbt(rmv_ovl_cbt)

    def resp_handler_remove_overlay(self, rmv_ovl_cbt):
        """
        Remove the overlay meta data. Even of the CBT fails it is safe to discard
        as this is because Tincan has to record of it.
        """
        parent_cbt = self.get_parent_cbt(rmv_ovl_cbt)
        olid = rmv_ovl_cbt.request.params["OverlayId"]
        ovl = self._tunnels.pop(olid, None)
        del ovl
        if parent_cbt is not None:
            parent_cbt.set_response("Overlay removed", True)
            self.complete_cbt(parent_cbt)
        self.register_cbt("Logger", "LOG_DEBUG", "Overlay removed {}".format(olid))

    def req_handler_query_link_info(self, cbt):
        olid = cbt.request.params["OverlayId"]
        peerid = cbt.request.params["LinkId"]
        self._lock.acquire()
        lnkid = self._peers[olid][peerid]
        cbt.set_response(self._links[lnkid]["Stats"], status=True)
        self._lock.release()
        self.complete_cbt(cbt)

    def _create_overlay(self, params, parent_cbt=None):
        overlay_id = params["OverlayId"]
        ol_type = self._cm_config["Overlays"][overlay_id]["Type"]
        lnkid = params["LinkId"]
        tap_name = self._cm_config["Overlays"][overlay_id]["TapName"][:8] + \
            str(lnkid[:7])
        olid = lnkid
        create_ovl_params = {
            "OID": overlay_id,
            "OverlayId": olid,
            "LinkId": lnkid,
            "StunServers": self._cm_config["Stun"],
            "Type": ol_type,
            "TapName": tap_name,
            "IP4": self._cm_config["Overlays"][overlay_id].get("IP4"),
            "MTU4": self._cm_config["Overlays"][overlay_id].get("MTU4"),
            "IP4PrefixLen": self._cm_config["Overlays"][overlay_id].get("IP4PrefixLen"),
            "IgnoredNetInterfaces": list(
                self._get_ignored_tap_names(overlay_id, tap_name))
        }
        if self._cm_config.get("Turn"):
            create_ovl_params["TurnServers"] = self._cm_config["Turn"]

        if parent_cbt is not None:
            ovl_cbt = self.create_linked_cbt(parent_cbt)
            ovl_cbt.set_request(self._module_name, "TincanInterface",
                                "TCI_CREATE_OVERLAY", create_ovl_params)
        else:
            ovl_cbt = self.create_cbt(self._module_name, "TincanInterface",
                                      "TCI_CREATE_OVERLAY", create_ovl_params)
        self.submit_cbt(ovl_cbt)

    def _request_peer_endpoint(self, params, parent_cbt):
        overlay_id = params["OverlayId"]
        lnkid = params["LinkId"]
        ovl_data = self._tunnels[lnkid]["Descriptor"]
        endp_param = {
            "NodeData": {
                "FPR": ovl_data["FPR"],
                "MAC": ovl_data["MAC"],
                "UID": self._cm_config["NodeId"],
                "VIP4": ovl_data.get("VIP4")}}
        endp_param.update(params)
        remote_act = dict(OverlayId=overlay_id,
                          RecipientId=parent_cbt.request.params["PeerId"],
                          RecipientCM="LinkManager",
                          Action="LNK_REQ_LINK_ENDPT",
                          Params=endp_param)
        if parent_cbt is not None:
            endp_cbt = self.create_linked_cbt(parent_cbt)
            endp_cbt.set_request(self._module_name, "Signal",
                                 "SIG_REMOTE_ACTION", remote_act)
        else:
            endp_cbt = self.create_cbt(self._module_name, "Signal",
                                       "SIG_REMOTE_ACTION", remote_act)
        # Send the message via SIG server to peer
        self.submit_cbt(endp_cbt)

    def req_handler_create_link(self, cbt):
        """
        Handle the request for capability LNK_CREATE_LINK.
        The caller provides the overlay id and the peer id which the link
        connects. The link id is generated here but it is returned to the
        caller after the local endpoint creation is completed asynchronously.
        The link is not necessarily ready for read/write at this time. The link
        status can be queried to determine when it is writeable. We request
        creation of the remote endpoint first to avoid cleaning up a local
        endpoint if the peer denies our request. The link id is communicated
        in the request and will be the same at both nodes.
        """
         # Create Link: Phase 1 Node A
        lnkid = uuid.uuid4().hex
        overlay_id = cbt.request.params["OverlayId"]
        peerid = cbt.request.params["PeerId"]
        with self._lock:
            if peerid in self._peers[overlay_id]:
                # Link already exists, TM should clean up first
                cbt.set_response("A link already exist or is being created for "
                                 "overlay id: {0} peer id: {1}"
                                 .format(overlay_id, peerid), False)
                self.complete_cbt(cbt)
                return
            # index for quick peer->link lookup
            self._peers[overlay_id][peerid] = lnkid
            self._links[lnkid] = dict(Stats=dict(), OverlayId=overlay_id, PeerId=peerid)
        self.register_cbt("Logger", "LOG_INFO", "Create Link:{} Phase 1/5 Node A".format(lnkid[:7]))
        params = {"OverlayId": overlay_id, "LinkId": lnkid,
                  "Type": self._cm_config["Overlays"][overlay_id]["Type"]}
        self._create_overlay(params, parent_cbt=cbt)

    def resp_handler_create_overlay(self, cbt):
        # Create Link: Phase 2 Node A
        parent_cbt = self.get_parent_cbt(cbt)
        resp_data = cbt.response.data
        if not cbt.response.status:
            self.free_cbt(cbt)
            parent_cbt.set_response(resp_data, False)
            self.complete_cbt(parent_cbt)
            self.register_cbt("Logger", "LOG_DEBUG", "Create overlay failed:{}"
                              .format(parent_cbt.response.data))
            return
        # store the overlay data
        lnkid = cbt.request.params["LinkId"] # config overlay id
        overlay_id = cbt.request.params["OID"] # config overlay id
        self.register_cbt("Logger", "LOG_INFO", "Create Link:{} Phase 2/5 Node A".format(lnkid[:7]))
        with self._lock:
            self._update_tunnel_descriptor(resp_data, lnkid)
        # create and send remote action to request endpoint from peer
        params = {"OverlayId": overlay_id, "LinkId": lnkid}
        self._request_peer_endpoint(params, parent_cbt)
        self.free_cbt(cbt)

    def req_handler_req_link_endpt(self, cbt):
        """
        Handle the request for capability LNK_REQ_LINK_ENDPT.
        This request occurs on the remote node B. It determines if it can
        facilitate a link between itself and the requesting node A. It must
        first send a TCI_QUERY_OVERLAY_INFO to accomplish this.
        """
        # Create Link: Phase 3 Node B
        params = cbt.request.params
        overlay_id = params["OverlayId"]
        if overlay_id not in self._cm_config["Overlays"]:
            self.register_cbt("Logger", "LOG_WARNING", "The requested overlay not specified in "
                              "local config, it will not be created")
            cbt.set_response("Unknown overlay id specified in request", False)
            self.complete_cbt(cbt)
            return

        lnkid = params["LinkId"]
        node_data = params["NodeData"]
        peer_id = node_data["UID"]
        self.register_cbt("Logger", "LOG_INFO", "Create Link:{} Phase 1/4 Node B".format(lnkid[:7]))
        self._lock.acquire()
        """
        A request to create a local link endpt can be received after we have
        sent out a similar request to a peer. To handle this race we choose to
        service the request only if we have not yet started to create an endpt
        - self._peers[overlay_id][peer_id] is None, or our node id
        is less than the peer. In which case we rename the existing link id to
        the request.
        """
        # Node A, fails the request. Things then proceed as normal
        if (peer_id in self._peers[overlay_id]
                and peer_id < self._cm_config["NodeId"]):
            cbt.set_response("LNK_REQ_LINK_ENDPT denied", False)
            self.complete_cbt(cbt)
            self._lock.release()
            self.register_cbt("Logger", "LOG_INFO", "A duplicate create link "
                              "endpoint request was discarded {0}". format(cbt))
            return
        # On node B (the larger node id) switch the link id to use the one sent by node A
        elif peer_id in self._peers[overlay_id] and peer_id > self._cm_config["Peer_id"]:
            def_lnk_descr = dict(Stats=dict(), OverlayId=overlay_id, PeerId=peer_id)
            link_descr = self._links.pop(lnkid, def_lnk_descr)
            self._links[lnkid] = link_descr
            self._peers[overlay_id][peer_id] = lnkid
            self._lock.release()
            self.register_cbt("Logger", "LOG_INFO", "A duplicate create link "
                              "endpoint request was merged {0}". format(cbt))

        elif peer_id not in self._peers[overlay_id]:
            # add/replace to index for quick peer->link lookup
            self._peers[overlay_id][peer_id] = lnkid
            self._links[lnkid] = dict(Stats=dict(), OverlayId=overlay_id, PeerId=peer_id)
            self._lock.release()
        ol_type = self._cm_config["Overlays"][overlay_id]["Type"]
        tap_name = self._cm_config["Overlays"][overlay_id]["TapName"][:8] + \
            str(lnkid[:7])
        olid = lnkid
        create_link_params = {
            "OID": overlay_id,
            # overlay params
            "OverlayId": olid,
            "StunServers": self._cm_config["Stun"],
            "Type": ol_type,
            "TapName": tap_name,
            "IP4": self._cm_config["Overlays"][overlay_id].get("IP4"),
            "MTU4": self._cm_config["Overlays"][overlay_id].get("MTU4"),
            "IP4PrefixLen": self._cm_config["Overlays"][overlay_id].get("IP4PrefixLen"),
            "IgnoredNetInterfaces": list(
                self._get_ignored_tap_names(overlay_id, tap_name)),
            # link params
            "LinkId": lnkid,
            "NodeData": {
                "FPR": node_data["FPR"],
                "MAC": node_data["MAC"],
                "UID": node_data["UID"],
                "VIP4": node_data.get("VIP4")}}
        if self._cm_config.get("Turn"):
            create_link_params["TurnServers"] = self._cm_config["Turn"]
        lcbt = self.create_linked_cbt(cbt)
        lcbt.set_request(self._module_name, "TincanInterface",
                         "TCI_CREATE_LINK", create_link_params)
        self.submit_cbt(lcbt)

    def _complete_link_endpt_request(self, cbt):
        """
        """
        # Create Link: Phase 4 Node B
        parent_cbt = self.get_parent_cbt(cbt)
        resp_data = cbt.response.data
        if not cbt.response.status:
            self.free_cbt(cbt)
            parent_cbt.set_response(resp_data, False)
            if parent_cbt.child_count == 1:
                self.complete_cbt(parent_cbt)
            self.register_cbt("Logger", "LOG_INFO", "Create link endpoint failed :{}"
                              .format(cbt.response.data))
            return
        lnkid = cbt.request.params["LinkId"]
        self.register_cbt("Logger", "LOG_INFO", "Create Link:{} Phase 2/4 Node B".format(lnkid[:7]))
        # store the overlay data
        self._update_tunnel_descriptor(resp_data, lnkid)
        # respond with this nodes connection parameters
        node_data = {
            "VIP4": resp_data.get("VIP4"),
            "MAC": resp_data["MAC"],
            "FPR": resp_data["FPR"],
            "UID": self._cm_config["NodeId"],
            "CAS": resp_data["CAS"]
        }
        data = {
            "OverlayId": cbt.request.params["OID"],
            "LinkId": lnkid,
            "NodeData": node_data
        }
        self.free_cbt(cbt)
        parent_cbt.set_response(data, True)
        self.complete_cbt(parent_cbt)

    def _create_link_endpoint(self, rem_act, parent_cbt):
        # Create Link: Phase 5 Node A
        lnkid = rem_act["Data"]["LinkId"]
        self.register_cbt("Logger", "LOG_INFO", "Create Link:{} Phase 3/5 Node A".format(lnkid[:7]))
        node_data = rem_act["Data"]["NodeData"]
        oid = rem_act["OverlayId"]
        olid = lnkid
        cbt_params = {"OID": oid, "OverlayId": olid, "LinkId": lnkid, "Type": "TUNNEL",
                      "NodeData": {
                          "VIP4": node_data.get("VIP4"),
                          "UID": node_data["UID"],
                          "MAC": node_data["MAC"],
                          "CAS": node_data["CAS"],
                          "FPR": node_data["FPR"]}}
        lcbt = self.create_linked_cbt(parent_cbt)
        lcbt.set_request(self._module_name, "TincanInterface", "TCI_CREATE_LINK", cbt_params)
        self.submit_cbt(lcbt)

    def _send_local_cas_to_peer(self, cbt):
        # Create Link: Phase 6 Node A
        lnkid = cbt.request.params["LinkId"]
        self.register_cbt("Logger", "LOG_INFO", "Create Link:{} Phase 4/5 Node A".format(lnkid[:7]))
        local_cas = cbt.response.data["CAS"]
        parent_cbt = self.get_parent_cbt(cbt)
        oid = cbt.request.params["OID"]
        olid = cbt.request.params["OverlayId"]
        peerid = parent_cbt.request.params["PeerId"]
        params = {
            "OID": oid,
            "OverlayId": olid,
            "LinkId": lnkid,
            "NodeData": {
                "UID": self._cm_config["NodeId"], "MAC": "",
                "CAS": local_cas, "FPR": ""}}
        remote_act = dict(OverlayId=oid, RecipientId=peerid, RecipientCM="LinkManager",
                          Action="LNK_ADD_PEER_CAS", Params=params)
        lcbt = self.create_linked_cbt(parent_cbt)
        lcbt.set_request(self._module_name, "Signal", "SIG_REMOTE_ACTION", remote_act)
        self.submit_cbt(lcbt)
        self.free_cbt(cbt)

    def req_handler_add_peer_cas(self, cbt):
        # Create Link: Phase 7 Node B
        lnkid = cbt.request.params["LinkId"]
        self.register_cbt("Logger", "LOG_INFO", "Create Link: {} Phase 3/4 Node B".format(lnkid[:7]))
        params = cbt.request.params
        lcbt = self.create_linked_cbt(cbt)
        oid = params["OID"]
        params["Type"] = self._cm_config["Overlays"][oid]["Type"]
        lcbt.set_request(self._module_name, "TincanInterface", "TCI_CREATE_LINK", params)
        self.submit_cbt(lcbt)

    def resp_handler_create_link_endpt(self, cbt):
        parent_cbt = self.get_parent_cbt(cbt)
        resp_data = cbt.response.data
        if not cbt.response.status:
            self.register_cbt("Logger", "LOG_INFO", "Create link endpoint failed :{}".format(cbt))
            self.free_cbt(cbt)
            parent_cbt.set_response(resp_data, False)
            self.complete_cbt(parent_cbt)
            return

        if parent_cbt.request.action == "LNK_REQ_LINK_ENDPT":
            """
            To complete this request the responding node has to supply its own
            NodeData and CAS. The NodeData was previously queried and is stored
            on the parent cbt. Add the cas and send to peer.
            """
            self._complete_link_endpt_request(cbt)

        elif parent_cbt.request.action == "LNK_CREATE_LINK":
            """
            Both endpoints are created now but the peer doesn't have our cas.
            It already has the node data so no need to send that again.
            """
            self._send_local_cas_to_peer(cbt)

        elif parent_cbt.request.action == "LNK_ADD_PEER_CAS":
            # Create Link: Phase 8 Node B
            rem_act = parent_cbt.request.params
            lnkid = rem_act["LinkId"]
            self.register_cbt("Logger", "LOG_INFO", "Create Link:{} Phase 4/4 Node B"
                              .format(lnkid[:7]))
            peer_id = rem_act["NodeData"]["UID"]
            olid = rem_act["OID"]
            parent_cbt.set_response(data="LNK_ADD_PEER_CAS successful", status=True)
            # publish notification of link creation Node B
            param = {
                "UpdateType": "ADDED", "OverlayId": olid, "PeerId": peer_id,
                "LinkId": lnkid, "TapName": self._tunnels[lnkid]["Descriptor"]["TapName"]
                }
            self.free_cbt(cbt)
            self.complete_cbt(parent_cbt)
            self._link_updates_publisher.post_update(param)
            self.register_cbt("Logger", "LOG_DEBUG", "Link created: {0}:{1}->{2}"
                              .format(olid[:7], self._cm_config["NodeId"][:7], peer_id[:7]))

    def _complete_create_link_request(self, parent_cbt):
        # Create Link: Phase 9 Node A
        # Complete the cbt that started this all
        olid = parent_cbt.request.params["OverlayId"]
        peerid = parent_cbt.request.params["PeerId"]
        with self._lock:
            lnkid = self._peers[olid][peerid]
        self.register_cbt("Logger", "LOG_INFO", "Create Link:{} Phase 5/5 Node A".format(lnkid[:7]))
        parent_cbt.set_response(data={"LinkId": lnkid}, status=True)
        # publish notification of link creation Node A
        param = {
            "UpdateType": "ADDED", "OverlayId": olid, "PeerId": peerid,
            "LinkId": lnkid, "TapName": self._tunnels[lnkid]["Descriptor"]["TapName"]
            }
        self.complete_cbt(parent_cbt)
        self._link_updates_publisher.post_update(param)
        self.register_cbt("Logger", "LOG_DEBUG", "Link created: {0}:{1}->{2}"
                          .format(olid[:7], self._cm_config["NodeId"][:7], peerid[:7]))

    def resp_handler_remote_action(self, cbt):
        parent_cbt = self.get_parent_cbt(cbt)
        resp_data = cbt.response.data
        if not cbt.response.status:
            self.register_cbt("Logger", "LOG_INFO", "Remote Action failed :{}"
                              .format(cbt))
            self.free_cbt(cbt)
            parent_cbt.set_response(resp_data, False)
            self.complete_cbt(parent_cbt)
            return
        else:
            rem_act = cbt.response.data
            self.free_cbt(cbt)
            if rem_act["Action"] == "LNK_REQ_LINK_ENDPT":
                self._create_link_endpoint(rem_act, parent_cbt)
            elif rem_act["Action"] == "LNK_ADD_PEER_CAS":
                self._complete_create_link_request(parent_cbt)

    def req_handler_tincan_msg(self, cbt):
        if cbt.request.params["Command"] == "LinkStateChange":
            if cbt.request.params["Data"] == "LINK_STATE_DOWN":
                lnkid = cbt.request.params["LinkId"]
                oid = self._links[lnkid]["OverlayId"]
                olid = lnkid
                params = {"OID": oid, "OverlayId": olid, "LinkId": lnkid}
                self.register_cbt("TincanInterface", "TCI_REMOVE_LINK", params)
            cbt.set_response(data=None, status=True)
        else:
            cbt.set_response(data=None, status=False)
        self.complete_cbt(cbt)

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "LNK_CREATE_LINK":
                # Create Link: Phase 1 Node A
                # TOP wants a new link, first SIGnal peer to create endpt
                self.req_handler_create_link(cbt)

            elif cbt.request.action == "LNK_REQ_LINK_ENDPT":
                # Create Link: Phase 3 Node B
                # Rcvd peer req to create endpt, send to TCI
                self.req_handler_req_link_endpt(cbt)

            elif cbt.request.action == "LNK_ADD_PEER_CAS":
                # Create Link: Phase 7 Node B
                # CAS rcvd from peer, sends to TCI to update link's peer CAS info
                self.req_handler_add_peer_cas(cbt)

            elif cbt.request.action == "LNK_REMOVE_LINK":
                self.req_handler_remove_link(cbt)

            elif cbt.request.action == "LNK_QUERY_LINK_INFO":
                self.req_handler_query_link_info(cbt)

            elif cbt.request.action == "VIS_DATA_REQ":
                self.req_handler_query_viz_data(cbt)

            elif cbt.request.action == "TCI_TINCAN_MSG_NOTIFY":
                self.req_handler_tincan_msg(cbt)

            elif cbt.request.action == "LNK_ADD_IGN_INF":
                self.req_handler_add_ign_inf(cbt)
            else:
                self.req_handler_default(cbt)
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

            elif cbt.request.action == "TCI_CREATE_OVERLAY":
                # Create Link: Phase 2 Node A
                # Retrieved our node data for response
                self.resp_handler_create_overlay(cbt)

            elif cbt.request.action == "TCI_QUERY_LINK_STATS":
                self.resp_handler_query_link_stats(cbt)

            elif cbt.request.action == "TCI_REMOVE_LINK":
                self.resp_handler_remove_link(cbt)

            elif cbt.request.action == "TCI_REMOVE_OVERLAY":
                self.resp_handler_remove_overlay(cbt)

            else:
                parent_cbt = self.get_parent_cbt(cbt)
                cbt_data = cbt.response.data
                cbt_status = cbt.response.status
                self.free_cbt(cbt)
                if (parent_cbt is not None and parent_cbt.child_count == 1):
                    parent_cbt.set_response(cbt_data, cbt_status)
                    self.complete_cbt(parent_cbt)

    def timer_method(self):
        self._query_link_stats()

    def terminate(self):
        pass
