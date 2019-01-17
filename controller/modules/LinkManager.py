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

import os
import threading
import uuid
import time
from collections import defaultdict
from controller.framework.ControllerModule import ControllerModule


class LinkManager(ControllerModule):

    def __init__(self, cfx_handle, module_config, module_name):
        super(LinkManager, self).__init__(cfx_handle, module_config, module_name)
        self._tunnels = {}   # maps tunnel(link) id to its descriptor
        self._peers = {}     # maps overlay id to peers map, which maps peer id to link id
        self._lock = threading.Lock()  # serializes access to _overlays, _links
        self._link_updates_publisher = None
        self._ignored_net_interfaces = defaultdict(set)

    def __repr__(self):
        state = "<_peers: %s, _tunnels: %s>" % (self._peers, self._tunnels)
        return state

    def initialize(self):
        self._link_updates_publisher = \
            self._cfx_handle.publish_subscription("LNK_TUNNEL_EVENTS")
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

        # We need to ignore ALL the ipop tap devices (regardless of their overlay id/link id)
        for tnlid in self._tunnels:
            if self._tunnels[tnlid].get("Descriptor"):
                ign_tap_names.add(
                    self._tunnels[tnlid]["Descriptor"]["TapName"])
        # Overlay_id is only used to selectively ignore physical interfaces and bridges
        ign_tap_names \
            |= self._ignored_net_interfaces[overlay_id]
        return ign_tap_names

    def req_handler_add_ign_inf(self, cbt):
        ign_inf_details = cbt.request.params
        for olid in ign_inf_details:
            self._ignored_net_interfaces[olid].add(ign_inf_details[olid])
        cbt.set_response(None, True)
        self.complete_cbt(cbt)

    def req_handler_remove_tnl(self, cbt):
        """Remove the tunnel and link given either the overlay id and peer id, or the tunnel id"""
        # not currently being used
        olid = cbt.request.params.get("OverlayId", None)
        peer_id = cbt.request.params.get("PeerId", None)
        tnl_id = cbt.request.params.get("TunnelId", None)
        if olid is not None and peer_id is not None:
            tnl_id = self._peers[olid][peer_id]
        elif tnl_id is not None:
            olid = self._tunnels[tnl_id]["OverlayId"]
        else:
            cbt.set_response("Insufficient parameters", False)
            self.complete_cbt(cbt)
            return
        if self._tunnels[tnl_id]["TunnelState"] == "TNL_ONLINE" or \
            self._tunnels[tnl_id]["TunnelState"] == "TNL_OFFLINE":
            params = {"OverlayId": olid, "TunnelId": tnl_id, "PeerId": peer_id}
            self.register_cbt("TincanInterface", "TCI_REMOVE_TUNNEL", params)
        else:
            cbt.set_response("Tunnel busy, retry operation", False)
            self.complete_cbt(cbt)

    def _update_tunnel_descriptor(self, tnl_desc, tnl_id):
        """
        Update the tunnel desc with with lock owned
        """
        if tnl_id not in self._tunnels:
            self._tunnels[tnl_id] = dict(Descriptor=dict())
        if "Descriptor" not in self._tunnels[tnl_id]:
            self._tunnels[tnl_id]["Descriptor"] = dict()
        self._tunnels[tnl_id]["Descriptor"]["MAC"] = tnl_desc["MAC"]
        self._tunnels[tnl_id]["Descriptor"]["TapName"] = tnl_desc["TapName"]
        self._tunnels[tnl_id]["Descriptor"]["FPR"] = tnl_desc["FPR"]
        self.register_cbt("Logger", "LOG_DEBUG", "_tunnels:{}".format(self._tunnels))

    def _query_link_stats(self):
        """Query the status of links that have completed creation process"""
        params = []
        for link_id in self._tunnels:
            if self._tunnels[link_id]["Link"]["CreationState"] == 0xC0:
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
        #self.register_cbt("Logger", "LOG_INFO", "Tunnel stats: {0}".format(data))
        # Handle any connection failures and update tracking data
        for tnl_id in data:
            for lnkid in data[tnl_id]:
                if data[tnl_id][lnkid]["Status"] == "UNKNOWN":
                    self._cleanup_removed_tunnel(lnkid)
                elif lnkid in self._tunnels:
                    if data[tnl_id][lnkid]["Status"] == "OFFLINE":
                        # tincan indicates offline so recheck the link status
                        retry = self._tunnels[lnkid]["Link"].get("StatusRetry", 0)
                        if retry >= 2 and self._tunnels[lnkid]["TunnelState"] == "TNL_CREATING":
                            # link is stuck creating so destroy it
                            olid = self._tunnels[lnkid]["OverlayId"]
                            params = {"OverlayId": olid, "TunnelId": tnl_id, "LinkId": lnkid}
                            self.register_cbt("TincanInterface", "TCI_REMOVE_TUNNEL", params)
                        elif retry >= 1 and self._tunnels[lnkid]["TunnelState"] == "TNL_QUERYING":
                            # link went offline so notify top
                            self._tunnels[lnkid]["TunnelState"] = "TNL_OFFLINE"
                            olid = self._tunnels[lnkid]["OverlayId"]
                            peer_id = self._tunnels[lnkid]["PeerId"]
                            param = {
                                "UpdateType": "DISCONNECTED", "OverlayId": olid, "PeerId": peer_id,
                                "TunnelId": lnkid, "LinkId": lnkid,
                                "TapName": self._tunnels[lnkid]["Descriptor"]["TapName"]}
                            self._link_updates_publisher.post_update(param)
                        else:
                            self._tunnels[lnkid]["Link"]["StatusRetry"] = retry + 1
                    elif data[tnl_id][lnkid]["Status"] == "ONLINE":
                        self._tunnels[lnkid]["TunnelState"] = "TNL_ONLINE"
                        self._tunnels[lnkid]["Link"]["IceRole"] = data[tnl_id][lnkid]["IceRole"]
                        self._tunnels[lnkid]["Link"]["Stats"] = data[tnl_id][lnkid]["Stats"]
                        self._tunnels[lnkid]["Link"]["StatusRetry"] = 0
                    else:
                        self.register_cbt("Logger", "LOG_WARNING", "Unrecognized tunnel state "
                                          "{0}:{1}".format(lnkid, data[tnl_id][lnkid]["Status"]))
        self.free_cbt(cbt)

    def _cleanup_removed_tunnel(self, tnlid):
        tnl = self._tunnels.pop(tnlid, None)
        if tnl:
            peer_id = tnl["PeerId"]
            olid = tnl["OverlayId"]
            self._peers[olid].pop(peer_id, None)


    def resp_handler_remove_tunnel(self, rmv_tnl_cbt):
        """
        Clean up the tunnel meta data. Even of the CBT fails it is safe to discard
        as this is because Tincan has no record of it.
        """
        parent_cbt = rmv_tnl_cbt.parent
        tnlid = rmv_tnl_cbt.request.params["TunnelId"]
        peer_id = rmv_tnl_cbt.request.params["PeerId"]
        olid = rmv_tnl_cbt.request.params["OverlayId"]
        # Notify subscribers of tunnel removal
        param = {
            "UpdateType": "REMOVED", "OverlayId": olid, "TunnelId": tnlid, "LinkId": tnlid,
            "PeerId": peer_id}
        if "TapName" in self._tunnels[tnlid]["Descriptor"]:
            param["TapName"] = self._tunnels[tnlid]["Descriptor"]["TapName"]
        self._link_updates_publisher.post_update(param)
        self._cleanup_removed_tunnel(tnlid)
        self.free_cbt(rmv_tnl_cbt)
        if parent_cbt is not None:
            parent_cbt.set_response("Tunnel removed", True)
            self.complete_cbt(parent_cbt)
        self.register_cbt("Logger", "LOG_INFO", "Tunnel {0} removed: {1}:{2}<->{3}"
                          .format(tnlid[:7], olid[:7], self._cm_config["NodeId"][:7], peer_id[:7]))
        #self.register_cbt("Logger", "LOG_DEBUG", "State:\n" + str(self))

    def req_handler_query_tunnels_info(self, cbt):
        results = {}
        for tnlid in self._tunnels:
            results[tnlid] = {"OverlayId": self._tunnels[tnlid]["OverlayId"],
                              "TunnelId": tnlid, "PeerId": self._tunnels[tnlid]["PeerId"],
                              "Stats": self._tunnels[tnlid]["Link"]["Stats"]}
        cbt.set_response(results, status=True)
        self.complete_cbt(cbt)

    def _create_tunnel(self, params, parent_cbt=None):
        overlay_id = params["OverlayId"]
        ol_type = self._cm_config["Overlays"][overlay_id]["Type"]
        lnkid = params["LinkId"]
        peer_id = params["PeerId"]
        tap_name = self._cm_config["Overlays"][overlay_id]["TapName"][:8] + str(peer_id[:7])
        if os.name == "nt":
            tap_name = self._cm_config["Overlays"][overlay_id]["TapName"]
        create_tnl_params = {
            "OverlayId": overlay_id,
            "NodeId": self._cm_config["NodeId"],
            "TunnelId": lnkid,
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
            create_tnl_params["TurnServers"] = self._cm_config["Turn"]

        if parent_cbt is not None:
            tnl_cbt = self.create_linked_cbt(parent_cbt)
            tnl_cbt.set_request(self._module_name, "TincanInterface",
                                "TCI_CREATE_TUNNEL", create_tnl_params)
        else:
            tnl_cbt = self.create_cbt(self._module_name, "TincanInterface",
                                      "TCI_CREATE_TUNNEL", create_tnl_params)
        self.submit_cbt(tnl_cbt)

    def _request_peer_endpoint(self, params, parent_cbt):
        overlay_id = params["OverlayId"]
        lnkid = params["LinkId"]
        tnl_dscr = self._tunnels[lnkid]["Descriptor"]
        endp_param = {
            "NodeData": {
                "FPR": tnl_dscr["FPR"],
                "MAC": tnl_dscr["MAC"],
                "UID": self._cm_config["NodeId"]}}
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

    def _rollback_tnl_creation_changes(self, tnl_id):
        """
        Remove the tunnel that failed at some point while creating it.
        """
        tnl = self._tunnels.pop(tnl_id, None)
        if tnl:
            olid = tnl["OverlayId"]
            peer_id = tnl["PeerId"]
            self._peers[olid].pop(peer_id, None)

    def _rollback_link_creation_changes(self, link_id):
        """
        Removes links that failed the setup handshake. Does not currently complete pending CBTs.
        This needs to be handled or these CBTs will remain in the pending queue.
        """
        if link_id not in self._tunnels:
            return
        creation_state = self._tunnels[link_id]["Link"]["CreationState"]
        if creation_state < 0xC0:
            olid = self._tunnels[link_id]["OverlayId"]
            peer_id = self._tunnels[link_id]["PeerId"]
            params = {"OverlayId": olid, "PeerId": peer_id, "TunnelId": link_id, "LinkId": link_id}
            self.register_cbt("TincanInterface", "TCI_REMOVE_TUNNEL", params)

            self.register_cbt("Logger", "LOG_INFO", "Initiated removal of incomplete link: "
                              "PeerId:{2}, LinkId:{0}, CreateState:{1}"
                              .format(link_id[:7], format(creation_state, "02X"), peer_id[:7]))

    def req_handler_create_tunnel(self, cbt):
        """
        Handle the request for capability LNK_CREATE_TUNNEL.
        The caller provides the overlay id and the peer id which the link
        connects. The link id is generated here but it is returned to the
        caller after the local endpoint creation is completed asynchronously.
        The link is not necessarily ready for read/write at this time. The link
        status can be queried to determine when it is writeable. The link id is
        communicated in the request and will be the same at both nodes.
        """
        # Create Link: Phase 1 Node A
        tnl_id = uuid.uuid4().hex
        overlay_id = cbt.request.params["OverlayId"]
        peerid = cbt.request.params["PeerId"]
        if peerid in self._peers[overlay_id]:
            # Link already exists, TM should clean up first
            cbt.set_response("A link already exist or is being created for "
                             "overlay id: {0} peer id: {1}"
                             .format(overlay_id, peerid), False)
            self.complete_cbt(cbt)
            return
        # index for quick peer->link lookup
        self._peers[overlay_id][peerid] = tnl_id
        self._tunnels[tnl_id] = dict(OverlayId=overlay_id,
                                     PeerId=peerid,
                                     TunnelState="TNL_CREATING",
                                     Descriptor=dict(),
                                     CreationStartTime=time.time(),
                                     Link=dict(CreationState=0xA1, Stats=dict()))

        self.register_cbt("Logger", "LOG_DEBUG", "Create Link:{} Phase 1/5 Node A"
                          .format(tnl_id[:7]))
        lnkupd_param = {
            "UpdateType": "CREATING", "OverlayId": overlay_id, "PeerId": peerid,
            "TunnelId": tnl_id, "LinkId": tnl_id}
        self._link_updates_publisher.post_update(lnkupd_param)

        params = {"OverlayId": overlay_id, "TunnelId": tnl_id, "LinkId": tnl_id,
                  "Type": self._cm_config["Overlays"][overlay_id]["Type"], "PeerId": peerid}
        self._create_tunnel(params, parent_cbt=cbt)

    def resp_handler_create_tunnel(self, cbt):
        # Create Link: Phase 2 Node A
        parent_cbt = cbt.parent
        lnkid = cbt.request.params["LinkId"]  # config overlay id
        resp_data = cbt.response.data
        if not cbt.response.status:
            self._rollback_tnl_creation_changes(lnkid)
            self.free_cbt(cbt)
            parent_cbt.set_response(resp_data, False)
            self.complete_cbt(parent_cbt)
            self.register_cbt("Logger", "LOG_WARNING", "The create tunnel operation failed:{}"
                              .format(parent_cbt.response.data))
            return
        # transistion connection connection state
        self._tunnels[lnkid]["Link"]["CreationState"] = 0xA2
        # store the overlay data
        overlay_id = cbt.request.params["OverlayId"]  # config overlay id
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link:{} Phase 2/5 Node A"
                          .format(lnkid[:7]))
        self._update_tunnel_descriptor(resp_data, lnkid)
        # create and send remote action to request endpoint from peer
        params = {"OverlayId": overlay_id, "TunnelId": lnkid, "LinkId": lnkid}
        self._request_peer_endpoint(params, parent_cbt)
        self.free_cbt(cbt)

    def req_handler_req_link_endpt(self, cbt):
        """
        Handle the request for capability LNK_REQ_LINK_ENDPT.
        This request occurs on the remote node B. It determines if it can
        facilitate a link between itself and the requesting node A.
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
        if peer_id in self._peers[overlay_id]:
            cbt.set_response("A tunnel already exists with this peer", False)
            self.complete_cbt(cbt)
            self.register_cbt("Logger", "LOG_INFO", "A create link endpoint request from a "
                              "paired peer was rejected {0}:{1}:{2}"
                              .format(overlay_id[:7], peer_id[:7], lnkid[:7]))
            return
        #if len(self._tunnels) > 10: # parameterize this
        #    cbt.set_response("No tunnels currently available", False)
        #    self.complete_cbt(cbt)
        #    self.register_cbt("Logger", "LOG_INFO", "A create link endpoint request was "
        #                      "discarded as the maximum number of tunnels has been reached. {0}"
        #                      . format(cbt))
        #    return
        # add to index for peer->link lookup
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link:{} Phase 1/4 Node B"
                          .format(lnkid[:7]))
        self._peers[overlay_id][peer_id] = lnkid
        self._tunnels[lnkid] = dict(OverlayId=overlay_id,
                                    PeerId=peer_id,
                                    TunnelState="TNL_CREATING",
                                    Descriptor=dict(),
                                    CreationStartTime=time.time(),
                                    Link=dict(CreationState=0xB1,
                                              Stats=dict()))

        # publish notification of link creation initiated Node B
        lnkupd_param = {
            "UpdateType": "CREATING", "OverlayId": overlay_id, "PeerId": peer_id,
            "TunnelId": lnkid, "LinkId": lnkid}
        self._link_updates_publisher.post_update(lnkupd_param)
        # Send request to Tincan
        ol_type = self._cm_config["Overlays"][overlay_id]["Type"]
        tap_name = self._cm_config["Overlays"][overlay_id]["TapName"][:8] + str(peer_id[:7])
        create_link_params = {
            "OverlayId": overlay_id,
            # overlay params
            "TunnelId": lnkid,
            "NodeId": self._cm_config["NodeId"],
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
                "UID": node_data["UID"]}}
        if self._cm_config.get("Turn"):
            create_link_params["TurnServers"] = self._cm_config["Turn"]
        lcbt = self.create_linked_cbt(cbt)
        lcbt.set_request(self._module_name, "TincanInterface",
                         "TCI_CREATE_LINK", create_link_params)
        self.submit_cbt(lcbt)

    def _complete_link_endpt_request(self, cbt):
        # Create Link: Phase 4 Node B
        parent_cbt = cbt.parent
        resp_data = cbt.response.data
        if not cbt.response.status:
            self.free_cbt(cbt)
            parent_cbt.set_response(resp_data, False)
            if parent_cbt.child_count == 1:
                self.complete_cbt(parent_cbt)
            self.register_cbt("Logger", "LOG_WARNING", "Create link endpoint failed :{}"
                              .format(cbt.response.data))
            return
        lnkid = cbt.request.params["LinkId"]
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link:{} Phase 2/4 Node B"
                          .format(lnkid[:7]))
        # store the overlay data
        self._update_tunnel_descriptor(resp_data, lnkid)
        self._tunnels[lnkid]["Link"]["CreationState"] = 0xB2
        # respond with this nodes connection parameters
        node_data = {
            "MAC": resp_data["MAC"],
            "FPR": resp_data["FPR"],
            "UID": self._cm_config["NodeId"],
            "CAS": resp_data["CAS"]
        }
        data = {
            "OverlayId": cbt.request.params["OverlayId"],
            "TunnelId": lnkid,
            "LinkId": lnkid,
            "NodeData": node_data
        }
        self.free_cbt(cbt)
        parent_cbt.set_response(data, True)
        self.complete_cbt(parent_cbt)

    def _complete_link_creation(self, cbt, parent_cbt):
        """
        Complete the parent cbt to add the peers CAS and update link created subscription
        """
        # Create Link: Phase 8 Node B
        rem_act = parent_cbt.request.params
        lnkid = rem_act["LinkId"]
        self._tunnels[lnkid]["Link"]["CreationState"] = 0xC0
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link:{} Phase 4/4 Node B"
                          .format(lnkid[:7]))
        peer_id = rem_act["NodeData"]["UID"]
        olid = rem_act["OverlayId"]
        resp_data = cbt.response.data
        node_data = {
            "MAC": resp_data["MAC"],
            "FPR": resp_data["FPR"],
            "UID": self._cm_config["NodeId"],
            "CAS": resp_data["CAS"]
        }
        data = {
            "OverlayId": cbt.request.params["OverlayId"],
            "TunnelId": lnkid,
            "LinkId": lnkid,
            "NodeData": node_data
        }
        parent_cbt.set_response(data=data, status=True)
        self.free_cbt(cbt)
        self.complete_cbt(parent_cbt)
        self.register_cbt("Logger", "LOG_INFO", "Tunnel {0} accepted: {1}:{2}<-{3}"
                          .format(lnkid[:7], olid[:7], self._cm_config["NodeId"][:7], peer_id[:7]))

    def _create_link_endpoint(self, rem_act, parent_cbt):
        """
        Send the Createlink control to local Tincan
        """
        # Create Link: Phase 5 Node A
        lnkid = rem_act["Data"]["LinkId"]
        if lnkid not in self._tunnels:
            # abort the handshake as the process timed out
            parent_cbt.set_response("Tunnel creation timeout failure", False)
            self.complete_cbt(parent_cbt)
            return
        self._tunnels[lnkid]["Link"]["CreationState"] = 0xA3
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link:{} Phase 3/5 Node A"
                          .format(lnkid[:7]))
        node_data = rem_act["Data"]["NodeData"]
        olid = rem_act["OverlayId"]
        cbt_params = {"OverlayId": olid, "TunnelId": lnkid, "LinkId": lnkid, "Type": "TUNNEL",
                      "NodeData": {
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
        self._tunnels[lnkid]["Link"]["CreationState"] = 0xA4
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link:{} Phase 4/5 Node A"
                          .format(lnkid[:7]))
        local_cas = cbt.response.data["CAS"]
        parent_cbt = cbt.parent
        olid = cbt.request.params["OverlayId"]
        peerid = parent_cbt.request.params["PeerId"]
        params = {
            "OverlayId": olid,
            "TunnelId": lnkid,
            "LinkId": lnkid,
            "NodeData": {
                "UID": self._cm_config["NodeId"], "MAC": cbt.response.data["MAC"],
                "CAS": local_cas, "FPR": cbt.response.data["FPR"]}}
        remote_act = dict(OverlayId=olid, RecipientId=peerid, RecipientCM="LinkManager",
                          Action="LNK_ADD_PEER_CAS", Params=params)
        lcbt = self.create_linked_cbt(parent_cbt)
        lcbt.set_request(self._module_name, "Signal", "SIG_REMOTE_ACTION", remote_act)
        self.submit_cbt(lcbt)
        self.free_cbt(cbt)

    def req_handler_add_peer_cas(self, cbt):
        # Create Link: Phase 7 Node B
        params = cbt.request.params
        olid = params["OverlayId"]
        lnkid = params["LinkId"]
        peer_id = params["NodeData"]["UID"]
        if peer_id not in self._peers[olid] or lnkid not in self._tunnels:
            self._cleanup_removed_tunnel(lnkid)
            self.register_cbt("Logger", "LOG_DEBUG",
                              "A response to an aborted add peer CAS operation was discarded: {0}".
                              format(str(cbt)))

        self._tunnels[lnkid]["Link"]["CreationState"] = 0xB3
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link: {} Phase 3/4 Node B"
                          .format(lnkid[:7]))
        lcbt = self.create_linked_cbt(cbt)
        params["Type"] = self._cm_config["Overlays"][olid]["Type"]
        lcbt.set_request(self._module_name, "TincanInterface", "TCI_CREATE_LINK", params)
        self.submit_cbt(lcbt)

    def resp_handler_create_link_endpt(self, cbt):
        parent_cbt = cbt.parent
        resp_data = cbt.response.data
        if not cbt.response.status:
            link_id = cbt.request.params["LinkId"]
            self._rollback_link_creation_changes(link_id)
            self.register_cbt("Logger", "LOG_WARNING", "Create link endpoint failed :{}"
                              .format(cbt))
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

        elif parent_cbt.request.action == "LNK_CREATE_TUNNEL":
            """
            Both endpoints are created now but the peer doesn't have our cas.
            It already has the node data so no need to send that again.
            """
            self._send_local_cas_to_peer(cbt)

        elif parent_cbt.request.action == "LNK_ADD_PEER_CAS":
            """
            The link creation handshake is complete on Node B, complete the outstanding request
            and publish notifications via subscription.
            """
            self._complete_link_creation(cbt, parent_cbt)

    def _complete_create_link_request(self, parent_cbt):
        # Create Link: Phase 9 Node A
        # Complete the cbt that started this all
        olid = parent_cbt.request.params["OverlayId"]
        peer_id = parent_cbt.request.params["PeerId"]
        if peer_id not in self._peers[olid]:
            self.register_cbt("Logger", "LOG_DEBUG",
                              "A response to an aborted create link operation was discarded: {0}".
                              format(parent_cbt))
            return
        lnkid = self._peers[olid][peer_id]
        self._tunnels[lnkid]["Link"]["CreationState"] = 0xC0
        self.register_cbt("Logger", "LOG_DEBUG", "Create Link:{} Phase 5/5 Node A"
                          .format(lnkid[:7]))
        parent_cbt.set_response(data={"LinkId": lnkid}, status=True)
        self.complete_cbt(parent_cbt)
        self.register_cbt("Logger", "LOG_INFO", "Tunnel {0} created: {1}:{2}->{3}"
                          .format(lnkid[:7], olid[:7], self._cm_config["NodeId"][:7], peer_id[:7]))

    def resp_handler_remote_action(self, cbt):
        parent_cbt = cbt.parent
        resp_data = cbt.response.data
        if not cbt.response.status:
            lnkid = cbt.request.params["Params"]["LinkId"]
            self._rollback_link_creation_changes(lnkid)
            self.free_cbt(cbt)
            parent_cbt.set_response(resp_data, False)
            self.complete_cbt(parent_cbt)
        else:
            rem_act = cbt.response.data
            self.free_cbt(cbt)
            if rem_act["Action"] == "LNK_REQ_LINK_ENDPT":
                self._create_link_endpoint(rem_act, parent_cbt)
            elif rem_act["Action"] == "LNK_ADD_PEER_CAS":
                self._complete_create_link_request(parent_cbt)

    def req_handler_tincan_msg(self, cbt):
        lts = time.time()
        if cbt.request.params["Command"] == "LinkStateChange":
            if cbt.request.params["Data"] == "LINK_STATE_DOWN":
                # issue a link state check
                lnkid = cbt.request.params["LinkId"]
                self._tunnels[lnkid]["TunnelState"] = "TNL_QUERYING"
                self.register_cbt("TincanInterface", "TCI_QUERY_LINK_STATS", [lnkid])
            if cbt.request.params["Data"] == "LINK_STATE_UP":
                lnkid = cbt.request.params["LinkId"]
                olid = self._tunnels[lnkid]["OverlayId"]
                peer_id = self._tunnels[lnkid]["PeerId"]
                lnk_status = self._tunnels[lnkid]["TunnelState"]
                self._tunnels[lnkid]["TunnelState"] = "TNL_ONLINE"
                if lnk_status != "TNL_QUERYING":
                    param = {
                        "UpdateType": "CONNECTED", "OverlayId": olid, "PeerId": peer_id,
                        "TunnelId": lnkid, "LinkId": lnkid, "ConnectedTimestamp": lts,
                        "TapName": self._tunnels[lnkid]["Descriptor"]["TapName"]}
                    self._link_updates_publisher.post_update(param)
                elif lnk_status == "TNL_QUERYING":
                    # Do not post a notification if the the connection state was being queried
                    self._tunnels[lnkid]["Link"]["StatusRetry"] = 0
                # if the lnk_status is TNL_OFFLINE the recconect event came in too late and the
                # tear down has already been issued. This scenario is unlikely as the recheck time
                # is long enough such that the webrtc reconnect attempts will have been abandoned.
            cbt.set_response(data=None, status=True)
        else:
            cbt.set_response(data=None, status=True)
        self.complete_cbt(cbt)

    def process_cbt(self, cbt):
        with self._lock:
            if cbt.op_type == "Request":
                if cbt.request.action == "LNK_CREATE_TUNNEL":
                    # Create Link: Phase 1 Node A
                    # TOP wants a new link, first SIGnal peer to create endpt
                    self.req_handler_create_tunnel(cbt)

                elif cbt.request.action == "LNK_REQ_LINK_ENDPT":
                    # Create Link: Phase 3 Node B
                    # Rcvd peer req to create endpt, send to TCI
                    self.req_handler_req_link_endpt(cbt)

                elif cbt.request.action == "LNK_ADD_PEER_CAS":
                    # Create Link: Phase 7 Node B
                    # CAS rcvd from peer, sends to TCI to update link's peer CAS info
                    self.req_handler_add_peer_cas(cbt)

                elif cbt.request.action == "LNK_REMOVE_TUNNEL":
                    self.req_handler_remove_tnl(cbt)

                elif cbt.request.action == "LNK_QUERY_LINK_INFO":
                    self.req_handler_query_tunnels_info(cbt)

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

                elif cbt.request.action == "TCI_CREATE_TUNNEL":
                    # Create Link: Phase 2 Node A
                    # Retrieved our node data for response
                    self.resp_handler_create_tunnel(cbt)

                elif cbt.request.action == "TCI_QUERY_LINK_STATS":
                    self.resp_handler_query_link_stats(cbt)

                # elif cbt.request.action == "TCI_REMOVE_LINK":
                #    self.resp_handler_remove_link(cbt)

                elif cbt.request.action == "TCI_REMOVE_TUNNEL":
                    self.resp_handler_remove_tunnel(cbt)

                else:
                    parent_cbt = cbt.parent
                    cbt_data = cbt.response.data
                    cbt_status = cbt.response.status
                    self.free_cbt(cbt)
                    if (parent_cbt is not None and parent_cbt.child_count == 1):
                        parent_cbt.set_response(cbt_data, cbt_status)
                        self.complete_cbt(parent_cbt)

    def _cleanup_expired_incomplete_links(self):
        link_expire = 4*self._cm_config["TimerInterval"]
        link_ids = list(self._tunnels.keys())
        for link_id in link_ids:
            tnl = self._tunnels[link_id]
            if (tnl["Link"]["CreationState"] != 0xC0 and \
                time.time() - tnl["CreationStartTime"] > link_expire):
                self._rollback_link_creation_changes(link_id)

    def timer_method(self):
        with self._lock:
            self._cleanup_expired_incomplete_links()
            self._query_link_stats()
            self.register_cbt("Logger", "LOG_DEBUG", "Timer LNK State:\n" + str(self))

    def terminate(self):
        pass

    def req_handler_query_viz_data(self, cbt):
        node_id = str(self._cm_config["NodeId"])
        tnls = dict()
        for tnlid in self._tunnels:
            tnl_data = {
                "NodeId": node_id,
                "PeerId": self._tunnels[tnlid]["PeerId"],
                "TunnelState": self._tunnels[tnlid]["TunnelState"]
                }
            descriptor = self._tunnels[tnlid]["Descriptor"]
            if "TapName" in descriptor:
                tnl_data["TapName"] = descriptor["TapName"]
            if "MAC" in descriptor:
                tnl_data["MAC"] = descriptor["MAC"]
            if "IceRole" in self._tunnels[tnlid]["Link"]:
                tnl_data["IceRole"] = self._tunnels[tnlid]["Link"]["IceRole"]
            if "Stats" in self._tunnels[tnlid]["Link"]:
                tnl_data["Stats"] = self._tunnels[tnlid]["Link"]["Stats"]
            overlay_id = self._tunnels[tnlid]["OverlayId"]

            if overlay_id not in tnls:
                tnls[overlay_id] = dict()
            if node_id not in tnls[overlay_id]:
                tnls[overlay_id][node_id] = dict()
            tnls[overlay_id][node_id][tnlid] = tnl_data

        cbt.set_response({"LinkManager": tnls}, bool(tnls))
        self.complete_cbt(cbt)
