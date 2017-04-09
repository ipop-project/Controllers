from controller.framework.ControllerModule import ControllerModule
import time,json,math

class ConnectionManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(ConnectionManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.connection_details = {}
        self.CMConfig = paramDict
        tincanparams = self.CFxHandle.queryParam("Tincan","Vnets")
        self.CMConfig["num_successors"] = self.CFxHandle.queryParam("BaseTopologyManager","NumberOfSuccessors")
        self.CMConfig["num_chords"]     = self.CFxHandle.queryParam("BaseTopologyManager","NumberOfChords")
        self.CMConfig["ttl_on_demand"]  = self.CFxHandle.queryParam("BaseTopologyManager","OnDemandLinkTTL")
        for k in range(len(tincanparams)):
            interface_name = tincanparams[k]["TapName"]
            self.connection_details[interface_name] = {}
            self.connection_details[interface_name]["successor"] = {}
            self.connection_details[interface_name]["chord"]     = {}
            self.connection_details[interface_name]["on_demand"] = {}
            self.connection_details[interface_name]["log_chords"] = []
            self.connection_details[interface_name]["xmpp_client_code"] = tincanparams[k]["XMPPModuleName"]
            self.connection_details[interface_name]["uid"]       = tincanparams[k]["uid"]
        self.CMConfig = paramDict

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def send_msg_srv(self, msg_type, uid, msg,interface_name):
        cbtdata = {"method": msg_type, "overlay_id": 0, "uid": uid, "data": msg,"interface_name":interface_name}
        self.registerCBT(self.connection_details[interface_name]["xmpp_client_code"], 'DO_SEND_MSG', cbtdata)


    def request_connection(self, con_type, uid,interface_name,data,ttl):
        # send connection request to larger nodes
        self.connection_details[interface_name][con_type][uid] = { "ttl":ttl ,"status": "con_req","mac":""}
        try:
            self.send_msg_srv("con_req", uid, json.dumps(data),interface_name)
        except:
            self.registerCBT('Logger', 'info', "Exception in send_msg_srv con_req")

        log = "sent con_req ({0}): {1}".format(con_type, uid)
        self.registerCBT('Logger', 'debug', log)

    def respond_connection(self,con_type,data,interface_name,uid,ttl):

        #changes done as part of release 17.0
        peer_mac = data.pop("peer_mac")
        self.connection_details[interface_name][con_type][uid] = {"ttl" : ttl, "status": "con_resp","mac": peer_mac}
        self.send_msg_srv("con_ack", uid, json.dumps(data),interface_name)
        log = "sent con_ack to {0}".format(uid)
        self.registerCBT('Logger','debug', log)

    # remove connection
    # remove a link by peer UID
    # - uid = UID of the peer
    def remove_connection(self, uid, interface_name):
        for con_type in ["successor", "chord", "on_demand"]:
            if uid in self.connection_details[interface_name][con_type].keys():
                if self.connection_details[interface_name][con_type][uid]["status"] in ["online", "offline"]:
                    if "mac" in list(self.connection_details[interface_name][con_type][uid].keys()):
                        mac = self.connection_details[interface_name][con_type][uid]["mac"]
                        if mac != None and mac != "":
                            msg = {"interface_name": interface_name, "uid": uid, "MAC": mac}
                            self.registerCBT('TincanSender', 'DO_TRIM_LINK', msg)
                self.connection_details[interface_name][con_type].pop(uid)
                message = {"uid": uid, "interface_name": interface_name, "msg_type": "remove_peer"}
                self.registerCBT("BaseTopologyManager", "UpdateConnectionDetails", message)

                log = "removed connection: {0}".format(uid)
                self.registerCBT('Logger', 'info', log)

    def update_connection(self,data):
        uid = data["uid"]
        interface_name  =  data["interface_name"]
        for con_type in ["successor","chord","on_demand"]:
            if uid in self.connection_details[interface_name][con_type].keys():
                self.connection_details[interface_name][con_type][uid]["ttl"]    = data["ttl"]
                self.connection_details[interface_name][con_type][uid]["stats"]  = data["stats"]
                self.connection_details[interface_name][con_type][uid]["status"] = data["status"]
                self.connection_details[interface_name][con_type][uid]["mac"]    =  data["mac"]

    # clean connections
    #  remove peers with expired time-to-live attributes
    def clean_connections(self, interface_name):
        # time-to-live attribute indicative of an offline link
        links = self.connection_details[interface_name]

        #for uid in list(self.ipop_interface_details[interface_name]["peers"].keys()):
        successors = list(links["successor"].keys())
        for uid in successors:
            # Check if there exists a link
            #if self.linked(uid,interface_name):
                # check whether the time to link has expired
                if time.time() > links["successor"][uid]["ttl"]:
                    self.remove_connection(uid, interface_name)
                    log = "Time to Live expired going to remove peer: {0}".format(uid)
                    self.registerCBT('Logger', 'info', log)
                    message = {"uid":uid,"interface_name":interface_name,"msg_type":"remove_peer"}
                    self.registerCBT("BaseTopologyManager","UpdateConnectionDetails",message)

        # periodically call policy for link removal
        self.clean_chord(interface_name)
        self.clean_on_demand(interface_name)

    ############################################################################
        # chords policy                                                            #
    ############################################################################
        # [1] A forwards a headless find_chord message approximated by a designated UID
        # [2] B discovers that it is the closest node to the designated UID
        #     B responds with a found_chord message to A
        # [3] A requests to link to B as A's chord
        # [4] B accepts A's link request, with A as B's inbound link
        #     B responds to link to A
        # [5] A and B are connected
        # [*] the link is terminated when the chord time-to-live attribute expires and
        #     a better chord was found or the link disconnects

    def find_chords(self, interface_name,current_node_uid):
        # find chords closest to the approximate logarithmic nodes
        link_details = self.connection_details[interface_name]

        if len(link_details["log_chords"]) == 0:
            for i in reversed(range(self.CMConfig["num_chords"])):
                log_num = (int(current_node_uid, 16) + int(math.pow(2, 160 - 1 - i))) % int(
                        math.pow(2, 160))
                log_uid = "{0:040x}".format(log_num)
                link_details["log_chords"].append(log_uid)

        # determine list of designated UIDs
        log_chords = link_details["log_chords"]
        for chord in link_details["chord"].values():
            if "log_uid" in chord.keys():
                if chord["log_uid"] in log_chords:
                    log_chords.remove(chord["log_uid"])

        # forward find_chord messages to the nodes closest to the designated UID
        for log_uid in log_chords:
            # forward find_chord message
            new_msg = {
                    "fwd_type"  : "closest",
                    "dst_uid"   : log_uid,
                    "interface_name" : interface_name,
                    "data"      : {
                                    "msg_type": "find_chord",
                                    "src_uid": current_node_uid,
                                    "dst_uid": log_uid,
                                    "log_uid": log_uid
                    }
            }

            self.registerCBT("BaseTopologyManager", "forward_msg", new_msg)

    def add_chord(self, uid, log_uid, interface_name):
        # if a chord associated with log_uid already exists, check if the found
        # chord is the same chord:
        # if they are the same then the chord is already the best one available
        # otherwise, remove the chord and link to the found chord

        for chord in list(self.connection_details[interface_name]["chord"].keys()):
            if self.connection_details[interface_name]["chord"][chord]["log_uid"] == log_uid:
                if chord == uid:
                    return
                else:
                    self.remove_link("chord", chord, interface_name)

        # add chord link
        self.connection_details[interface_name]["chord"][uid] = {
            "log_uid": log_uid,
            "ttl": time.time() + self.CMConfig["ChordLinkTTL"]
        }

    def clean_chord(self,interface_name):
        links = self.connection_details[interface_name]
        if not links["chord"].keys():
            return

        # find chord with the oldest time-to-live attribute
        uid = min(links["chord"].keys(), key=lambda u: (links["chord"][u]["ttl"]))

        # time-to-live attribute has expired: determine if a better chord exists
        if time.time() > links["chord"][uid]["ttl"]:
            # forward find_chord message
            if "log_uid" in links["chord"][uid].keys():
                new_msg = {
                    "msg_type": "find_chord",
                    "src_uid": self.connection_details[interface_name]["uid"],
                    "dst_uid": links["chord"][uid]["log_uid"],
                    "log_uid": links["chord"][uid]["log_uid"]
                }

                #self.forward_msg("closest", links["chord"][uid]["log_uid"], new_msg,interface_name)   # TO DO LInk to BTM
                forward_message = {
                    "fwd_type"  : "closest",
                    "dst_uid"   : links["chord"][uid]["log_uid"],
                    "interface_name" : interface_name,
                    "data" : new_msg
                }
                self.registerCBT("BaseTopologyManager","forward_msg", forward_message)

                # extend time-to-live attribute
                links["chord"][uid]["ttl"] = time.time() + self.CMConfig["ttl_chord"]

    def linked(self, uid, interface_name):
        if uid in self.connection_details[interface_name]["successor"].keys():
            if "status" in self.connection_details[interface_name]["successor"][uid].keys():
                if self.connection_details[interface_name]["successor"][uid]["status"] == "online":
                    return True
        if uid in self.connection_details[interface_name]["on_demand"].keys():
            if "status" in self.connection_details[interface_name]["on_demand"][uid].keys():
                if self.connection_details[interface_name]["on_demand"][uid]["status"] == "online":
                    return True
        if uid in self.connection_details[interface_name]["chord"].keys():
            if "status" in self.connection_details[interface_name]["chord"][uid].keys():
                if self.connection_details[interface_name]["chord"][uid]["status"] == "online":
                    return True
        return False

    def clean_on_demand(self,interface_name):
        for uid,link_details in self.connection_details[interface_name]["on_demand"].items():

            # rate exceeds threshold: increase time-to-live attribute
            if link_details["stats"]["rate"] >= self.CMConfig["OndemandLinkRateThreshold"]:
                link_details["ttl"] = time.time() + self.CMConfig["ttl_on_demand"]
            # rate is below theshold and the time-to-live attribute expired: remove link
            elif time.time() > link_details["ttl"]:
                self.remove_link("on_demand", uid,interface_name)

    def remove_link(self, con_type, uid, interface_name):
        # remove peer from link type
        links  = self.connection_details[interface_name]
        if uid in links[con_type].keys():
            links[con_type].pop(uid)

        # this peer does not have any outbound links
        if uid not in links["successor"].keys() + links["chord"].keys() + links["on_demand"].keys():
            # remove connection
            self.remove_connection(uid, interface_name)  #TODO

    def remove_successors(self,interface_name,current_uid):
        # sort nodes into rotary, unique list with respect to this UID
        links = self.connection_details[interface_name]

        successors = list(sorted(links["successor"].keys()))

        #Allow the least node in the network to have the as many connections as required to maintain a fully connected
        #Ring Topology

        if max([current_uid] + successors) != current_uid:
            while successors[0] < current_uid:
                successors.append(successors.pop(0))

        # remove all linked successors not within the closest <num_successors> linked nodes
        # remove all unlinked successors not within the closest <num_successors> nodes
        num_linked_successors = 0

        if len(successors)%2 == 0:
            loop_counter = len(successors)/2
        elif len(successors)== 1:
            loop_counter = 0
        else:
            loop_counter = len(successors) / 2 +1

        i=0
        while i < (loop_counter):
            if self.linked(successors[i], interface_name):
                num_linked_successors += 1
                if num_linked_successors > (2 * int(self.CMConfig["num_successors"])):
                    self.remove_link("successor", successors[i], interface_name)
            if self.linked(successors[-(i+1)], interface_name):
                    num_linked_successors += 1
                    if num_linked_successors > (2 * int(self.CMConfig["num_successors"])):
                        self.remove_link("successor", successors[-(i+1)], interface_name)
            i+=1


    def processCBT(self, cbt):
        if cbt.action == "clean_connection":
           self.clean_connections(cbt.data.get("interface_name"))

        elif cbt.action == "request_connection":
            msg = cbt.data
            peer_uid = msg.pop("peer_uid")
            interface_name = msg.pop("interface_name")
            ttl = msg.pop("ttl")
            self.request_connection(msg["con_type"],peer_uid,interface_name,msg,ttl)

        elif cbt.action == "respond_connection":
            msg = cbt.data
            interface_name = msg.pop("interface_name")
            uid = msg.pop("uid")
            ttl = msg.pop("ttl")
            con_type = msg.get("con_type")
            self.respond_connection(con_type,msg,interface_name,uid,ttl)

        elif cbt.action == "remove_connection":
            self.remove_connection(cbt.data.get("uid"),cbt.data.get("interface_name"))

        elif cbt.action == "create_connection":
            msg = cbt.data
            interface_name = cbt.data.get("interface_name")
            msg["data"] = json.loads(msg["data"])
            uid = msg["uid"]
            con_type = msg["data"]["con_type"]
            peer_mac = msg["data"]["mac"]

            if uid not in self.connection_details[interface_name][con_type]:
                self.connection_details[interface_name][con_type][uid] ={}
            self.connection_details[interface_name][con_type][uid]["ttl"] = time.time() + self.CMConfig[
                    "InitialLinkTTL"]
            self.connection_details[interface_name][con_type][uid]["mac"] = peer_mac
            log = "recvd con_ack ({0}): {1}".format(con_type, uid)
            self.registerCBT('Logger', 'debug', log)
            self.registerCBT('TincanSender', 'DO_CREATE_LINK', msg)
            self.registerCBT("BaseTopologyManager", "UpdateConnectionDetails",
                             {"uid": uid, "interface_name": interface_name, "msg_type": "add_peer", "mac":peer_mac})

        elif cbt.action == "update_link_attr":
            self.update_connection(cbt.data)

        elif cbt.action == "remove_successor":
            msg = cbt.data
            self.remove_successors(msg["interface_name"],msg["uid"])

        elif cbt.action  == "find_chord":
            msg = cbt.data
            self.find_chords(msg["interface_name"], msg["uid"])


        elif cbt.action == "get_visualizer_data":
            successors,chords,on_demands = [],[],[]
            for interface_name in self.connection_details.keys():
                for successor in list(self.connection_details[interface_name]["successor"].keys()):
                    if "status" in self.connection_details[interface_name]["successor"][successor].keys():
                        if self.connection_details[interface_name]["successor"][successor]["status"] == "online":
                            successors.append(successor)

                for chord in list(self.connection_details[interface_name]["chord"].keys()):
                    if "status" in self.connection_details[interface_name]["chord"][chord].keys():
                        if self.connection_details[interface_name]["chord"][chord]["status"] == "online":
                            chords.append(chord)

                for ondemand in list(self.connection_details[interface_name]["on_demand"].keys()):
                    if "status" in self.connection_details[interface_name]["on_demand"][ondemand].keys():
                        if self.connection_details[interface_name]["on_demand"][ondemand]["status"] == "online":
                            on_demands.append(ondemand)
                msg = {
                    "interface_name": interface_name,
                    "links": {
                        "successor" : successors,
                        "chord"     : chords,
                        "on_demand" : on_demands
                    }
                }
                self.registerCBT("OverlayVisualizer","link_details",msg)
        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def timer_method(self):
        pass

    def terminate(self):
        pass
