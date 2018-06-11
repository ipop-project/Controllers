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


from abc import ABCMeta, abstractmethod
import threading
from distutils import spawn
import controller.framework.ipoplib as ipoplib
from controller.framework.ControllerModule import ControllerModule

IPEXE = spawn.find_executable("ip")


class BridgeABC():
    __metaclass__ = ABCMeta

    def __init__(self, name, ip_addr, prefix_len, mtu, *args, **kwargs):
        self.name = name
        self.ip_addr = ip_addr
        self.prefix_len = prefix_len
        self.mtu = mtu

    @abstractmethod
    def add_port(self, port_name):
        pass

    @abstractmethod
    def del_port(self, port_name):
        pass

    @abstractmethod
    def del_br(self):
        pass

    @property
    @abstractmethod
    def brctlexe(self,):
        pass


class OvsBridge(BridgeABC):
    brctlexe = spawn.find_executable("ovs-vsctl")

    def __init__(self, name, ip_addr, prefix_len, mtu, stp_enable,
                 sdn_ctrl_cfg=dict()):
        """ Initialize a bridge object. """
        super(OvsBridge, self).__init__(name, ip_addr, prefix_len, mtu)
        ipoplib.runshell_su([OvsBridge.brctlexe,
                             "--may-exist", "add-br", self.name])

        p = ipoplib.runshell_su([IPEXE, "link", "set", "dev", self.name, "mtu", str(mtu)])

        net = "{0}/{1}".format(ip_addr, prefix_len)

        p = ipoplib.runshell_su([IPEXE, "addr", "show", self.name])
        ip_addr_info = p.stdout.decode()
        if net not in ip_addr_info:
            ipoplib.runshell_su([IPEXE, "addr", "add", net, "dev", self.name])

        self.stp(stp_enable)
        ipoplib.runshell_su([IPEXE, "link", "set", "dev", self.name, "up"])

        if sdn_ctrl_cfg:
            self.add_sdn_ctrl(sdn_ctrl_cfg)

    def add_sdn_ctrl(self, sdn_ctrl_cfg):
        if sdn_ctrl_cfg["ConnectionType"] == "tcp":
            ctrl_conn_str = ":".join([sdn_ctrl_cfg["ConnectionType"],
                                      sdn_ctrl_cfg["HostName"],
                                      sdn_ctrl_cfg["Port"]])

            ipoplib.runshell_su([OvsBridge.brctlexe,
                                 "set-controller",
                                 self.name,
                                 ctrl_conn_str])

    def del_sdn_ctrl(self):
        ipoplib.runshell_su([OvsBridge.brctlexe, "del-controller", self.name])

    def del_br(self):
        self.del_sdn_ctrl()

        ipoplib.runshell_su([OvsBridge.brctlexe,
                             "--if-exists", "del-br", self.name])

    def add_port(self, port_name):
        ipoplib.runshell_su([OvsBridge.brctlexe,
                             "--may-exist", "add-port", self.name, port_name])

    def del_port(self, port_name):
        ipoplib.runshell_su([OvsBridge.brctlexe,
                             "--if-exists", "del-port", self.name, port_name])

    def stp(self, enable):
        if enable:
            ipoplib.runshell_su([OvsBridge.brctlexe,
                                 "set", "bridge", self.name,
                                 "stp_enable=true"])
        else:
            ipoplib.runshell_su([OvsBridge.brctlexe,
                                 "set", "bridge", self.name,
                                 "stp_enable=false"])


class LinuxBridge(BridgeABC):
    brctlexe = spawn.find_executable("brctl")

    def __init__(self, name, ip_addr, prefix_len, mtu, stp_enable,
                 *args, **kwargs):
        """ Initialize a bridge object. """

        super(LinuxBridge, self).__init__(name, ip_addr, prefix_len, mtu)

        p = ipoplib.runshell_su([LinuxBridge.brctlexe, 'show'])
        wlist = map(str.split, p.stdout.decode("utf-8").splitlines()[1:])
        brwlist = filter(lambda x: len(x) != 1, wlist)
        brlist = map(lambda x: x[0], brwlist)
        for br in brlist:
            print(br)
            if br == name:
                print("deleting {}".format(br))
                self.del_br()

        p = ipoplib.runshell_su([LinuxBridge.brctlexe, "addbr", self.name])
        p = ipoplib.runshell_su([IPEXE, "link", "set", "dev", self.name, "mtu", str(mtu)])
        net = "{0}/{1}".format(ip_addr, prefix_len)
        ipoplib.runshell_su([IPEXE, "addr", "add", net, "dev", name])
        self.stp(stp_enable)
        ipoplib.runshell_su([IPEXE, "link", "set", "dev", name, "up"])

    def __str__(self):
        """ Return a string of the bridge name. """
        return self.name

    def __repr__(self):
        """ Return a representaion of a bridge object. """
        return "<Bridge: %s>" % self.name

    def del_br(self):
        """ Set the device down and delete the bridge. """
        ipoplib.runshell_su([IPEXE, "link", "set", "dev", self.name, "down"])
        ipoplib.runshell_su([LinuxBridge.brctlexe, "delbr", self.name])

    def add_port(self, port):
        ipoplib.runshell_su([LinuxBridge.brctlexe, "addif", self.name, port])

    def del_port(self, port):
        ipoplib.runshell_su([LinuxBridge.brctlexe, "delif", self.name, port])

    def stp(self, val=True):
        """ Turn STP protocol on/off. """

        if val:
            state = "on"
        else:
            state = "off"
        ipoplib.runshell_su([LinuxBridge.brctlexe, "stp", self.name, state])

    def set_bridge_prio(self, prio):
        """ Set bridge priority value. """
        ipoplib.runshell_su([LinuxBridge.brctlexe,
                             "setbridgeprio", self.name, str(prio)])

    def set_path_cost(self, port, cost):
        """ Set port path cost value for STP protocol. """
        ipoplib.runshell_su([LinuxBridge.brctlexe,
                             "setpathcost", self.name, port, str(cost)])

    def set_port_prio(self, port, prio):
        """ Set port priority value. """
        ipoplib.runshell_su([LinuxBridge.brctlexe,
                             "setportprio", self.name, port, str(prio)])


class BridgeController(ControllerModule):
    def __init__(self, cfx_handle, module_config,
                 module_name, *args, **kwargs):
        super(BridgeController, self).__init__(cfx_handle, module_config,
                                               module_name)
        self._overlays = dict()
        self._lock = threading.Lock()

    def initialize(self):
        ign_br_names = dict()

        for olid in self._cm_config["Overlays"]:
            br_cfg = self._cm_config["Overlays"][olid]

            if self._cm_config["Overlays"][olid]["Type"] == "LXBR":
                self._overlays[olid] = LinuxBridge(br_cfg["BridgeName"],
                                                   br_cfg["IP4"],
                                                   br_cfg["PrefixLen"],
                                                   br_cfg.get("MTU", 1500),
                                                   br_cfg.get("STP", False))

            elif self._cm_config["Overlays"][olid]["Type"] == "OVS":
                self._overlays[olid] = OvsBridge(br_cfg["BridgeName"],
                                                 br_cfg["IP4"],
                                                 br_cfg["PrefixLen"],
                                                 br_cfg.get("MTU", 1500),
                                                 br_cfg.get("STP", False),
                                                 sdn_ctrl_cfg=br_cfg.get("SDNController",
                                                 dict()))
                ign_br_names[olid] = br_cfg["BridgeName"]

            self.register_cbt("LinkManager",
                              "LNK_ADD_IGN_INF", ign_br_names)

        self._cfx_handle.start_subscription("LinkManager", "LNK_DATA_UPDATES")
        self.register_cbt("Logger", "LOG_INFO", "Module Loaded")

    def req_handler_add_port(self, cbt):
        pass

    def req_handler_del_port(self, cbt):
        pass

    def req_handler_manage_bridge(self, cbt):
        try:
            olid = cbt.request.params["OverlayId"]
            port_name = cbt.request.params["TapName"]
            br = self._overlays[olid]
            if cbt.request.params["UpdateType"] == "ADDED":
                br.add_port(port_name)
                self.register_cbt(
                    "Logger", "LOG_INFO", "Port {0} added to bridge {1}"
                    .format(port_name, str(br)))
            elif cbt.request.params["UpdateType"] == "REMOVED":
                br.del_port(port_name)
                self.register_cbt(
                    "Logger", "LOG_INFO", "Port {0} removed from bridge {1}"
                    .format(port_name, str(br)))
        except RuntimeError as err:
            self.register_cbt("Logger", "LOG_WARNING", str(err))
        cbt.set_response(None, True)
        self.complete_cbt(cbt)

    def resp_handler_(self, cbt):
        pass

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            if cbt.request.action == "BRG_ADD_PORT":
                self.req_handler_add_port(cbt)
            if cbt.request.action == "BRG_DEL_PORT":
                self.req_handler_del_port(cbt)
            if cbt.request.action == "LNK_DATA_UPDATES":
                self.req_handler_manage_bridge(cbt)
            else:
                self.req_handler_default(cbt)
        elif cbt.op_type == "Response":
            if cbt.request.action == "TOP_QUERY_PEER_IDS":
                self.resp_handler_query_peers(cbt)
            else:
                parent_cbt = self.get_parent_cbt(cbt)
                cbt_data = cbt.response.data
                cbt_status = cbt.response.status
                self.free_cbt(cbt)
                if (parent_cbt is not None and parent_cbt.child_count == 1):
                    parent_cbt.set_response(cbt_data, cbt_status)
                    self.complete_cbt(parent_cbt)

    def timer_method(self):
        pass

    def terminate(self):
        for olid in self._overlays:
            self._overlays[olid].del_br()
