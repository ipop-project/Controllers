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



class BridgeABC():
    __metaclass__ = ABCMeta

    bridge_type = NotImplemented
    iptool = spawn.find_executable("ip")

    def __init__(self, name, ip_addr, prefix_len, mtu):
        self.name = name
        self.ip_addr = ip_addr
        self.prefix_len = prefix_len
        self.mtu = mtu
        self.ports = set()

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
    def brctl(self,):
        pass

    def __repr__(self):
        """ Return a representaion of a bridge object. """
        return "%s %s" % (self.bridge_type, self.name)

    def __str__(self):
        """ Return a string of the bridge name. """
        return self.__repr__()


class OvsBridge(BridgeABC):
    brctl = spawn.find_executable("ovs-vsctl")
    bridge_type = "OVS"

    def __init__(self, name, ip_addr, prefix_len, mtu, stp_enable, sdn_ctrl_cfg=None):
        """ Initialize an OpenvSwitch bridge object. """
        super(OvsBridge, self).__init__(name, ip_addr, prefix_len, mtu)
        if OvsBridge.brctl is None or OvsBridge.iptool is None:
            raise RuntimeError("openvswitch-switch was not found" if not OvsBridge.brctl else
                               "iproute2 was not found")
        ipoplib.runshell_su([OvsBridge.brctl,
                             "--may-exist", "add-br", self.name])

        try:
            p = ipoplib.runshell_su([OvsBridge.brctl, "set", "int",
                                     self.name,
                                     "mtu_request=" + str(self.mtu)])
        except RuntimeError as e:
            pass
        # self.register_cbt(
        # "Logger", "LOG_WARN", "Following error occurred while"
        # " setting MTU for OVS bridge: " + e.message
        # + ". Proceeding with OVS-specified default"
        # " value for the bridge...")

        net = "{0}/{1}".format(ip_addr, prefix_len)

        p = ipoplib.runshell_su([OvsBridge.iptool, "addr", "show", self.name])
        ip_addr_info = p.stdout.decode()
        if net not in ip_addr_info:
            ipoplib.runshell_su([OvsBridge.iptool, "addr", "add", net, "dev", self.name])

        self.stp(stp_enable)
        ipoplib.runshell_su([OvsBridge.iptool, "link", "set", "dev", self.name, "up"])

        if sdn_ctrl_cfg:
            self.add_sdn_ctrl(sdn_ctrl_cfg)

    def add_sdn_ctrl(self, sdn_ctrl_cfg):
        if sdn_ctrl_cfg["ConnectionType"] == "tcp":
            ctrl_conn_str = ":".join([sdn_ctrl_cfg["ConnectionType"],
                                      sdn_ctrl_cfg["HostName"],
                                      sdn_ctrl_cfg["Port"]])

            ipoplib.runshell_su([OvsBridge.brctl,
                                 "set-controller",
                                 self.name,
                                 ctrl_conn_str])

    def del_sdn_ctrl(self):
        ipoplib.runshell_su([OvsBridge.brctl, "del-controller", self.name])

    def del_br(self):
        self.del_sdn_ctrl()

        ipoplib.runshell_su([OvsBridge.brctl,
                             "--if-exists", "del-br", self.name])

    def add_port(self, port_name):
        ipoplib.runshell_su([OvsBridge.iptool, "link", "set", "dev", port_name, "mtu",
                             str(self.mtu)])
        ipoplib.runshell_su([OvsBridge.brctl,
                             "--may-exist", "add-port", self.name, port_name])
        self.ports.add(port_name)

    def del_port(self, port_name):
        ipoplib.runshell_su([OvsBridge.brctl,
                             "--if-exists", "del-port", self.name, port_name])
        if port_name in self.ports:
            self.ports.remove(port_name)

    def stp(self, enable):
        if enable:
            ipoplib.runshell_su([OvsBridge.brctl,
                                 "set", "bridge", self.name,
                                 "stp_enable=true"])
        else:
            ipoplib.runshell_su([OvsBridge.brctl,
                                 "set", "bridge", self.name,
                                 "stp_enable=false"])


class LinuxBridge(BridgeABC):
    brctl = spawn.find_executable("brctl")
    bridge_type = "LXBR"

    def __init__(self, name, ip_addr, prefix_len, mtu, stp_enable):
        """ Initialize a Linux bridge object. """
        super(LinuxBridge, self).__init__(name, ip_addr, prefix_len, mtu)
        if LinuxBridge.brctl is None or LinuxBridge.iptool is None:
            raise RuntimeError("bridge-utils was not found" if not LinuxBridge.brctl else
                               "iproute2 was not found")
        p = ipoplib.runshell_su([LinuxBridge.brctl, "show"])
        wlist = map(str.split, p.stdout.decode("utf-8").splitlines()[1:])
        brwlist = filter(lambda x: len(x) != 1, wlist)
        brlist = map(lambda x: x[0], brwlist)
        for br in brlist:
            if br == name:
                return

        p = ipoplib.runshell_su([LinuxBridge.brctl, "addbr", self.name])
        net = "{0}/{1}".format(ip_addr, prefix_len)
        ipoplib.runshell_su([LinuxBridge.iptool, "addr", "add", net, "dev", name])
        self.stp(stp_enable)
        ipoplib.runshell_su([LinuxBridge.iptool, "link", "set", "dev", name, "up"])

    def del_br(self):
        # Set the device down and delete the bridge
        ipoplib.runshell_su([LinuxBridge.iptool, "link", "set", "dev", self.name, "down"])
        ipoplib.runshell_su([LinuxBridge.brctl, "delbr", self.name])

    def add_port(self, port_name):
        ipoplib.runshell_su([LinuxBridge.iptool, "link", "set", port_name, "mtu", str(self.mtu)])
        ipoplib.runshell_su([LinuxBridge.brctl, "addif", self.name, port_name])
        self.ports.add(port_name)

    def del_port(self, port_name):
        p = ipoplib.runshell_su([LinuxBridge.brctl, "show", self.name])
        wlist = map(str.split, p.stdout.decode("utf-8").splitlines()[1:])
        port_lines = filter(lambda x: len(x) == 4, wlist)
        ports = map(lambda x: x[-1], port_lines)
        for port in ports:
            if port == port_name:
                ipoplib.runshell_su([LinuxBridge.brctl, "delif", self.name, port_name])
                if port_name in self.ports:
                    self.ports.remove(port_name)

    def stp(self, val=True):
        """ Turn STP protocol on/off. """
        if val:
            state = "on"
        else:
            state = "off"
        ipoplib.runshell_su([LinuxBridge.brctl, "stp", self.name, state])

    def set_bridge_prio(self, prio):
        """ Set bridge priority value. """
        ipoplib.runshell_su([LinuxBridge.brctl,
                             "setbridgeprio", self.name, str(prio)])

    def set_path_cost(self, port, cost):
        """ Set port path cost value for STP protocol. """
        ipoplib.runshell_su([LinuxBridge.brctl,
                             "setpathcost", self.name, port, str(cost)])

    def set_port_prio(self, port, prio):
        """ Set port priority value. """
        ipoplib.runshell_su([LinuxBridge.brctl,
                             "setportprio", self.name, port, str(prio)])


class BridgeController(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(BridgeController, self).__init__(cfx_handle, module_config,
                                               module_name)
        self._overlays = dict()
        self._lock = threading.Lock()

    def initialize(self):
        ign_br_names = dict()

        for olid in self._cm_config["Overlays"]:
            br_cfg = self._cm_config["Overlays"][olid]

            if self._cm_config["Overlays"][olid]["Type"] == \
                    LinuxBridge.bridge_type:
                self._overlays[olid] = LinuxBridge(br_cfg["BridgeName"],
                                                   br_cfg["IP4"],
                                                   br_cfg["PrefixLen"],
                                                   br_cfg.get("MTU", 1410),
                                                   br_cfg.get("STP", True))

            elif self._cm_config["Overlays"][olid]["Type"] == \
                    OvsBridge.bridge_type:
                self._overlays[olid] = OvsBridge(br_cfg["BridgeName"],
                                                 br_cfg["IP4"],
                                                 br_cfg["PrefixLen"],
                                                 br_cfg.get("MTU", 1410),
                                                 br_cfg.get("STP", True),
                                                 sdn_ctrl_cfg=br_cfg.get("SDNController",
                                                                         dict()))
                ign_br_names[olid] = br_cfg["BridgeName"]

            self.register_cbt("LinkManager",
                              "LNK_ADD_IGN_INF", ign_br_names)

        self._cfx_handle.start_subscription("LinkManager", "LNK_TUNNEL_EVENTS")
        self.register_cbt("Logger", "LOG_INFO", "Module Loaded")

    def req_handler_add_port(self, cbt):
        pass

    def req_handler_del_port(self, cbt):
        pass

    def req_handler_manage_bridge(self, cbt):
        try:
            olid = cbt.request.params["OverlayId"]
            br = self._overlays[olid]
            if cbt.request.params["UpdateType"] == "CONNECTED":
                port_name = cbt.request.params["TapName"]
                br.add_port(port_name)
                self.register_cbt(
                    "Logger", "LOG_INFO", "Port {0} added to bridge {1}"
                    .format(port_name, str(br)))
            elif cbt.request.params["UpdateType"] == "REMOVED":
                if br.bridge_type == OvsBridge.bridge_type:
                    port_name = cbt.request.params.get("TapName")
                    if port_name:
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
            if cbt.request.action == "LNK_TUNNEL_EVENTS":
                self.req_handler_manage_bridge(cbt)
            else:
                self.req_handler_default(cbt)
        elif cbt.op_type == "Response":
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
        try:
            for olid in self._overlays:
                br = self._overlays[olid]

                if self._cm_config["Overlays"][olid].get("AutoDelete", False):
                    br.del_br()
                else:
                    if br.bridge_type == OvsBridge.bridge_type:
                        for port in br.ports:
                            br.del_port(port)
        except RuntimeError as err:
            self.register_cbt("Logger", "LOG_WARNING", str(err))
