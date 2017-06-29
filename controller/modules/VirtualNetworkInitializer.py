from controller.framework.ControllerModule import ControllerModule
import controller.framework.ipoplib as ipoplib
import controller.framework.fxlib as fxlib
import socket


class VirtualNetworkInitializer(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(VirtualNetworkInitializer, self).__init__(CFxHandle, paramDict, ModuleName)
        # Obtain the configuration details from the ipop-config file
        self.CONFIG = paramDict
        self.vnetdetails = paramDict["Vnets"]
        # Query VPN Type from CFX Module
        self.vpn_type = self.CFxHandle.queryParam('CFx', 'Model')

    def initialize(self):
        # Create connection to Tincan
        self.registerCBT("Logger", "info", "Creating Tincan inter-process link")
        ep = ipoplib.ENDPT
        if socket.has_ipv6 is False:
            ep["IPOP"]["Request"]["AddressFamily"] = "af_inet"
            ep["IPOP"]["Request"]["IP"] = self.CFxHandle.queryParam("TincanInterface", "localhost")
        self.registerCBT("TincanInterface", "DO_SEND_TINCAN_MSG", ep)

        # Set Tincan LogLevel
        log_level = self.CFxHandle.queryParam("Logger", "LogLevel")
        self.registerCBT("Logger", "info", "Setting Tincan log level to " + log_level)
        lgl = ipoplib.LOGCFG
        lgl["IPOP"]["Request"]["Level"] = log_level
        lgl["IPOP"]["Request"]["Device"] = self.CFxHandle.queryParam("Logger", "LogOption")
        lgl["IPOP"]["Request"]["Directory"] = self.CFxHandle.queryParam("Logger", "LogFilePath")
        lgl["IPOP"]["Request"]["Filename"] = self.CFxHandle.queryParam("Logger", "TincanLogFileName")
        lgl["IPOP"]["Request"]["MaxArchives"] = self.CFxHandle.queryParam("Logger", "BackupLogFileCount")
        lgl["IPOP"]["Request"]["MaxFileSize"] = self.CFxHandle.queryParam("Logger", "LogFileSize")
        lgl["IPOP"]["Request"]["ConsoleLevel"] = self.CFxHandle.queryParam("Logger", "ConsoleLevel")
        self.registerCBT("TincanInterface", "DO_SEND_TINCAN_MSG", lgl)

        # Iterate across the virtual network details given the config file
        for i in range(len(self.vnetdetails)):
            vn = ipoplib.VNET
            if self.vpn_type == 'GroupVPN':
                ip4 = self.CFxHandle.queryParam('DHCP', 'IP4')
                if ip4 is None:
                    ip4 = self.vnetdetails[i]["IP4"]
                self.vnetdetails[i]["uid"] = fxlib.gen_uid(ip4)
                self.vnetdetails[i]["ip6"] = fxlib.gen_ip6(self.vnetdetails[i]["uid"])
            elif self.vpn_type == 'SocialVPN':
                self.vnetdetails[i]["IP4"] = self.CONFIG['AddressMapper']["ip4"]
                self.vnetdetails[i]["uid"] = self.CFxHandle.queryParam('CFx', 'local_uid')
                self.vnetdetails[i]["ip6"] = fxlib.gen_ip6(self.vnetdetails[i]["uid"])

            # Create VNET Request Message
            self.registerCBT("Logger", "info", "Creating Vnet {0}".format(self.vnetdetails[i]["TapName"]))
            vn["IPOP"]["Request"]["LocalUID"] = self.vnetdetails[i]["uid"]
            vn["IPOP"]["Request"]["LocalVirtIP6"] = self.vnetdetails[i]["ip6"]
            vn["IPOP"]["Request"]["LocalVirtIP4"] = self.vnetdetails[i]["IP4"]

            vn["IPOP"]["Request"]["InterfaceName"] = self.vnetdetails[i]["TapName"]
            vn["IPOP"]["Request"]["Description"] = self.vnetdetails[i]["Description"]
            # Currently configured to take the first stun address
            vn["IPOP"]["Request"]["StunAddress"] = self.CONFIG["Stun"][0]
            vn["IPOP"]["Request"]["TurnAddress"] = self.CONFIG["Turn"][0]["Address"]
            vn["IPOP"]["Request"]["TurnUser"] = self.CONFIG["Turn"][0]["User"]
            vn["IPOP"]["Request"]["TurnPass"] = self.CONFIG["Turn"][0]["Password"]

            if "IP4Prefix" in self.vnetdetails[i]:
                vn["IPOP"]["Request"]["LocalPrefix4"] = self.vnetdetails[i]["IP4Prefix"]
            else:
                vn["IPOP"]["Request"]["LocalPrefix4"] = self.CONFIG["LocalPrefix4"]

            if "IP6Prefix" in self.vnetdetails[i]:
                vn["IPOP"]["Request"]["LocalPrefix6"] = self.vnetdetails[i]["IP6Prefix"]
            else:
                vn["IPOP"]["Request"]["LocalPrefix6"] = self.CONFIG["LocalPrefix6"]

            if "MTU4" in self.vnetdetails[i]:
                vn["IPOP"]["Request"]["MTU4"] = self.vnetdetails[i]["MTU4"]
            else:
                vn["IPOP"]["Request"]["MTU4"] = self.CONFIG["MTU4"]

            if "MTU6" in self.vnetdetails[i]:
                vn["IPOP"]["Request"]["MTU6"] = self.vnetdetails[i]["MTU6"]
            else:
                vn["IPOP"]["Request"]["MTU6"] = self.CONFIG["MTU6"]

            if self.vnetdetails[i]["L2TunnellingEnabled"] == 1:
                vn["IPOP"]["Request"]["L2TunnelEnabled"] = True
            else:
                vn["IPOP"]["Request"]["L2TunnelEnabled"] = False

            if "TrimEnabled" in self.vnetdetails[i]:
                vn["IPOP"]["Request"]["AutoTrimEnabled"] = self.vnetdetails[i]["TrimEnabled"]

            if self.vpn_type == "GroupVPN":
                vn["IPOP"]["Request"]["IPMappingEnabled"] = False
            else:
                vn["IPOP"]["Request"]["IPMappingEnabled"] = True

            # Send VNET creation request to Tincan
            self.registerCBT("TincanInterface", "DO_SEND_TINCAN_MSG", vn)

            self.registerCBT("Logger", "info", "Ignoring interfaces {0}".
                             format(self.vnetdetails[i]["IgnoredNetInterfaces"]))
            if "IgnoredNetInterfaces" in self.vnetdetails[i]:
                net_ignore_list = ipoplib.IGNORE
                net_ignore_list["IPOP"]["Request"]["IgnoredNetInterfaces"] = self.vnetdetails[i]["IgnoredNetInterfaces"]
                net_ignore_list["IPOP"]["Request"]["InterfaceName"] = self.vnetdetails[i]["TapName"]
                # Network Interfaces that have to be ignored while NAT traversal
                self.registerCBT("TincanInterface", "DO_SEND_TINCAN_MSG", net_ignore_list)

        self.registerCBT("Logger", "info", "Virtual Network Initialized")

    def processCBT(self, cbt):
        pass

    def timer_method(self):
        pass

    def terminate(self):
        pass
