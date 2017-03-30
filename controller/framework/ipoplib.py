#!/usr/bin/env python
import sys
py_ver = sys.version_info[0]

ENDPT = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "CreateCtrlRespLink",
            "AddressFamily": "af_inetv6",
            "Protocol": "proto_datagram",
            "IP": "::1",
            "Port": 5801
            }
    }
}
LOGCFG = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 0,
        "ControlType": "TincanRequest",
        "Request": {
          "Command" : "ConfigureLogging",
          "Level" : "WARNING",
          "Device" : "FILE",
          "Directory" : "./logs/",
          "Filename" : "tincan_log",
          "MaxArchives" : 10,
          "MaxFileSize" : 1048576,
          "ConsoleLevel" : "ERROR"
        }
    }
}
LSTATE = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 0,
        "ControlType": "TincanRequest",
        "Request": {
          "Command": "QueryNodeInfo",
          "InterfaceName": "ipop_tap0",
          "MAC" : ""
        }
    }
}
ECHO = {
  "IPOP": {
      "ProtocolVersion": 4,
      "TransactionId" : 0,
      "ControlType": "TincanRequest",
      "Request": {
        "Command": "Echo",
        "InterfaceName": "ipop_tap0",
        "Message" : "echo message"
      }
  }
}
VNET = {
    "IPOP": {
        "ProtocolVersion": 4,
        "ControlType": "TincanRequest",
        "TransactionId" : 0,
        "Request": {
          "Command" : "CreateVnet",
          "InterfaceName" : "ipop_tap0",
          "Description" : "My Devices",
          "LocalVirtIP4" : "",
          "LocalPrefix4" : "",
          "LocalPrefix6" : "",
          "MTU4" : "",
          "MTU6" : "",
          "LocalUID" : "",
          "LocalVirtIP6" : "",
          "StunAddress": "",
          "TurnAddress" : "",
          "TurnUser" : "",
          "TurnPass" : "",
          "L2TunnelEnabled" : True,
          "IPMappingEnabled" : False
        }
    }
}
LCAS = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 100005,
        "ControlType": "TincanRequest",
        "Request": {
          "Command" : "CreateLinkListener",
          "InterfaceName" : "ipop_tap0",
          "EncryptionEnabled" : True,
          "PeerInfo" : {
            "VIP4" : "",
            "UID" : "",
            "MAC" : "",
            "Fingerprint" : ""
          }
        }
    }
}
CONCT = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 100006,
        "ControlType": "TincanRequest",
        "Request": {
          "Command" : "ConnectToPeer",
          "InterfaceName" : "ipop_tap0",
          "EncryptionEnabled" : True,
          "PeerInfo" : {
            "VIP4" : "",
            "UID" : "",
            "MAC" : "",
            "Fingerprint" : ""
          }
        }
    }
}
IGNORE = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 100007,
        "ControlType": "TincanRequest",
        "Request": {
          "Command" : "SetIgnoredNetInterfaces",
          "InterfaceName" : "ipop_tap0",
          "IgnoredNetInterfaces" : []
        }
    }    
}
ICC = {
    "IPOP": {
      "ProtocolVersion": 4,
      "Tag" : 100020,
      "ControlType": "TincanRequest",
      "Request" : {
        "Command" : "ICC",
        "RecipientMac": "",
        "InterfaceName" : "ipop_tap0",
        "Recipient" : "peer_uid",
        "Data" : "encoded string"
      }
     }
  }
PACKET = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 100030,
        "ControlType": "TincanRequest",
        "Request": {
            "Command" : "UpdateRoutes",
            "InterfaceName" : "ipop_tap0",
            "Data" : "encoded_string"
        }
     }
  }
INSERT_TAP_PACKET = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 100030,
        "ControlType": "TincanRequest",
        "Request": {
            "Command" : "InjectFrame",
            "InterfaceName" : "ipop_tap0",
            "Data" : "encoded_string"
        }
     }
  }
REMOVE = {
    "IPOP": {
        "ProtocolVersion" : 4,
        "TransactionId" : 100015,
        "ControlType": "TincanRequest",
        "Request": {
            "Command" : "RemovePeer",
            "InterfaceName" : "ipop_tap0",
            "MAC" : ""
        }
     }
  }
RESP = {
    "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 100000,
        "ControlType": "TincanResponse",
        "Request": {
        },
        "Response" : {
            "Success" : True,
            "Message" : "description"
        }
    }
}

ADD_ROUTING = {
        "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 102001,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "UpdateMap",
            "InterfaceName" : "ipop_tap0",
            "Routes" : []
            }
        }
}

DELETE_ROUTING = {
        "IPOP": {
        "ProtocolVersion": 4,
        "TransactionId" : 102001,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "RemoveRoutes",
            "InterfaceName" : "ipop_tap0",
            "Routes" : []
            }
    }
}

def ip6_a2b(str_ip6):
    if py_ver == 3:
        return b''.join(int(x, 16).to_bytes(2, byteorder='big') for x in str_ip6.split(':'))
    else:
        return ''.join(x.decode("hex") for x in str_ip6.split(':'))

def ip6_b2a(bin_ip6):
    if py_ver == 3:
        return ''.join("%04x" % int.from_bytes(bin_ip6[i:i+2], byteorder='big') + ':'\
                for i in range(0, 16, 2))[:-1]
    else:
        return ''.join(bin_ip6[x:x+2].encode("hex") + ":" for x in range(0, 16, 2))[:-1]

def ip4_a2b(str_ip4):
    if py_ver == 3:
        return b''.join(int(x, 10).to_bytes(1, byteorder='big') for x in str_ip4.split('.'))
    else:
        return ''.join(chr(int(x)) for x in str_ip4.split('.'))

def ip4_b2a(bin_ip4):
    if py_ver == 3:
        return ''.join(str(int.from_bytes(bin_ip4[i:i+1], byteorder='big')) + '.'\
                for i in range(0, 4, 1))[:-1]
    else:
        return ''.join(str(ord(bin_ip4[x])) + "." for x in range(0, 4))[:-1]

def mac_a2b(str_mac):
    if py_ver == 3:
        return b''.join(int(x, 16).to_bytes(1, byteorder='big') for x in str_mac.split(':'))
    else:
        return ''.join(x.decode("hex") for x in str_mac.split(':'))

def mac_b2a(bin_mac):
    if py_ver == 3:
        return ''.join("%02x" % int.from_bytes(bin_mac[i:i+1], byteorder='big') + ':'\
                for i in range(0, 6, 1))[:-1]
    else:
        return ''.join(bin_mac[x].encode("hex") + ":" for x in range(0, 6))[:-1]

def uid_a2b(str_uid):
    if py_ver == 3:
        return int(str_uid, 16).to_bytes(20, byteorder='big')
    else:
        return str_uid.decode("hex")

def uid_b2a(bin_uid):
    if py_ver == 3:
        return "%40x" % int.from_bytes(bin_uid, byteorder='big')
    else:
        return bin_uid.encode("hex")

def hexstr2b(hexstr):
    if py_ver == 3:
        return b''.join(int(hexstr[i:i+2], 16).to_bytes(1, byteorder='big') for i in range(0, len(hexstr), 2))
    else:
        return hexstr.decode('hex')

def b2hexstr(binary):
    if py_ver == 3:
        return ''.join("%02x" % int.from_bytes(binary[i:i+1], byteorder='big') for i in range(0, len(binary), 1))
    else:
        return binary.encode('hex')

def gen_ip4(uid, peer_map, ip4):
    try:
        return peer_map[uid]
    except KeyError:
        pass

    ips = set(peer_map.values())
    prefix, _ = ip4.rsplit(".", 1)
    # we allocate *.101 - *254 ensuring a 3-digit suffix and avoiding the
    # broadcast address; *.100 is the IPv4 address of this node
    for i in range(101, 255):
        peer_map[uid] = "%s.%s" % (prefix, i)
        if peer_map[uid] not in ips:
            return peer_map[uid]
    del peer_map[uid]
    raise OverflowError("too many peers, out of IPv4 addresses")

