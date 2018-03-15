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

import sys
py_ver = sys.version_info[0]

CTL_CREATE_CTRL_LINK = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
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
CTL_CONFIGURE_LOGGING = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "ConfigureLogging",
            "Level": "DEBUG",
            "Device": "All",
            "Directory": "./logs/",
            "Filename": "tincan_log",
            "MaxArchives": 10,
            "MaxFileSize": 1048576,
            "ConsoleLevel": "DEBUG"
        }
    }
}
CTL_QUERY_OVERLAY_INFO = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "QueryOverlayInfo",
            "OverlayId": ""
        }
    }
}
CTL_CREATE_OVERLAY = {
    "IPOP": {
        "ProtocolVersion": 5,
        "ControlType": "TincanRequest",
        "TransactionId": 0,
        "Request": {
            "Command": "CreateOverlay",
            "TapName": "",
            "IP4": "",
            "IP4PrefixLen": "",
            "MTU4": "",
            "StunAddress": "",
            "TurnAddress": "",
            "TurnUser": "",
            "TurnPass": "",
        }
    }
}
CTL_CREATE_LINK = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "CreateLink",
            "OverlayId": "",
            "LinkId": "",
            "PeerInfo": {
                "VIP4": "",
                "UID": "",
                "MAC": "",
                "FPR": ""
            }
        }
    }
}
CTL_SET_IGNORED_NET_INTERFACES = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "SetIgnoredNetInterfaces",
            "OverlayId": "",
            "IgnoredNetInterfaces": []
        }
    }
}
CTL_SEND_ICC = {
    "IPOP": {
        "ProtocolVersion": 5,
        "Tag": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "SendIcc",
            "LinkId": "",
            "OverlayId": "",
            "Data": "encoded string"
        }
    }
}
INSERT_TAP_PACKET = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "InjectFrame",
            "OverlayId": "",
            "Data": "encoded_string"
        }
    }
}
CTL_REMOVE_OVERLAY = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "RemoveOverlay",
            "OverlayId": ""
        }
    }
}
CTL_REMOVE_LINK = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "RemoveLink",
            "OverlayId": "",
            "LinkId": ""
        }
    }
}
RESP = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanResponse",
        "Request": {
        },
        "Response": {
            "Success": True,
            "Message": "description"
        }
    }
}
ADD_FORWARDING_RULE = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "UpdateMap",
            "OverlayId": "",
            "Routes": []
        }
    }
}
DELETE_FORWARDING_RULE = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "RemoveRoutes",
            "OverlayId": "",
            "Routes": []
        }
    }
}
CTL_QUERY_LINK_STATS = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "QueryLinkStats",
            "OverlayIds" : []
        }
    }
}
CTL_QUERY_CAS = {
    "IPOP": {
        "ProtocolVersion": 5,
        "TransactionId": 0,
        "ControlType": "TincanRequest",
        "Request": {
            "Command": "QueryCandidateAddressSet",
            "OverlayId": "",
            "LinkId": ""
        }
    }
}


def ip4_a2hex(ipstr):
    return "".join(hex(int(x, 10))[2:] for x in ipstr.split("."))


def ip6_a2b(str_ip6):
    if py_ver == 3:
        return b"".join(int(x, 16).to_bytes(2, byteorder="big") for x in str_ip6.split(":"))
    else:
        return "".join(x.decode("hex") for x in str_ip6.split(":"))


def ip6_b2a(bin_ip6):
    if py_ver == 3:
        return "".join("%04x" % int.from_bytes(bin_ip6[i:i + 2], byteorder="big") + ":"
                       for i in range(0, 16, 2))[:-1]
    else:
        return "".join(bin_ip6[x:x + 2].encode("hex") + ":" for x in range(0, 16, 2))[:-1]


def ip4_a2b(str_ip4):
    if py_ver == 3:
        return b"".join(int(x, 10).to_bytes(1, byteorder="big") for x in str_ip4.split("."))
    else:
        return "".join(chr(int(x)) for x in str_ip4.split("."))


def ip4_b2a(bin_ip4):
    if py_ver == 3:
        return "".join(str(int.from_bytes(bin_ip4[i:i + 1], byteorder="big")) + "."
                       for i in range(0, 4, 1))[:-1]
    else:
        return "".join(str(ord(bin_ip4[x])) + "." for x in range(0, 4))[:-1]


def mac_a2b(str_mac):
    if py_ver == 3:
        return b"".join(int(x, 16).to_bytes(1, byteorder="big") for x in str_mac.split(":"))
    else:
        return "".join(x.decode("hex") for x in str_mac.split(":"))


def mac_b2a(bin_mac):
    if py_ver == 3:
        return "".join("%02x" % int.from_bytes(bin_mac[i:i + 1], byteorder="big") + ":"
                       for i in range(0, 6, 1))[:-1]
    else:
        return "".join(bin_mac[x].encode("hex") + ":" for x in range(0, 6))[:-1]


def uid_a2b(str_uid):
    if py_ver == 3:
        return int(str_uid, 16).to_bytes(20, byteorder="big")
    else:
        return str_uid.decode("hex")


def uid_b2a(bin_uid):
    if py_ver == 3:
        return "%40x" % int.from_bytes(bin_uid, byteorder="big")
    else:
        return bin_uid.encode("hex")


def hexstr2b(hexstr):
    if py_ver == 3:
        return b"".join(int(hexstr[i:i + 2], 16).to_bytes(1, byteorder="big") for i in range(0, len(hexstr), 2))
    else:
        return hexstr.decode("hex")


def b2hexstr(binary):
    if py_ver == 3:
        return "".join("%02x" % int.from_bytes(binary[i:i + 1], byteorder="big") for i in range(0, len(binary), 1))
    else:
        return binary.encode("hex")


def gen_ip4(uid, peer_map, ip4):
    try:
        return peer_map[uid]
    except KeyError as error:
        print("Exception Caught in ipoplib: {0}".format(str(error)))
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


# Function to add 2 hex data and return the result
def addhex(data1, data2):
    bindata1 = list(("{0:0" + str((len(data1)) * 4) + "b}").format(int(data1, 16)))
    bindata2 = list(("{0:0" + str((len(data2)) * 4) + "b}").format(int(data2, 16)))
    if len(bindata1) == len(bindata2):
        j = len(bindata1) - 1
    elif len(bindata1) > len(bindata2):
        j = len(bindata1) - 1
        bindata2 = [0] * (len(bindata1) - len(bindata2)) + bindata2
    else:
        j = len(bindata2) - 1
        bindata1 = [0] * (len(bindata2) - len(bindata1)) + bindata1

    carry = 0
    result = []
    while j > 0:
        summer = carry + int(bindata1[j]) + int(bindata2[j])
        result.insert(0, str(summer % 2))
        carry = summer / 2
        j -= 1
    return hex(int("".join(result), 2))


# Function to calculate checksum and return it in HEX format
def getchecksum(hexstr):
    result = "0000"
    for i in range(0, len(hexstr), 4):
        result = addhex(result, hexstr[i:i + 4])
    if len(result) != 4:
        result = addhex(result[0:len(result) - 4], result[len(result) - 4:])
    return hex(65535 ^ int(result, 16))
