#!/usr/bin/env python

ipop_ver = "\x02"
tincan_control = "\x01"
tincan_packet = "\x02"
icc_control = "\x03"
icc_packet = "\x04"
icc_mac_control = "\x00\x69\x70\x6f\x70\x03"
icc_mac_packet = "\x00\x69\x70\x6f\x70\x04"
icc_ethernet_padding = "\x00\x00\x00\x00\x00\x00\x00\x00"

def ip6_a2b(str_ip6):
    return "".join(x.decode("hex") for x in str_ip6.split(':'))

def ip6_b2a(bin_ip6):
    return "".join(bin_ip6[x:x+2].encode("hex") + ":" for x in range(0, 14, 2))\
           + bin_ip6[14].encode("hex") + bin_ip6[15].encode("hex")


def ip4_a2b(str_ip4):
    return "".join(chr(int(x)) for x in str_ip4.split('.'))

def ip4_b2a(bin_ip4):
    return "".join(str(ord(bin_ip4[x])) + "." for x in range(0, 3)) \
           + str(ord(bin_ip4[3]))

def mac_a2b(str_mac):
    return "".join(x.decode("hex") for x in str_mac.split(':'))

def mac_b2a(bin_mac):
    return "".join(bin_mac[x].encode("hex") + ":" for x in range(0, 5)) +\
           bin_mac[5].encode("hex")

def uid_a2b(str_uid):
    return str_uid.decode("hex")

def gen_ip4(uid, peer_map, ip4):
    try:
        return peer_map[uid]
    except KeyError:
        pass

    ips = set(peer_map.itervalues())
    prefix, _ = ip4.rsplit(".", 1)
    # We allocate to *.101 - *.254. This ensures a 3-digit suffix and avoids
    # the broadcast address. *.100 is our IPv4 address.
    for i in range(101, 255):
        peer_map[uid] = "%s.%s" % (prefix, i)
        if peer_map[uid] not in ips:
            return peer_map[uid]
    del peer_map[uid]
    raise OverflowError("Too many peers, out of IPv4 addresses")

def make_remote_call(sock, dest_addr, dest_port, m_type, payload, **params):
    dest = (dest_addr, dest_port)
    if m_type == tincan_control:
        return sock.sendto(ipop_ver + m_type + json.dumps(params), dest)
    else:
        return sock.sendto(ipop_ver + m_type + payload, dest)
