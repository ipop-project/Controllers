#!/usr/bin/env python

from ipoplib import *
import sleekxmpp
import thread
import time
import struct
import logging
import shelve
from sleekxmpp.xmlstream.stanzabase import ElementBase, ET, JID
from sleekxmpp.xmlstream import register_stanza_plugin
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.stanza.message import Message
from sleekxmpp.plugins.base import base_plugin
#create and configure module logger
log = logging.getLogger('gvpn_controller')
log.setLevel(logging.DEBUG)
#set up a new custom message stanza
class Ipop_Msg(ElementBase):
    namespace = 'Conn_setup'
    name = 'Ipop'
    plugin_attrib = 'Ipop'
    interfaces = set(('setup','payload','uid'))
    subinterfaces = interfaces

#Handles interaction both with tincan and XMPP server
class Gvpn_UDPServer(UdpServer,sleekxmpp.ClientXMPP):
    def __init__(self,user,password,host,nick,room_passwd = None):
        sleekxmpp.ClientXMPP.__init__(self,user,password)
        #Initially the values are undefined, will either be provided
        #by admin via XMPP message or existing cached values will be
        #used 
        self.room = None
        self.ip4 = None
        self.uid = None 
        self.nick = nick
        #dict for keeping track of JID's and their 
        #connection status. ie. req_send
        #,resp_recvd etc
        self.xmpp_peers = {}
        #dict for keeping track of peer's xmpp_overlay status
        self.peer_xmpp_status = {}
        #uid-jid mapping,key = uid, val = jid
        self.uid_jid = {}
        #register new plugin stanza and the handler 
        #for , whenever a matching 
        #msg will be rcvd on xmpp stream
        #MsgListener method will be triggered.
        register_stanza_plugin(Message, Ipop_Msg)
        self.registerHandler(
            Callback('Ipop',
                     StanzaPath('message/Ipop'),
                     self.MsgListener))
         #Register event handler for session 
         #initialization/called after session 
         #with xmpp server established.
        self.add_event_handler("session_start",self.start)
        #Register handlers for groupchat messages and MuC invitations,
        #any invitation will be accepted automatically
        self.add_event_handler("groupchat_message", self.muc_message)
        self.add_event_handler("groupchat_invite", self.accept_invite)
        #Initialize other parameters
        self.user = user
        self.password = password
        self.room_passwd = room_passwd
        self.host = host
        self.uid_ip_table = {}
        
    #start a new thread to handle sleekXmpp functionality
    def xmpp_handler(self):
        try:
            if (self.connect(address = (self.host,5222))):
                thread.start_new_thread(self.process,())	
        except:
            log.error("**** Unable to start xmpp_handler thread.****")

    def ctrl_conn_init(self):
        do_set_logging(self.sock, CONFIG["tincan_logging"])
        do_set_cb_endpoint(self.sock, self.sock.getsockname())
        do_set_local_ip(self.sock, self.uid, self.ip4, gen_ip6(self.uid), 
                        CONFIG["ip4_mask"],CONFIG["ip6_mask"],
                        CONFIG["subnet_mask"])
        do_get_state(self.sock)
        log.debug("***** done conn_init******")

    # iterate over the list of all peers and request 
    # their state from tincan.	
    def get_peer_state(self):
        peer_uid_list  =  self.uid_jid.keys()
        for key in peer_uid_list:
            do_get_state(self.sock,key)

    # call tin-can to allocate resources for p2p link.
    def create_connection(self, uid, fpr, overlay_id, sec, cas, ip4):
        do_create_link(self.sock, uid, fpr, overlay_id, sec, cas)
        do_set_remote_ip(self.sock, uid, ip4, gen_ip6(uid))

    # create a conn_req after getting IP address from tincan
    def create_connection_req(self, data):
        version_ihl = struct.unpack('!B', data[54:55])
        version = version_ihl[0] >> 4
        if version == 4:
            s_addr = socket.inet_ntoa(data[66:70])
            d_addr = socket.inet_ntoa(data[70:74])
        elif version == 6:
            s_addr = socket.inet_ntop(socket.AF_INET6, data[62:78])
            d_addr = socket.inet_ntop(socket.AF_INET6, data[78:94])
            # At present, we do not handle ipv6 multicast
            if d_addr.startswith("ff02"):
                return
        # given the IP address generate UID.
        peer_uid = gen_uid(d_addr)
        log.debug('dest IP: %s dest UID: %s',d_addr,peer_uid)
        # look in the dictionary to find JID corresponding to the generated UID.
        try:
            peer_jid = self.uid_jid[peer_uid]
        except KeyError:
            log.debug("**** matching peer not found on xmpp overlay ***")
            return
        self.send_con_req(peer_jid)
        log.debug(' ON_DEMAND CON_REQ sent to %s',(peer_jid.full))
        
    #checks if all room/ip4 are present in shelve
    def check_shelve(self):
        try:
            s = shelve.open('access.db')
            if (s.has_key('room') and s.has_key('ip4')):
                self.room = s['room']
                self.ip4 = s['ip4']
                return True
            else:
                return False
        finally:
            try:
                s.close()
            except:
                log.debug("encountered problem with shelve")

    #After a xmpp session is established get roster and broadcast 
	#presence message.
    def start(self,event):
        self.get_roster()
        self.send_presence()
        if (self.check_shelve() == False):
            return
        # Register event handler for MuC presence
        self.add_event_handler("muc::%s::got_online"%self.room,
                               self.muc_online)
        # Register event handler to respond to the event when a xmpp_peer goes 
        # offline on xmpp overlay.(handle controller restart).
        self.add_event_handler("muc::%s::got_offline"%self.room,
                               self.muc_offline)
        log.debug("room %s nick %s password %s ip4 %s"%(self.room,self.nick,\
                    self.password,self.ip4))
        if (self.initialize()):
            try:
                self.plugin['xep_0045'].joinMUC(self.room, 
                    self.nick,password = self.room_passwd ,
                    wait = True)
            except:
                log.error(" *** Unable to join the MuC room. ***")
                 
    #the below handler method listens for the 
    #matched messages on the xmpp stream, extracts the set-up
	#type and payload and takes suitable action depending on them
    def MsgListener(self,message):
        # filter out messages sent from self.
        if message['mucnick'] != self.nick:
            # extract set-up type and payload.
            setup = str(message['Ipop']['setup'])
            payload = str(message['Ipop']['payload'])
            log.debug('** SETUP ** Sender: %s ** %s',\
                      message['mucnick'],setup)
            log.debug('** PAYLOAD ** Sender: %s ** %s',\
                      message['mucnick'],payload)
            # check if the message is meant for self.
            if self.nick in setup:
                if "conn_resp" in setup:
                    resp_msg = payload.split("#")
                    peer_nick = message['from'].full.split("/")[1]
                    peer_uid = resp_msg[0]
                    peer_fpr = resp_msg[1]
                    peer_ip4 = resp_msg[2]
                    peer_cas = resp_msg[3]
                    if self.xmpp_peers.get(peer_nick,None) != "resp_recvd":
                        self.xmpp_peers[peer_nick] = "resp_recvd"
                        self.uid_jid[peer_uid] = message['from']
                        log.debug('recvd conn resp from %s',(peer_nick))
                        self.create_connection( peer_uid, peer_fpr,0,
                                    CONFIG["sec"],peer_cas, peer_ip4)
                elif "conn_req" in setup:
                    req_msg =payload.split("#")
                    peer_uid = req_msg[0]
                    peer_fpr = req_msg[1]
                    peer_ip4 = req_msg[2]
                    peer_nick = message['from'].full.split("/")[1]
                    #cannot handle this request so have to initiate one to peer 
                    if (CONFIG["on-demand_connection"] or \
                        (self.xmpp_peers.get(peer_nick,None) == "p2poffline" \
                        or peer_nick not in \
                        self.xmpp_peers)) and (peer_uid<self.uid): 
                        peer_jid = message['from']
                        self.send_con_req(peer_jid)
                        log.debug('***returned connection request \
                            to %s***',peer_nick)
                    # entertain con_reqs only from > uid's to avoid loop.
                    elif (peer_uid>self.uid):
                        log.debug('*** Passed collision check, \
                                  proceeding with connection creation**')
                        # record JID corresponding to UID.
                        self.uid_jid[peer_uid] = message['from']
                        self.create_connection(peer_uid,peer_fpr,\
                                               0,CONFIG["sec"],"",peer_ip4)

                # just record JID for the given 
                # UID .
                elif "uid_exchange" in setup:
                    peer_uid = str(payload)
                    self.uid_jid[peer_uid] = message['from']
                    peer_nick = message['from'].full.split("/")[1]
                    log.debug('uid_recvd from %s',peer_nick)
                # Message recvd in case peer has closed p2p 
                # link from his side,close link from my side .
                elif "destroy" in setup:
                    peer_uid = str(payload)
                    peer_nick = message['from'].full.split("/")[1]
                    do_trim_link(self.sock, peer_uid)
                    log.debug('destroyed link to %s',(peer_nick))
                    #we check if the peer is on-line on the xmpp overlay 
                    #On-Demand is False i.e. connections should be 
                    #re-established automatically
                    #we will send conn_req back to peer
                    if not CONFIG["on-demand_connection"] and \
                       self.peer_xmpp_status[peer_nick] == "xmpp_online":
                        self.send_con_req(message['from'])
                        
    def muc_message(self, msg):
        if msg['mucnick'] != self.nick and self.nick in msg['body']:
            self.send_message(mto=msg['from'].bare,
            mbody="I heard that, %s." % msg['mucnick'],
            mtype='groupchat')
    # handler to listen to presence stanzas on the xmpp stream.
    def muc_online(self, presence):
        if presence['muc']['nick'] != self.nick:
            peer_nick = presence['muc']['nick']
            log.debug('********presence recvd from: %s',(peer_nick))
            #Handle three cases.
            # 1: If peer had gone offline on xmpp overlay its 
            #     important to broadcast one's uid to it to help 
            #    it perform trim connections.
            #  2: On-Demand --  do not send a conn_req as soon as presence 
            #     is recvd from a peer, just send one's UID 
            #  3: Regular -- respond back with a connection request.
            #     peer_xmpp_status can take only two values -- offline/online
            if self.peer_xmpp_status.get(peer_nick,None) != "xmpp_online":
                self.broadcast_uid(presence['from'],peer_nick)
                self.peer_xmpp_status[peer_nick] = "xmpp_online"
            if CONFIG["on-demand_connection"] and \
                        (peer_nick not in self.xmpp_peers
                        or self.xmpp_peers[peer_nick] == "p2poffline"):
                self.broadcast_uid(presence['from'],peer_nick)
            elif peer_nick not in self.xmpp_peers or \
                    self.xmpp_peers[peer_nick] == "p2poffline":
                self.send_con_req(presence['from'])
                
                
    # initialize all attributes required, wait until done
    def initialize(self):
        self.uid = gen_uid(self.ip4)
        parts = self.ip4.split(".")
        ip_prefix = parts[0] + "." + parts[1] + "."
        for i in range(0, 255):
            for j in range(0, 255):
                ip = ip_prefix + str(i) + "." + str(j)
                uid = gen_uid(ip)
                self.uid_ip_table[uid] = ip
        UdpServer.__init__(self,self.user,self.password,self.host,self.ip4)
        log.debug("***** Initialized UDP server ****")
        self.ctrl_conn_init()
        while (self.ipop_state == None or len(self.ipop_state) == 0):
            log.debug("#### WAITING FOR IPOP STATE ####")
            time.sleep(5)
        return True
            
        
                
    # handler method to automatically accept invites to a MuC room.         
    def accept_invite(self,inv):
        log.debug('********Invite from %s to %s',inv["from"], inv["to"])
        self.room = inv["from"]
        self.ip4 = (inv['body'].split('#')[1])
        #store room,ip4 into shelve, to be used in case controller
        #has to be restarted. 
        s = shelve.open('access.db',writeback=True)
        try:
            s['room'] = self.room
            s['ip4'] = self.ip4
            log.debug("shelve written due to new invite")
        finally:
            s.close()
        self.add_event_handler("muc::%s::got_online"%self.room,
                               self.muc_online)
        # Register event handler to respond to the event when a xmpp_peer goes 
        # offline on xmpp overlay.(handle controller restart).
        self.add_event_handler("muc::%s::got_offline"%self.room,
                               self.muc_offline)
        if (self.initialize()):
            try:
                self.plugin['xep_0045'].joinMUC(self.room, 
                    self.nick,password = self.room_passwd ,
                    wait = True)
            except:
                log.error(" *** Unable to join the MuC room. ***")


        

    # handler for xmpp_offline event
    def muc_offline(self, presence):
        # if not from self
        if presence['muc']['nick'] != self.nick:
            peer_nick = presence['muc']['nick']
            log.debug('********presence--offline recvd from: %s',(peer_nick))
            self.peer_xmpp_status[peer_nick] = "xmpp_offline"

    # Messages sent to bootstrap p2p links contain two parts
    #	1.  setup_load --- contains information that enables the 
    #            recipient to check the type of msg and if its
    #            meant for it.
    #	2.  payload --- contains information required to act on , 
    #            depending on the type of msg	
    #
    # Prepare a msg, containing ones UID and send it to peer
    def broadcast_uid(self,peer_jid,peer_nick):
        payload = unicode(self.uid)
        setup_load = unicode("uid_exchange#"+ peer_nick)
        msg = self.Message()
        msg['to'] = peer_jid.bare
        msg['type'] = 'groupchat'
        msg['Ipop']['setup'] = setup_load
        msg['Ipop']['payload'] = payload
        msg.send()
        log.debug('uid_exchange msg sent to %s',peer_nick)

    # Prepare and send a mag to peer asking him to destroy p2p link to me.
    def destroy_link(self,peer_jid):
        peer_nick = peer_jid.full.split("/")[1]
        payload = unicode(self.uid)
        setup_load = unicode("destroy#"+ peer_nick)
        msg = self.Message()
        msg['to'] = peer_jid.bare
        msg['type'] = 'groupchat'
        msg['Ipop']['setup'] = setup_load
        msg['Ipop']['payload'] = payload
        msg.send()
        log.debug('destroy_link msg sent to %s',(peer_nick))

    # Prepares and send a connection request message to peer 
    #containing one's UID,_fpr and IP address(Virtual).
    def send_con_req(self,peer_jid):
        if self.ipop_state != None and len(self.ipop_state) != 0:
            log.debug("*****IPOP STATE*****")
            log.debug( self.ipop_state)
            req_payload = self.ipop_state['_uid']+ "#" + \
                self.ipop_state['_fpr']+ \
                "#" +self.ipop_state['_ip4']
            setup_load = unicode("conn_req" + "#" + peer_jid.full)
            msg = self.Message()
            msg['to'] = peer_jid.bare
            msg['type'] = 'groupchat'
            msg['Ipop']['setup'] = setup_load
            msg['Ipop']['payload'] = req_payload
            msg.send()
            peer_nick = peer_jid.full.split("/")[1]
            self.xmpp_peers[peer_nick] = "req_sent"
            log.debug('sent conn_req to %s',(peer_nick))

    # Prepares and send a connection response to the peer, 
    # containing one's UID,_fpr,IP address(Virtual) and 
    # most vitally CAS (Candidate Set).
    def send_con_resp(self,peer_jid,peer_cas):
        resp_payload =  self.ipop_state['_uid'] + "#" + \
            self.ipop_state['_fpr'] \
            + "#" + self.ipop_state['_ip4']
        resp_payload = resp_payload + "#" + peer_cas
        resp_payload = unicode(resp_payload)
        setup_load = unicode("conn_resp" + "#" + \
                             str(peer_jid.full))
        msg = self.Message()
        msg['to'] = peer_jid.bare
        msg['type'] = 'groupchat'
        msg['Ipop']['setup'] = setup_load
        msg['Ipop']['payload'] = resp_payload
        peer_nick = peer_jid.full.split("/")[1]
        msg.send()
        log.debug('sent conn_resp to %s',(peer_nick))

    # Method called regularly to free resources allocated 
    # to offline(p2p) or Idle connections.
    def trim_connections(self):
        log.debug('peers--> %s',(self.peers))
        for k, v in self.peers.iteritems():
            # Remove off-line connection.
            if "fpr" in v and v["status"] == "offline":
                if v["last_time"] > CONFIG["wait_time"] * 2:
                    peer_jid = self.uid_jid.get(k,None)
                    peer_nick = peer_jid.full.split("/")[1]
                    # Ask peer to close connection to me and destroy 
                    # connection from me to peer.
                    self.destroy_link(peer_jid)
                    do_trim_link(self.sock, k)
                    self.xmpp_peers[peer_nick] = "p2poffline"
                    log.debug('trimmed connection with %s',(peer_nick))
            # Remove Idle connection
            if CONFIG["on-demand_connection"] and v["status"] == "online": 
                if v["last_active"] + \
                   CONFIG["on-demand_inactive_timeout"] < time.time():
                    peer_jid = self.uid_jid.get(k,None)
                    peer_nick = peer_jid.full.split("/")[1]
                    self.destroy_link(peer_jid)
                    do_trim_link(self.sock, k)
                    self.xmpp_peers[peer_nick] = "p2poffline"
                    log.debug('trimmed inactive node %s',(peer_nick))

    def serve(self):
        socks = select.select([self.sock], [], [], CONFIG["wait_time"])
        for sock in socks[0]:
            data, addr = sock.recvfrom(CONFIG["buf_size"])
            if data[0] != ipop_ver:
                logging.debug("ipop version mismatch: \
                tincan:{0} controller:{1}".format(data[0].encode("hex")
                , ipop_ver.encode("hex")))
                sys.exit()
            if data[1] == tincan_control:
                msg = json.loads(data[2:])
                logging.debug("recv %s %s" % (addr, data[2:]))
                msg_type = msg.get("type", None)
                if msg_type == "local_state":
                    self.ipop_state = msg
                elif msg_type == "con_resp":
                    fpr_len = len(self.ipop_state["_fpr"])
                    fpr = msg["data"][:fpr_len]
                    cas = msg["data"][fpr_len + 1:]
                    uid = msg["uid"]
                    log.debug('recvd conn response from tincan,\
                              will now forward it to peer')
                    self.send_con_resp(self.uid_jid[uid],cas)
                elif  msg_type == "peer_state": 
                    log.debug('***PEER STATE***')
                    log.debug('*********** LINK WITH %s  IS %s',
                              self.uid_jid.get(msg["uid"] ,None).full, 
                              msg["status"])
                    if msg["status"] == "offline" or "stats" not in msg:
                        self.peers[msg["uid"]] = msg
                    else:
                        stats = msg["stats"]
                        total_byte = 0
                        for stat in stats:
                            total_byte += stat["sent_total_bytes"]
                            total_byte += stat["recv_total_bytes"]
                        msg["total_byte"]=total_byte
                        if not msg["uid"] in self.peers:
                            msg["last_active"]=time.time()
                        elif not "total_byte" in self.peers[msg["uid"]]:
                            msg["last_active"]=time.time()
                        else:
                            if msg["total_byte"] > \
                                self.peers[msg["uid"]]["total_byte"]:
                                msg["last_active"] = time.time()
                            else:
                                msg["last_active"] = \
                                    self.peers[msg["uid"]]["last_active"]
                        self.peers[msg["uid"]] = msg			
            elif data[1] == tincan_packet:
                if not CONFIG["on-demand_connection"]:
                    return
                if len(data) < 16:
                    return
                self.create_connection_req(data[2:])

def main():
    parse_config()
    nick = CONFIG["nick"]
    server = Gvpn_UDPServer(CONFIG["xmpp_username"],CONFIG["xmpp_password"],
                            CONFIG["xmpp_host"],
                            nick,room_passwd = None)
    server.register_plugin('xep_0030') # Service Discovery
    server.register_plugin('xep_0045') # Multi-User Chat
    server.register_plugin('xep_0199') # XMPP Ping
    server.register_plugin('xep_0004') # Data Forms
    server.xmpp_handler()
    last_time = time.time()
    log.debug("**************XMPP Thread Started*********")
    while True:
        try:
            server.serve()
            time_diff = time.time() - last_time
            if time_diff > CONFIG["wait_time"]:
                server.trim_connections()
                server.get_peer_state()
                do_get_state(server.sock)
                last_time = time.time()
        except:
            pass

if __name__ == "__main__":
    main()



