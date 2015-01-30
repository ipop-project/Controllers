
#!/usr/bin/env python

#        Usage:
#        1. To Invite users
#        python manageUsers.py -i invite -u applicants.ini
#        2. To block users and free allocated IP addresses.
#        python manageUsers.py -d delete -u applicants.ini
#        3. To view current IP/JID allocation.
#        python manageUsers.py -s show
#
#        Note:
#        applicants.ini file contains XMPP credentials and JID's on which the 
#        action has to be performed.
#
#        Sample applicants.ini file
#
#        # user's xmpp credentials for logging into xmpp account
#        [credentials]
#        xmpp_ip_address = 192.168.14.131
#        jid : ipopuser@ejabberd
#        password : password
#        room_name : ipoptestroom3@conference.ejabberd
#        #room_password : password
#        network_prefix : 172.31.0.0
#        # File containing configuration parameters for the room.
#        [jids]
#        ipoptester@ejabberd : None
#        ipoptester3@ejabberd : None
#        #ipoptester12@ejabberd : None <-- commented out
#        #ipoptester13@ejabberd : None
#        #ipoptester14@ejabberd : None
#        #ipoptester15@ejabberd : None
#        #ipoptester16@ejabberd : None
#        #ipoptester17@ejabberd : None
        


import sys
import sleekxmpp
import argparse
import ConfigParser
import shelve
import random
import logging

#Handle character encoding if using sub 3 python
if sys.version_info <(3,0):
    reload(sys)
    sys.setdefaultencoding('utf8')

#This class interacts with XMPP server, logs into the server
#with room admin credentials and invites/blocks other users
#Mapping between JID and IP address allocated to them are maintained in 
#a file database.
class roomManager(sleekxmpp.ClientXMPP):

    def __init__(self,jid,password,room_name,action,jid_file):
        super(roomManager,self).__init__(jid,password)
        self.room_name = room_name
        self.nick = jid.split("@")[0]
        users = shelve.open('ip_table.db')
        self.jid_file = jid_file
        self.ip_table = dict(users)
        users.close()
        self.action = action
        self.add_event_handler('session_start', self.session_start)
        self.add_event_handler('muc::%s::got_online' % self.room_name, \
                                self.muc_online)
        
    def session_start(self, event):
        self.send_presence()
        self.get_roster()
        self.room = self.plugin['xep_0045']
        self.room.joinMUC(self.room_name,self.nick)
        
        
    def muc_online(self, presence):

        logging.debug("presence message : %s"%(presence))
        if presence['muc']['nick'] == self.nick:
            logging.debug("ip table: %s"%(self.ip_table))
            #action here refers to the presence of invite
            #flag in command line argument,depending on which either
            #a invitation is sent to the JID's in applicants.ini file
            #or their access to the room is revoked.
            if self.action != None:
                for ip in self.ip_table:
                    self.room.invite(self.room_name,self.ip_table[ip],\
                        reason = '#'+ip+'#')
            elif self.action == None:
                for user in self.jid_file:
                    self.room.setAffiliation(self.room_name, \
                        jid=user,affiliation='none')
        
            self.send_presence('offline')
            self.disconnect(wait=True)
#Randomly generate a IP address, checks if it is unassigned
#and allocate it to a new user.also make a note of it in the database.
#currently generates a address in the range (ip prefix && 255.255.xxx.xxx).
#can be modified to accomdate any other range.            
def allocateIP(prefix,user_jid):
    parts = prefix.split(".")
    ip_prefix = parts[0] + "." + parts[1] + "."
    ip_table = shelve.open('ip_table.db',writeback=True)
    jid_ip_table = shelve.open('jid_ip_table.db',writeback=True)
    ip_prefix = ip_prefix + str(random.randint(1,254)) + "." + \
                str(random.randint(1,254))
    current_ip_user = ip_table.get(ip_prefix,None)
    while(True):
        if current_ip_user == None:
            try:
                ip_table[ip_prefix] = user_jid
                jid_ip_table[user_jid] = ip_prefix
            finally:
                ip_table.close()
                jid_ip_table.close()
            break
        else:
            ip_prefix = ip_prefix  + str(random.randint(1,254)) + "." + \
                        str(random.randint(1,254))
            current_ip_user = ip_table.get(ip_prefix,None)
    
#Frees the IP4 addresses by removing them from the database
#Users are blocked from the room in muc_online() method of roomManager            
def deallocate(prefix,users):
    jid_ip_table = shelve.open('jid_ip_table.db',writeback=True)
    ip_table = shelve.open('ip_table.db',writeback=True)
    jid_table = shelve.open('jid_table.db',writeback=True)
    for user in users:
        ip_prefix = jid_ip_table.get(user,None)
        if ip_prefix != None:
            del(jid_ip_table[user]) 
            del(ip_table[ip_prefix])
            del(jid_table[user])
    jid_ip_table.close()
    ip_table.close()
    jid_table.close()
    print(" Task completed, users forbidden from room, allocated IP's freed.")
    print("current allocation status.")
    show_allocation()
    
#open the database file and print the current JID <-> IP4 mappings.       
def show_allocation():
    try:
        jid_ip_table = shelve.open('jid_ip_table.db')
        for entry in jid_ip_table:
            print(entry + "::" + jid_ip_table[entry])
    finally:
        jid_ip_table.close()
    
            
#Wrapper for allocate method, simply checks that the JID is not
#already in use before allocating new IP4 address    
def allocate(prefix,users):
    jid_table = shelve.open('jid_table.db',writeback=True)
    for user in users:
        jid_inUse = jid_table.get(user,None)
        if (jid_inUse == None or jid_inUse == False):
            jid_table[user] = True
            allocateIP(prefix,user)
    jid_table.close()
    print("Task completed, IP addresses assigned")
    print("current allocation status.")
    show_allocation()
        


#reads input file to retrieve XMPP credentials and 
#other configuration data.
def mapFile(section):
    config = {}
    options = Config.options(section)
    for option in options:
        try:
            config[option] = Config.get(section, option)
            if config[option] == -1:
                print("ignore: %s" %option)
        except:
            config[option] = None
    return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-u","--userfile",help = "file containing user JIDs")
    parser.add_argument("-d","--delete",help = "delete users in file")
    parser.add_argument("-i","--invite",help = "invite users in file")
    parser.add_argument("-s","--show",help = "show current JID-IP mapping.")
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        filename='access.logging',
                         filemode='w')
    args = parser.parse_args()
    if (args.show == "show"):
        show_allocation()
        exit()
    if (args.userfile == None):
        print ("usage: ./manageUsers.py -u <file.ini> -d|i")
        exit()
    if (args.delete == None and args.invite == None):
        print ("usage: ./manageUsers.py -u <file.ini> -d|i")
        exit()
    if (args.delete != None and args.invite != None):
        print ("use only one of '-d','-i' options at a time")
        exit()
    Config = ConfigParser.ConfigParser()
    Config.read(args.userfile)
    xmpp_credentials = mapFile("credentials")
    xmpp_ip = xmpp_credentials['xmpp_ip_address']
    jid = xmpp_credentials['jid']
    xmpp_room = xmpp_credentials['room_name']
    network_prefix =  xmpp_credentials['network_prefix']
    password = xmpp_credentials['password']
    users = mapFile("jids")
    logging.debug("Users %s"%(users))
    if args.delete != None:
        deallocate(network_prefix,users)
    if args.invite != None:
        allocate(network_prefix,users)
    xmpp = roomManager(jid, password,xmpp_room,args.invite,users)
    xmpp.register_plugin('xep_0030')
    xmpp.register_plugin('xep_0045')
    xmpp.register_plugin('xep_0004')

    if xmpp.connect(address = (xmpp_ip,5222)):
        xmpp.process(block=True)
    else:
        logging.error ("Unable to connect with XMPP server.")
        

