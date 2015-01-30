#!/usr/bin/env python

#        
#        Usage:
#
#        python create_room.py -r room_config.ini
#
#        sample room_config.ini file:
#
#        # user's xmpp credentials for logging into xmpp account
#        [credentials]
#        jid : ipopuser@ejabberd
#        password : password
#        # IP address/DNS for XMPP server
#        xmpp_ip : 192.168.14.131 
#        # File containing configuration parameters for the room.
#        [parameters]
#        room_name = ipoptestroom9@conference.ejabberd
#        room_description : Script configured room
#        room_persistent : True
#        room_public : True
#        room_password_protected : False
#        # room password <--commented out
#        #room_roomsecret : password
#        room_maxusers : 400
#        room_membersonly : True
#        room_members_by_default : True
#        room_allow_private_messages : True
#        room_moderated : False
#
#        Note: JID used to create the room will be the owner and will have all
#        admin rights, same JID should be used when managing users subsequently.
        
import sys
import sleekxmpp
import argparse
import ConfigParser
from sleekxmpp.xmlstream import ET
from sleekxmpp.plugins.xep_0004 import *
import logging

#Handle character encoding for sub 3 python versions
if sys.version_info <(3,0):
    reload(sys)
    sys.setdefaultencoding('utf8')

#This class handles interaction with the XMPP server, logs in with
#provided XMPP credentials and creates and configures the room.
class roomSetup(sleekxmpp.ClientXMPP):

    def __init__(self,jid,password,room_config):
        super(roomSetup,self).__init__(jid,password)
        self.room_name = room_config['room_name']
        self.room_config = room_config
        self.nick = jid.split("@")[0]
        logging.debug("nick: " + self.nick)
        logging.debug("room_name: "+self.room_name)
        #register event handlers
        self.add_event_handler('session_start', self.session_start)
        self.add_event_handler('muc::%s::got_online' % self.room_name, \
                                self.muc_online)
        
    def session_start(self, event):
        self.send_presence()
        self.get_roster()
        self.room = self.plugin['xep_0045']
        self.room.joinMUC(self.room_name,self.nick)
        
    #room is configured by pushing a XML form containing configuration
    #    to the XMPP server   
    def muc_online(self, presence):
        logging.debug("presence from server: %s"%(presence))
        if presence['muc']['nick'] == self.nick:
            d = self.plugin['xep_0045'].getRoomConfig(self.room_name)
            form = \
                reparse_form(self.plugin['xep_0045'].getRoomConfig(self.room_name))
            form.field['muc#roomconfig_roomdesc']['value'] = \
                self.room_config['room_description']
            form.field['muc#roomconfig_persistentroom']['value'] = \
                str2bool(self.room_config['room_persistent'])
            form.field['muc#roomconfig_publicroom']['value'] = \
                str2bool(self.room_config['room_public'])
            form.field['muc#roomconfig_passwordprotectedroom']['value'] = \
                str2bool(self.room_config['room_password_protected'])
            form.field['muc#roomconfig_moderatedroom']['value'] = \
                str2bool(self.room_config['room_moderated'])
            form.field['muc#roomconfig_membersonly']['value'] = \
                str2bool(self.room_config['room_membersonly'])
            form.field['muc#roomconfig_maxusers']['value'] = \
                self.room_config['room_maxusers']
            form.set_type('submit')
            self.room.setRoomConfig(self.room_name, form)
            logging.debug ("Receive presence for self. Exiting.") 
            self.send_presence('offline')
            self.disconnect(wait=True)
            print("Successfully set up the room, configuration is below")
            for key in self.room_config:
                print(key + "\t" + self.room_config[key])
    
def reparse_form(form):
    x = ET.fromstring(str(form))
    return Form(xml=x)
         
def str2bool(string):
    if string in ['True','true','yes','Yes','YES','TRUE']:
        return True
    else:
        return False
#Read the configuration file containing configuration and access information
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
    parser.add_argument("-r","--room_config",\
        help = "room configuration parameters")
    args = parser.parse_args()
    if (args.room_config == None):
        print ("need room configuration file.")
        exit()
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        filename='room_creation.logging',
                         filemode='w')
    Config = ConfigParser.ConfigParser()
    Config.read(args.room_config)
    xmpp_credentials = mapFile("credentials")
    jid = xmpp_credentials['jid']
    xmpp_ip = xmpp_credentials['xmpp_ip']
    password = xmpp_credentials['password']
    room_p = mapFile("parameters")
    logging.debug("Room Parameters: %s"%(room_p))
    xmpp = roomSetup(jid, password,room_p)
    xmpp.register_plugin('xep_0030')
    xmpp.register_plugin('xep_0045')
    xmpp.register_plugin('xep_0004')
    if xmpp.connect(address = (xmpp_ip,5222)):
        xmpp.process(block=True)
    else:
        print ("Unable to connect to XMPP server, check access connfig.")
    
