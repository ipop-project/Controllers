#!/usr/bi/env python
import controller.framework.fxlib as fxlib
from controller.framework.ControllerModule import ControllerModule

class NetworkLeaderElector(ControllerModule):
    def __init__(self, CFxHandle, paramDict):
        super(NetworkLeaderElector,self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.peers = {} # populated after notifcations from tincan.
        self.uid = "" 
        self.ip4 = ""
        
    def initialize(self):
        logCBT = self.CFxHandle.createCBT(initiator='NetworkLeaderElector',
                                            recipient='Logger',
                                            action='info',
                                            data="NetworkLeaderElector Loaded")
        self.CFxHandle.submitCBT(logCBT)
        
        
    def registerCBT(self, _recipient, _action, _data=''):
        cbt = self.CFxHandle.createCBT(
                                        initiator='NetworkLeaderElector',
                                        recipient=_recipient,
                                        action=_action,
                                        data=_data
                                        )
        self.CFxHandle.submitCBT(cbt)
                                            
        
    ############################################################################
    #   Messaging functions-Adopted from BaseTopologyManager                   #
	############################################################################
	
	# Send message over XMPP
	# - msg_type -> message type attribute    
	# - uid -> UID of destination node
	# - mag -> message
	def send_msg_srv(self, msg_type,uid, msg):
	    cbtdata = {"method": msg_type, "overlay_id":1, "uid":uid, "data": msg}
	    self.registerCBT('TincanSender', 'DO_SEND_MSG', cbtdata)
	    
    # Send message through ICC
    # - uid -> UID of destination peer (a tincan link must exist)
    # - msg -> message
    def send_msg_icc(self, uid, msg):
        if uid in self.peers:
                cbtdata = { 
                           "icc_type": "control",
                           "src_uid": self.uid,
                           "dst_uid": uid,
                           "msg": msg
                            }
                self.registerCBT('TincanSender','DO_SEND_ICC_MSG', cbtdata)
                
    def timer_method(self):
        my_msg = {
                  "msg_type": "nle1",
                  "src_uid": self.uid,
                  "payload": "Hello there I am NLE"
                  }
        for uid in self.peers.keys():
            if (self.peers[uid] == "online"):
                self.send_msg_icc(uid, my_msg)
                log_cbt = self.CFxHandle.createCBT(initiator='NetworkLeaderElector',
                                                    recipient='Logger',
                                                    action='info',
                                                    data="ICC msg sent to NLE"
                                                    )
                self.CFxHandle.submitCBT(log_cbt)
        
    def processCBT(self, cbt):
        
        if (cbt.action == "TINCAN_MSG"):
            msg = cbt.data
            msg_type = msg.get("type", None)
            
            if msg_type == "local_state":
                self.uid = msg["_uid"]
                self.ip4 = msg["_ip4"]
                
            elif msg_type == "peer_state":
                peer_uid = msg["uid"]
                peer_status = msg["status"]
                self.peers[peer_uid] = peer_status
                log_cbt = self.CFxHandle.createCBT(initiator='NetworkLeaderElector',
                                                    recipient='Logger',
                                                    action='info',
                                                    data= "NLE--\n"+ str(msg)
                                                    )
                self.CFxHandle.submitCBT(log_cbt) 
               
        elif (cbt.action == "ICC_MSG"):
            msg = cbt.data
            msg_type = msg.get("msg_type", None)
            
            if (msg_type == "nle1"):
                log_cbt = self.CFxHandle.createCBT(initiator='NetworkLeaderElector',
                                                    recipient='Logger',
                                                    action='info',
                                                    data=msg.get("payload", None)
                                                    )
                self.CFxHandle.submitCBT(log_cbt)
                
    def terminate(self):
        pass
            
        
                
    
                                                                             
