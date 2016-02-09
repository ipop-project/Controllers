#!/usr/bi/env python
import controller.framework.fxlib as fxlib
from controller.framework.ControllerModule import ControllerModule
import time

class NetworkLeaderElector(ControllerModule):
    def __init__(self, CFxHandle, paramDict):
        super(NetworkLeaderElector,self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.peers = {} # populated after notifcations from tincan.
        self.uid = "" 
        self.ip4 = ""
    #################################
    #   NLE   Variables             #
    #################################
        self.LCM_State = "EMPTY"
        self.MSM_State = "NONMEMBER"
        self.priority = 10 # must later read from config file
        self.members = []
        self.joinTimerStart = 0
        self.pingTimerStart = 0
        self.leaveTimerStart = 0
        self.join_retries = 5 # must later read from config file.
        self.ping_retries = 5
        self.join_retry_counter = 0
        self.ping_retry_counter = 0
        self.timeout = 30 # timeout in seconds -- to be read from config file.
        # Binding LSA <Leader-Priority,Leader-UID,Source-UID>
        self.Binding = {"priority":self.priority,"leader":self.uid,"source":self.uid}
    
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
                
    def log_msg(self,log):
        log_cbt = self.CFxHandle.createCBT(initiator='NetworkLeaderElector',
                                                    recipient='Logger',
                                                    action='info',
                                                    data=log
                                                    )
        self.CFxHandle.submitCBT(log_cbt)
        
                
    def timer_method(self):
        # Send out LSA Binding's every iteration 
        # May be a issue in large all to all network.
        Binding = self.checkState()[1]
        Binding["source"] = self.uid # change source to self.
        msg = {
                "msg_type": "LSA",
                "src_uid": self.uid,
                "Binding": Binding
                }
        for uid in self.peers.keys():
            if (self.peers[uid] == "online"):
                self.send_msg_icc(uid, msg)
        # We also at this point check if any of the 
        # pings/Joins sent to the leader have timed out.
        if (self.MSM_State == "JOINING" and self.LCM_State == "REMOTE"):
            if ( int(time.time() - self.joinTimerStart) > self.timeout):
                if (self.join_retry_counter > self.join_retries):
                    self.changeState("PENDING")
                else:
                    self.join_retry_counter+1
                    self.joinTimerStart = int(time.time())
                    self.send_join()
        if (self.LCM_State == "REMOTE"):
            if ( int(time.time() - self.pingTimerStart) > self.timeout):
                if (self.ping_retry_counter > self.ping_retries):
                    self.changeState("PENDING")
                else:
                    self.ping_retry_counter+1
                    self.pingTimerStart = int(time.time())
                    self.send_PingToLeader()
        self.log_msg("State "+self.LCM_State+ " "+self.MSM_State+" Binding "+str(self.Binding))
                
    def changeState(self,toState,msg):
        ####   LOCK HERE ######
        self.CFxHandle.syncLock.acquire()
        if (toState == "REMOTE"):
            # clear membership list
            del self.members[:]
            self.Binding = msg
            self.LCM_State = "REMOTE"
            # reset counters
            self.join_retry_counter = 0
            self.ping_retry_counter = 0
            # send ping msg to leader
            self.send_PingToLeader()
            self.pingTimerStart = int(time.time())
            # send Join to leader
            self.send_Join()
            self.MSM_State = "JOINING"
            self.joinTimerStart = int(time.time())
               
        elif (toState == "LOCAL"):
            self.Binding = {"priority":self.priority,"leader":self.uid,"source":self.uid}
            self.LCM_State = "LOCAL"
               
        elif (toState == "PENDING"):
            self.Binding = {"priority":self.priority,"leader":self.uid,"source":self.uid}
            self.LCM_State = "PENDING"
            
        self.log_msg("State Change "+self.LCM_State+ " "+self.MSM_State+" Binding "+str(self.Binding))
        #### UNLOCK HERE ######
        self.CFxHandle.syncLock.release()
        
    def checkState(self):
        #### LOCK ####
        self.CFxHandle.syncLock.acquire()
        state =  [self.LCM_State,self.Binding]
        #### UNLOCK ####
        self.CFxHandle.syncLock.release()
        return state
        
    def send_PingToLeader(self,stack = []):
        msg = {
                "msg_type": "PING_LEADER",
                "src_uid": self.uid,
                "target_uid" : self.Binding.get("leader"),
                "stack": stack.append(self.uid),
                "TTL": 0
                }
        # payload is a stack, to help route ACK back
        # to the node using ICC as no direct tincan
        # link might exist.
        # The ICC message has to be sent to the 
        # source in binding.
        nexthop_uid = self.checkState()[1].get("source")
        if (self.peers[nexthop_uid] == "online"):
            self.send_msg_icc(nexthop_uid, msg)
            
    def send_PingLeaderAck(self,stack):
        nexthop_uid = stack.pop()
        msg = { 
                "msg_type": "PING_LEADER_ACK",
                "src_uid": self.uid,
                "stack": stack
                }
        if (self.peers[nexthop_uid] == "online"):
            self.send_msg_icc(nexthop_uid, msg)
            
    def send_Join(self,stack=[]):
        Binding = self.checkState[1]
        msg = {
                "msg_type": "JOIN",
                "src_uid": self.uid,
                "target_uid" : Binding.get("leader"),
                "stack": stack.append(self.uid),
                "TTL": 0
                }
        nexthop_uid = Binding.get("source")
        if (self.peers[nexthop_uid] == "online"):
            self.send_msg_icc(nexthop_uid, msg)
    
    def send_Join_Ack(self,stack):
        nexthop_uid = stack.pop()
        msg = { 
            "msg_type": "JOIN_ACK",
            "src_uid": self.uid,
            "stack": stack
            }
        if (self.peers[nexthop_uid] == "online"):
            self.send_msg_icc(nexthop_uid, msg)
            
    def send_Leave(self,stack=[]):
        Binding = self.checkState()[1]
        msg = {
                "msg_type": "LEAVE",
                "src_uid": self.uid,
                "target_uid" : Binding.get("leader"),
                "stack": stack.append(self.uid),
                "TTL": 0
                }
        nexthop_uid = Binding.get("source")
        if (self.peers[nexthop_uid] == "online"):
            self.send_msg_icc(nexthop_uid, msg)
    # This method forwards messages using ICC 
    # It is possible that there be no direct
    # link between the node and leader
    def forward_msg(self,msg):
        Binding = self.checkState()[1]
        if (msg.get("msg_type") == "PING_LEADER"):
            msg["stack"] = msg["stack"].append(self.uid)
            msg["TTL"] = msg["TTL"] + 1
            nexthop_uid = Binding.get("source")
        elif (msg.get("msg_type") == "PING_LEADER_ACK"):
            nexthop_uid = msg["stack"].pop()
        elif (msg.get("msg_type") == "JOIN"):
            msg["stack"] = msg["stack"].append(self.uid)
            msg["TTL"] = msg["TTL"] + 1
            nexthop_uid = Binding.get("source")
        elif (msg.get("msg_type") == "JOIN_ACK"):
            nexthop_uid = msg["stack"].pop()
        elif (msg.get("msg_type") == "LEAVE"):
            msg["stack"] = msg["stack"].append(self.uid)
            msg["TTL"] = msg["TTL"] + 1
            nexthop_uid = Binding.get("source")
        # Whatever be the type of msg above we 
        # select next_hop uid and the message
        # next we just send the msg using ICC 
        # to next hop
        if (self.peers[nexthop_uid] == "online"):
            self.send_msg_icc(nexthop_uid, msg) 
                   
        
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
            ###################################################################
            #   There would be 7 types of ICC messages to be handled in NLE   #
            #    1. LSA                                                       #  
            #    2. PING                                                      #  
            #    3. PING_ACK                                                  #                      
            #    4. JOIN                                                      #              
            #    5. JOIN_ACK                                                  #                      
            #    6. LEAVE                                                     #                                  
            #    7. LEAVE_ACK                                                 #                                      
            ###################################################################
            curr_state = self.checkState()[0]
            Binding = self.checkState()[1]
            if (msg_type == "LSA"):
                msg_Binding = msg.get("Binding",none)
                if (curr_state == "EMPTY"):
                    if (msg_Binding.get("priority") > Binding.get("priority")):
                        # set leader ping timeout
                        # set retry value
                        # start pinging leader
                        # send join to leader
                        # clear membership table
                        # set state to REMOTE
                        self.changeState("REMOTE",msg)
                    else:
                        self.changeState("LOCAL",msg)
                elif (curr_state == "LOCAL"):
                    if (msg_Binding.get("priority") > Binding.get("priority")):
                        # set leader ping timeout
                        # set retry value
                        # start pinging leader
                        # send join to leader
                        # clear membership table
                        # set state to REMOTE
                        self.changeState("REMOTE",msg)
                    else:
                        # Do nothing-stay in local state
                        pass
                elif (curr_state == "REMOTE"):
                    if (msg_Binding.get("priority") > Binding.get("priority")):
                        # set leader ping timeout
                        # set retry value
                        # start pinging leader
                        # send LEAVE to old leader
                        self.send_Leave()
                        # send join to new leader
                        # clear membership table
                        # set state to REMOTE
                        self.changeState("REMOTE",msg)
                    else:
                        # Do nothing stay in the same state.
                        pass
                elif (curr_state == "PENDING"):
                    if (msg_Binding.get("priority") > Binding.get("priority")):
                        # set leader ping timeout
                        # set retry value
                        # start pinging leader
                        # send join to leader
                        # clear membership table
                        # set state to REMOTE
                        self.changeState("REMOTE",msg)
                    else:
                        self.changeState("LOCAL",msg)
            elif (msg_type == "PING_LEADER"):
                # Am I the intended recepient, if not put my UID in the stack
                # and forward to src_uid in my Binding.
                if (msg.get("target_uid") != self.uid and msg["TTL"]<10):
                    self.forward_msg(msg)
                else:
                    if (msg.get("target_uid")== self.uid and curr_state == "LOCAL"):
                        # Return a PING_LEADER_ACK message
                        self.send_PingLeaderAck(stack)
            elif (msg_type == "JOIN"):
                if (msg.get("target_uid") != self.uid and msg["TTL"]<10):
                    self.forward_msg(msg)
                elif (msg.get("target_uid")== self.uid and curr_state == "LOCAL"):
                    # Add member to member's list
                    self.members.append(msg.get("src_uid"))
                    # return JOIN_ACK message
                    self.send_Join_Ack()
            elif (msg_type == "LEAVE"):
                if (msg.get("target_uid") != self.uid and msg["TTL"]<10):
                    self.forward_msg(msg)
                elif (msg.get("target_uid")== self.uid and curr_state == "LOCAL"):
                    # remove member from member's list
                    self.members.remove(msg.get("src_uid"))
                   
            elif (msg_type == "PING_LEADER_ACK" and curr_state == "REMOTE"):
                stack = msg.get("stack")
                if (len(stack)==0):
                    # I am the intended recipient
                    # check if response if from current leader
                    # send ping message again
                    if (msg.get("src_uid") == Binding.get("leader")):
                        self.send_PingToLeader()
                        self.pingTimerStart = int(time.time())
                        self.ping_retry_counter = 0
                else:
                    self.forward_msg(msg)
                
            elif (msg_type == "JOIN_ACK" and curr_state == "REMOTE"):
                # check if the response is from current leader
                stack = msg.get("stack")
                if (len(stack)==0):
                    # I am the intended recipient
                    # check if response if from current leader
                    # send ping message again
                    if (msg.get("src_uid") == Binding.get("leader")):
                        self.MSM_State = "MEMBER"
                else:
                    self.forward_msg(msg)
            
            self.log_msg("Received ICC Msg " + msg_type + "\n"+str(msg))
                
            
    def terminate(self):
        pass
            
        
'''
Not handling LEAVE_ACK's -- A LEAVE message sent if node finds a new leader or
if old leader becomes unavailable.
In that case we will move to PENDING state and will not care about LEAVE ACK as we
cannot resend a message to the old leader anyway.

Problem:
In case of a partition between the leader and its partioned members -- the leader
will have a incoorect state of members table---
Solution-- ping from leader to it's members--
-- this could have higher time period.
'''    
    
                                                                             
