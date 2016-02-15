#!/usr/bi/env python
import controller.framework.fxlib as fxlib
from controller.framework.ControllerModule import ControllerModule
import time
import copy

class NetworkLeaderElector(ControllerModule):
    def __init__(self, CFxHandle, paramDict):
        super(NetworkLeaderElector,self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.peers = {} # populated after notifcations from tincan.
        self.uid = "" 
        self.ip4 = ""
        # visualizer_enabled/disabled
        self.visualizer = True
    #################################
    #   NLE   Variables             #
    #################################
        self.LCM_State = "EMPTY"
        self.MSM_State = "NONMEMBER"
        self.priority = self.CMConfig.get("priority")
        self.members = []
        self.joinTimerStart = 0
        self.pingTimerStart = 0
        self.leaveTimerStart = 0
        self.join_retries = self.CMConfig.get("join_retries")
        self.ping_retries = self.CMConfig.get("ping_retries")
        self.join_retry_counter = 0
        self.ping_retry_counter = 0
        self.timeout = self.CMConfig.get("timeout")
        # values for pending timer
        self.pending_timeout = self.CMConfig.get("pending_timeout")
        self.pendingTimerStart = 0
        # Binding LSA <Leader-Priority,Leader-UID,Source-UID>
        self.Binding = {"priority":self.priority,"leader":self.uid,"source":self.uid}
        self.log_msg("join_retries "+str(self.join_retries)+" ping_retries "+\
            str(self.ping_retries)+" timeout "+str(self.timeout)+\
                " pending_timeout "+str(self.pending_timeout)+\
                " priority "+str(self.priority))
                
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
	# - msg -> message
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
        LCM_State,binding_lsa,MSM_State = self.checkState()
        binding = copy.deepcopy(binding_lsa)
        binding_lsa["source"] = self.uid # change source to self.
        msg = {
                "msg_type": "LSA",
                "src_uid": self.uid,
                "Binding": binding_lsa
                }
        for uid in self.peers.keys():
            if (self.peers[uid] == "online" and LCM_State != "PENDING"):
                self.send_msg_icc(uid, msg)
                self.log_msg("XXXXXXXXXXXXXXXXXXXXXXXXXXX")
        # We also at this point check if any of the 
        # pings/Joins sent to the leader have timed out.
        if (MSM_State == "JOINING" and LCM_State == "REMOTE"):
            if ( int(time.time() - self.joinTimerStart) > self.timeout):
                if (self.join_retry_counter > self.join_retries):
                    self.changeState("PENDING")
                else:
                    self.join_retry_counter+=1
                    self.joinTimerStart = int(time.time())
                    self.send_Join(binding)
        if (LCM_State == "REMOTE"):
            self.log_msg("TIMER "+str(int(time.time()))+" "+str(self.pingTimerStart)+" "+str(self.ping_retry_counter))
            if ( int(time.time() - self.pingTimerStart) > self.timeout):
                if (self.ping_retry_counter > self.ping_retries):
                    self.changeState("PENDING")
                else:
                    self.ping_retry_counter+=1
                    self.pingTimerStart = int(time.time())
                    self.send_PingToLeader(binding)
        if (LCM_State == "PENDING"):
            if ( int(time.time() - self.pendingTimerStart) > self.pending_timeout):
                self.changeState("LOCAL")
                
        # Call visualizer to update the state
        if (self.visualizer):
            self.visual_debugger(LCM_State,MSM_State)
        self.log_msg("State "+self.LCM_State+ " "+self.MSM_State+" Binding "+str(self.Binding))
                
    def changeState(self,toState,msg=None):
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
            #self.Binding = {"priority":self.priority,"leader":self.uid,"source":self.uid}
            self.LCM_State = "PENDING"
            self.pendingTimerStart = int(time.time())
            
        self.log_msg("State Change "+self.LCM_State+ " "+self.MSM_State+" Binding "+str(self.Binding))
        #### UNLOCK HERE ######
        self.CFxHandle.syncLock.release()
        
    def checkState(self):
        #### LOCK ####
        self.CFxHandle.syncLock.acquire()
        state =  [self.LCM_State,self.Binding,self.MSM_State]
        #### UNLOCK ####
        self.CFxHandle.syncLock.release()
        return copy.deepcopy(state)
        
    def send_PingToLeader(self,binding=None):
        if (binding != None):
            target_uid = binding.get("leader")
            source = binding.get("source")
        else:
            target_uid = self.Binding.get("leader")
            source = self.Binding.get("source")
        stack = []
        stack.append(self.uid)
        msg = {
                "msg_type": "PING_LEADER",
                "src_uid": self.uid,
                "target_uid" : target_uid,
                "stack": stack,
                "TTL": 0
                }
        # payload is a stack, to help route ACK back
        # to the node using ICC as no direct tincan
        # link might exist.
        # The ICC message has to be sent to the 
        # source in binding.
        nexthop_uid = source
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
            
    def send_Join(self,binding=None):
        if (binding != None):
            target_uid = binding.get("leader")
            source = binding.get("source")
        else:
            target_uid = self.Binding.get("leader")
            source = self.Binding.get("source")
        stack = []
        stack.append(self.uid)
        msg = {
                "msg_type": "JOIN",
                "src_uid": self.uid,
                "target_uid" : target_uid,
                "stack": stack,
                "TTL": 0
                }
        nexthop_uid = source
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
            
    def send_Leave(self,binding=None):
        if (binding != None):
            target_uid = binding.get("leader")
            source = binding.get("source")
        else:
            target_uid = self.Binding.get("leader")
            source = self.Binding.get("source")
        stack = []
        Binding = self.checkState()[1]
        stack.append(self.uid)
        msg = {
                "msg_type": "LEAVE",
                "src_uid": self.uid,
                "target_uid" : target_uid,
                "stack": stack,
                "TTL": 0
                }
        nexthop_uid = source
        if (self.peers[nexthop_uid] == "online"):
            self.send_msg_icc(nexthop_uid, msg)
    # This method forwards messages using ICC 
    # It is possible that there be no direct
    # link between the node and leader
    def forward_msg(self,msg):
        Binding = self.checkState()[1]
        if (msg.get("msg_type") == "PING_LEADER"):
            msg["stack"].append(self.uid)
            msg["TTL"] = msg["TTL"] + 1
            nexthop_uid = Binding.get("source")
        elif (msg.get("msg_type") == "PING_LEADER_ACK"):
            nexthop_uid = msg["stack"].pop()
        elif (msg.get("msg_type") == "JOIN"):
            msg["stack"].append(self.uid)
            msg["TTL"] = msg["TTL"] + 1
            nexthop_uid = Binding.get("source")
        elif (msg.get("msg_type") == "JOIN_ACK"):
            nexthop_uid = msg["stack"].pop()
        elif (msg.get("msg_type") == "LEAVE"):
            msg["stack"].append(self.uid)
            msg["TTL"] = msg["TTL"] + 1
            nexthop_uid = Binding.get("source")
        # Whatever be the type of msg above we 
        # select next_hop uid and the message
        # next we just send the msg using ICC 
        # to next hop
        if (self.peers[nexthop_uid] == "online"):
            self.send_msg_icc(nexthop_uid, msg) 
            self.log_msg("FORWARDED MSG " + str(msg))
                   
        
    def processCBT(self, cbt):
        
        if (cbt.action == "TINCAN_MSG"):
            msg = cbt.data
            msg_type = msg.get("type", None)
            
            if msg_type == "local_state":
                self.uid = msg["_uid"]
                self.ip4 = msg["_ip4"]
                # This happens before any ICC message is exchanged
                # and can be regarded as part of initialization
                if (self.LCM_State == "EMPTY"):
                    self.Binding = {"priority":self.priority,"leader":self.uid,"source":self.uid}
                
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
            self.log_msg("Received ICC Msg " + msg_type + "\n"+str(msg))
            
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
                self.log_msg("LSA----------------   "+str(msg))
                msg_Binding = msg.get("Binding",None)
                self.log_msg("msg_Binding----------------   "+str(msg_Binding))
                if (curr_state == "EMPTY"):
                    if (msg_Binding.get("priority") > Binding.get("priority")):
                        # set leader ping timeout
                        # set retry value
                        # start pinging leader
                        # send join to leader
                        # clear membership table
                        # set state to REMOTE
                        self.changeState("REMOTE",msg_Binding)
                    else:
                        self.changeState("LOCAL",msg_Binding)
                elif (curr_state == "LOCAL"):
                    if (msg_Binding.get("priority") > Binding.get("priority")):
                        # set leader ping timeout
                        # set retry value
                        # start pinging leader
                        # send join to leader
                        # clear membership table
                        # set state to REMOTE
                        self.changeState("REMOTE",msg_Binding)
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
                        self.changeState("REMOTE",msg_Binding)
                    else:
                        # Do nothing stay in the same state.
                        pass
                elif (curr_state == "PENDING"):
                    if (msg_Binding.get("priority") > self.priority):
                        # set leader ping timeout
                        # set retry value
                        # start pinging leader
                        # send join to leader
                        # clear membership table
                        # set state to REMOTE
                        # if I am in pending state beacause of
                        # a unreachable leader, i should not accept
                        # LSA's advertising the same node as leader
                        # till i do a timeout, it's qiuite possible that
                        # other node might have not realized that the leader is 
                        # down.
                        if (msg_Binding.get("leader") != Binding.get("leader")):
                            self.changeState("REMOTE",msg_Binding)
                    
            elif (msg_type == "PING_LEADER"):
                # Am I the intended recepient, if not put my UID in the stack
                # and forward to src_uid in my Binding.
                if (msg.get("target_uid") != self.uid and msg["TTL"]<10):
                    self.forward_msg(msg)
                else:
                    if (msg.get("target_uid")== self.uid and curr_state == "LOCAL"):
                        # Return a PING_LEADER_ACK message
                        stack = msg.get("stack")
                        self.send_PingLeaderAck(stack)
            elif (msg_type == "JOIN"):
                if (msg.get("target_uid") != self.uid and msg["TTL"]<10):
                    self.forward_msg(msg)
                elif (msg.get("target_uid")== self.uid and curr_state == "LOCAL"):
                    # Add member to member's list
                    self.members.append(msg.get("src_uid"))
                    # return JOIN_ACK message
                    stack = msg.get("stack")
                    self.send_Join_Ack(stack)
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
            
            self.log_msg("Processed ICC Msg " + msg_type + "\n"+str(msg))
                
            
    def terminate(self):
        pass
        
    def visual_debugger(self,LCM_State,MSM_State):
        debug_msg = {
                        "type": "NLE",
                        "uid": self.uid,
                        "LCM_State": LCM_State,
                        "MSM_State": MSM_State
                    }
        self.registerCBT('CentralVisualizer', 'SEND_INFO', debug_msg)
            
        
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
    
                                                                             
