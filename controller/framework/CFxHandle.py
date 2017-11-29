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
import logging
import threading
import traceback
from controller.framework.CBT import CBT

py_ver = sys.version_info[0]
if py_ver == 3:
    import queue as Queue
else:
    import Queue


class CFxHandle(object):
    def __init__(self, CFxObject):
        self.CMQueue = Queue.Queue()  # CBT queue
        self.CMInstance = None
        self.CMThread = None  # CM worker thread
        self.CMConfig = None
        self.__CFxObject = CFxObject  # CFx object reference
        self.joinEnabled = False
        self.timer_thread = None
        self.terminateFlag = False
        self.interval = 1
        self.PendingCBTs = {}
        self.OwnedCBTs = {}

    def __getCBT(self):
        cbt = self.CMQueue.get()  # blocking call
        return cbt

    def submitCBT(self, cbt):
        # submit CBT to the CFx
        self.__CFxObject.submitCBT(cbt)

    def createCBT(self, initiator='', recipient='', action='', data=''):
        # create and return a CBT with optional parameters
        cbt = CBT(initiator, recipient, action, data)
        self.OwnedCBTs[cbt.Tag] = cbt
        return cbt

    def CreateLinkedCBT(self, parent):
        cbt = self.createCBT()
        cbt.Parent = parent
        parent.ChildCount = parent.ChildCount + 1
        return cbt

    def GetParentCBT(self, cbt):
        return cbt.Parent

    def freeCBT(self, cbt):
        if not cbt.ChildCount == 0:
            raise RuntimeError("Invalid attempt to free a linked CBT")
        if not cbt.Parent is None:
            cbt.Parent.ChildCount = cbt.Parent.ChildCount - 1
            cbt.Parent = None
        # explicitly deallocate CBT
        self.OwnedCBTs.pop(cbt.Tag, None)
        del cbt

    def CompleteCBT(self, cbt):
        cbt.Completed = True
        self.PendingCBTs.pop(cbt.Tag, None)
        self.__CFxObject.submitCBT(cbt)
        if not cbt.ChildCount == 0:
            raise RuntimeError("Invalid attempt to complete a CBT with outstanding dependencies")

    def initialize(self):
        # intialize the CM
        self.CMInstance.initialize()

        # create the worker thread, which is started by CFx
        self.CMThread = threading.Thread(target=self.__worker)
        self.CMThread.setDaemon(True)

        # check whether CM requires join() or not
        self.joinEnabled = True

        # check if the CMConfig has timer_interval specified
        timer_enabled = False

        try:
            self.interval = int(self.CMConfig['TimerInterval'])
            timer_enabled = True
        except ValueError:
            logging.warning("Invalid timer configuration for {0}"
                            ". Timer has been disabled for this module".format("CFXHandle"))
        except KeyError:
            pass

        if timer_enabled:
            # create the timer worker thread, which is started by CFx
            self.timer_thread = threading.Thread(target=self.__timer_worker,
                                                 args=())
            self.timer_thread.setDaemon(False)

    def updateTimerInterval(self, interval):
        self.interval = interval

    def __worker(self):
        # get CBT from the local queue and call processCBT() of the
        # CBT recipient and passing the CBT as an argument
        while True:
            cbt = self.__getCBT()

            # break on special termination CBT
            if cbt.action == 'CFX_TERMINATE':
                self.terminateFlag = True
                module_name = self.CMInstance.__class__.__name__
                logging.info("{0} exiting".format(module_name))
                self.CMInstance.terminate()
                break
            else:
                try:
                    if not cbt.Completed:
                        self.PendingCBTs[cbt.Tag] = cbt
                    self.CMInstance.processCBT(cbt)
                except SystemExit:
                    sys.exit()
                except:
                    logCBT = self.createCBT(
                        initiator=self.CMInstance.__class__.__name__,
                        recipient='Logger',
                        action='warning',
                        data="CBT exception:\n"
                             "    initiator {0}\n"
                             "    recipient {1}:\n"
                             "    action    {2}:\n"
                             "    data      {3}:\n"
                             "    traceback:\n{4}"
                             .format(cbt.initiator, cbt.recipient, cbt.action,
                                     cbt.data, traceback.format_exc())
                    )

                    self.submitCBT(logCBT)

    def __timer_worker(self):
        # call the timer_method of each CM every timer_interval seconds
        event = threading.Event()
        while True:
            if self.terminateFlag:
                break
            event.wait(self.interval)

            try:
                self.CMInstance.timer_method()
            except SystemExit:
                sys.exit()
            except:
                logCBT = self.createCBT(
                    initiator=self.CMInstance.__class__.__name__,
                    recipient='Logger',
                    action='warning',
                    data="timer_method exception:\n{0}".format(traceback.format_exc())
                )
                self.submitCBT(logCBT)

    def queryParam(self, ModuleName, ParamName=""):
        pv = self.__CFxObject.queryParam(ModuleName, ParamName)
        return pv

    # Caller is the subscription source
    def PublishSubscription(self, SubscriptionName):
        return self.__CFxObject.PublishSubscription(self.CMInstance.__class__.__name__, SubscriptionName, self.CMInstance)

    def RemoveSubscription(self, sub):
        self.__CFxObject.RemoveSubscriptionPublisher(sub)

    # Caller is the subscription sink
    def StartSubscription(self, OwnerName, SubscriptionName):
        self.__CFxObject.StartSubscription(OwnerName, SubscriptionName, self.CMInstance)

    def EndSubscription(self, OwnerName, SubscriptionName):
        self.__CFxObject.EndSubscription(OwnerName, SubscriptionName, self.CMInstance)
