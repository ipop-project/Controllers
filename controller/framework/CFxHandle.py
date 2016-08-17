#!/usr/bin/env python
import sys
import logging
import threading
import traceback

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

    def __getCBT(self):
        cbt = self.CMQueue.get()  # blocking call
        return cbt

    def submitCBT(self, cbt):
        # submit CBT to the CFx
        self.__CFxObject.submitCBT(cbt)

    def createCBT(self, initiator='', recipient='', action='', data=''):
        # create and return a CBT with optional parameters
        cbt = self.__CFxObject.createCBT(initiator, recipient, action, data)
        return cbt

    def freeCBT(self):
        # deallocate CBT (use python's automatic garbage collector)
        pass

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
            interval = int(self.CMConfig['timer_interval'])
            timer_enabled = True
        except ValueError:
            logging.warning("Invalid timer configuration for {0}"
                        ". Timer has been disabled for this module".format(key))
        except KeyError:
            pass

        if timer_enabled:
            # create the timer worker thread, which is started by CFx
            self.timer_thread = threading.Thread(target=self.__timer_worker,
                                                 args=(interval,))
            self.timer_thread.setDaemon(False)

    def __worker(self):
        # get CBT from the local queue and call processCBT() of the 
        # CBT recipient and passing the CBT as an argument
        while True:
            cbt = self.__getCBT()

            # break on special termination CBT
            if cbt.action == 'TERMINATE':
                self.terminateFlag = True
                module_name = self.CMInstance.__class__.__name__
                logging.info("{0} exiting".format(module_name))
                self.CMInstance.terminate()
                break
            else:
                try:
                    self.CMInstance.processCBT(cbt)
                except SystemExit:
                    sys.exit()
                except:
                    logCBT = self.createCBT(
                        initiator=self.CMInstance.__class__.__name__,
                        recipient='Logger',
                        action='warning',
                        data="CBT exception:\n"\
                             "    initiator {0}\n"\
                             "    recipient {1}:\n"\
                             "    action    {2}:\n"\
                             "    data      {3}:\n"\
                             "    traceback:\n{4}"\
                             .format(cbt.initiator, cbt.recipient, cbt.action,
                                    cbt.data, traceback.format_exc())
                    )

                    self.submitCBT(logCBT)

    def __timer_worker(self, interval):
        # call the timer_method of each CM every timer_interval seconds
        event = threading.Event()

        while True:
            if self.terminateFlag:
                break
            event.wait(interval)

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


    def queryParam(self, ParamName=""):
        pv = self.__CFxObject.queryParam(ParamName)
        return pv
