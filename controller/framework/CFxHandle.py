import Queue
import logging
import threading
import traceback

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

        cbt = self.CMQueue.get()  # Blocking call
        return cbt

    def submitCBT(self, cbt):

        # Submit CBT to the CFx
        self.__CFxObject.submitCBT(cbt)

    def createCBT(self, initiator='', recipient='', action='', data=''):

        # Create and return a CBT with optional parameters
        cbt = self.__CFxObject.createCBT(initiator, recipient, action, data)
        return cbt

    def freeCBT(self):

        # Deallocate the CBT here
        # Python automatic garbage collector handles it anyway
        pass

    def initialize(self):

        # Intialize CM first
        self.CMInstance.initialize()

        # Create worker thread, which is started by CFx
        self.CMThread = threading.Thread(target=self.__worker)
        self.CMThread.setDaemon(True)

        # Check whether CM requires join() or not
        #if(self.CMConfig['joinEnabled'] == 'True'):
        self.joinEnabled = True

        # Check if the CMConfig has timer_interval specified
        timer_enabled = False

        try:
            interval = int(self.CMConfig['timer_interval'])
            timer_enabled = True
        except ValueError:
            logging.warning("Invalid timer configuration for " + key +
                            ". Timer has been disabled for this module")
        except KeyError:
            pass

        if(timer_enabled):

            # Create timer worker thread. CFx is responsible to start
            # this thread
            self.timer_thread = threading.Thread(target=self.__timer_worker,
                                                 args=(interval,))
            self.timer_thread.setDaemon(False)

    def __worker(self):

        # Get CBT from local queue, and call processCBT() which
        # is responsible for processing one CBT, given as a parameter

        while(True):

            cbt = self.__getCBT()

            # Break the loop if special terminate CBT is received
            if(cbt.action == 'TERMINATE'):
                self.terminateFlag = True
                module_name = self.CMInstance.__class__.__name__
                logging.info(module_name+" exiting")
                self.CMInstance.terminate()
                break
            else:
                try:
                    self.CMInstance.processCBT(cbt)
                except:
                    logCBT = self.createCBT(initiator=self.CMInstance.__class__.__name__,
                                                                  recipient='Logger',
                                                                  action='warning',
                                                                  data=traceback.format_exc())
                    self.submitCBT(logCBT)

    def __timer_worker(self, interval):

        # Call the timer_method of CMs every x seconds
        # x is specified in config.json as timer_interval
        event = threading.Event()

        while(True):
            if(self.terminateFlag):
                break
            event.wait(interval)
            try:
                self.CMInstance.timer_method()
            except:
                logCBT = self.createCBT(initiator=self.CMInstance.__class__.__name__,
                                                                recipient='Logger',
                                                                action='warning',
                                                                data=traceback.format_exc())
                self.submitCBT(logCBT)

    def queryParam(self, ParamName=""):
        pv = self.__CFxObject.queryParam(ParamName)
        return pv