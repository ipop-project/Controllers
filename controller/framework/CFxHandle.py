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
import queue as Queue


class CFxHandle(object):
    def __init__(self, CFxObject):
        self._cm_queue = Queue.Queue()  # CBT queue
        self._cm_instance = None
        self._cm_thread = None  # CM worker thread
        self._cm_config = None
        self.__cfx_object = CFxObject  # CFx object reference
        self._join_enabled = False
        self._timer_thread = None
        self._terminate_flag = False
        self.interval = 1
        self._pending_cbts = {}
        self._owned_cbts = {}

    def __get_cbt(self):
        cbt = self._cm_queue.get()  # blocking call
        return cbt

    def submit_cbt(self, cbt):
        # submit CBT to the CFx
        self.__cfx_object.submit_cbt(cbt)

    def create_cbt(self, initiator="", recipient="", action="", params=""):
        # create and return a CBT with optional parameters
        cbt = CBT(initiator, recipient, action, params)
        self._owned_cbts[cbt.tag] = cbt
        return cbt

    def create_linked_cbt(self, parent):
        cbt = self.create_cbt()
        cbt.parent = parent
        parent.child_count = parent.child_count + 1
        return cbt

    def get_parent_cbt(self, cbt):
        return cbt.parent

    def free_cbt(self, cbt):
        if not cbt.child_count == 0:
            raise RuntimeError("Invalid attempt to free a linked CBT")
        if not cbt.parent is None:
            cbt.parent.child_count = cbt.parent.child_count - 1
            cbt.parent = None
        # explicitly deallocate CBT
        self._owned_cbts.pop(cbt.tag, None)
        del cbt

    def complete_cbt(self, cbt):
        cbt.completed = True
        self._pending_cbts.pop(cbt.tag, None)
        if not cbt.child_count == 0:
            raise RuntimeError("Invalid attempt to complete a CBT with outstanding dependencies")
        self.__cfx_object.submit_cbt(cbt)

    def initialize(self):
        # intialize the CM
        self._cm_instance.initialize()

        # create the worker thread, which is started by CFx
        self._cm_thread = threading.Thread(target=self.__worker)
        self._cm_thread.setDaemon(True)

        # check whether CM requires join() or not
        self._join_enabled = True

        # check if the _cm_config has timer_interval specified
        timer_enabled = False

        try:
            self.interval = int(self._cm_config["TimerInterval"])
            timer_enabled = True
        except ValueError:
            logging.warning("Invalid timer configuration for {0}"
                            ". Timer has been disabled for this module".format("CFXHandle"))
        except KeyError:
            pass

        if timer_enabled:
            # create the timer worker thread, which is started by CFx
            self._timer_thread = threading.Thread(target=self.__timer_worker,
                                                 args=())
            self._timer_thread.setDaemon(False)

    def update_timer_interval(self, interval):
        self.interval = interval

    def __worker(self):
        # get CBT from the local queue and call process_cbt() of the
        # CBT recipient and passing the CBT as an argument
        while True:
            cbt = self.__get_cbt()

            # break on special termination CBT
            if cbt.request.action == "CFX_TERMINATE":
                self._terminate_flag = True
                module_name = self._cm_instance.__class__.__name__
                logging.info("{0} exiting".format(module_name))
                self._cm_instance.terminate()
                break
            else:
                try:
                    if not cbt.completed:
                        self._pending_cbts[cbt.tag] = cbt
                    self._cm_instance.process_cbt(cbt)
                except SystemExit:
                    sys.exit()
                except:
                    log_cbt = self.create_cbt(
                        initiator=self._cm_instance.__class__.__name__,
                        recipient="Logger",
                        action="LOG_WARNING",
                        params="CBT exception:\n"
                             "  {0}\n"
                             "  traceback:\n{1}"
                             .format(cbt, traceback.format_exc())
                    )

                    self.submit_cbt(log_cbt)

    def __timer_worker(self):
        # call the timer_method of each CM every timer_interval seconds
        event = threading.Event()
        while True:
            if self._terminate_flag:
                break
            event.wait(self.interval)

            try:
                self._cm_instance.timer_method()
            except SystemExit:
                sys.exit()
            except:
                log_cbt = self.create_cbt(
                    initiator=self._cm_instance.__class__.__name__,
                    recipient="Logger",
                    action="LOG_WARNING",
                    params="timer_method exception:\n{0}".format(traceback.format_exc())
                )
                self.submit_cbt(log_cbt)

    def query_param(self, param_name=""):
        pv = self.__cfx_object.query_param(param_name)
        return pv

    # Caller is the subscription source
    def publish_subscription(self, subscription_name):
        return self.__cfx_object.publish_subscription(self._cm_instance.__class__.__name__, subscription_name, self._cm_instance)

    def remove_subscription(self, sub):
        self.__cfx_object.RemoveSubscriptionPublisher(sub)

    # Caller is the subscription sink
    def start_subscription(self, owner_name, subscription_name):
        self.__cfx_object.start_subscription(owner_name, subscription_name, self._cm_instance)

    def end_subscription(self, owner_name, subscription_name):
        self.__cfx_object.end_subscription(owner_name, subscription_name, self._cm_instance)
