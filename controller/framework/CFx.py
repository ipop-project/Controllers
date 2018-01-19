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

import os
import sys
import json
import signal
import argparse
import threading
import importlib
import uuid
import controller.framework.fxlib as fxlib
import controller.framework.ipoplib as ipoplib
from collections import OrderedDict
from controller.framework.CBT import CBT as CBT
from controller.framework.CFxHandle import CFxHandle
from controller.framework.CFxSubscription import CFxSubscription


class CFX(object):

    def __init__(self):
        self.CONFIG = {}
        self.parse_config()
        '''
        CFxHandleDict is a dict containing the references to CFxHandles of all CMs with key as the module name and
        value as the CFxHandle reference
        '''
        self.CFxHandleDict = {}
        self.vpn_type = self.CONFIG['CFx']['Model']
        self.loaded_modules = ['CFx']  # list of modules already loaded
        self.event = None
        self.Subscriptions = {}
        self.NodeId = self.SetNodeId()

    def submitCBT(self, cbt):
        recipient = cbt.Request.Recipient
        self.CFxHandleDict[recipient].CMQueue.put(cbt)

    def initialize(self,):
        # check for circular dependencies in the configuration file
        dependency_graph = {}
        for key in self.CONFIG:
            if key != 'CFx':
                try:
                    dependency_graph[key] = self.CONFIG[key]['Dependencies']
                except Exception as error:
                    pass

        if self.detect_cyclic_dependency(dependency_graph):
            print("Circular dependency detected in config.json. Exiting")
            sys.exit()

        # iterate and load the modules specified in the configuration file
        for key in self.CONFIG:
            if key not in self.loaded_modules:
                self.load_module(key)

        # intialize all the CFxHandles which in turn initialize the CMs
        for module_name in self.CFxHandleDict:
         	self.CFxHandleDict[module_name].initialize()

        # start all the worker and timer threads
        for module_name in self.CFxHandleDict:
            self.CFxHandleDict[module_name].CMThread.start()
            if self.CFxHandleDict[module_name].timer_thread:
                self.CFxHandleDict[module_name].timer_thread.start()

    def load_module(self, module_name):
        if 'Enabled' in self.CONFIG[module_name]:
            module_enabled = self.CONFIG[module_name]['Enabled']
        else:
            module_enabled = True

        if (module_name not in self.loaded_modules) and module_enabled and module_name != "Tincan":
            # load the dependencies of the module
            self.load_dependencies(module_name)

            # import the modules dynamically
            try:
                module = importlib.import_module("controller.modules.{0}".format(module_name))
            except ImportError as error:
                if self.vpn_type == "GroupVPN":
                    module = importlib.import_module("controller.modules.gvpn.{0}".format(module_name))
                elif self.vpn_type == "SocialVPN":
                    module = importlib.import_module("controller.modules.svpn.{0}".format(module_name))
                else:
                    module = importlib.import_module("controller.modules.{0}.{1}".format(self.vpn_type, module_name))

            # get the class with name key from module
            module_class = getattr(module, module_name)

            # create a CFxHandle object for each module
            handle = CFxHandle(self)
            self.CONFIG[module_name]["NodeId"] = self.NodeId
            instance = module_class(handle, self.CONFIG[module_name], module_name)

            handle.CMInstance = instance
            handle.CMConfig = self.CONFIG[module_name]

            # store the CFxHandle object references in the
            # dict with module name as the key
            self.CFxHandleDict[module_name] = handle

            # intialize all the CFxHandles which in turn initialize the CMs

            self.loaded_modules.append(module_name)

    def load_dependencies(self, module_name):
        # load the dependencies of the module as specified in the configuration file
        try:
            dependencies = self.CONFIG[module_name]['Dependencies']
            for module_name in dependencies:
                if module_name not in self.loaded_modules:
                    self.load_module(module_name)
        except KeyError:
            pass

    def detect_cyclic_dependency(self, g):
        # test if the directed graph g has a cycle
        path = set()

        def visit(vertex):
            path.add(vertex)
            for neighbour in g.get(vertex, ()):
                if (neighbour in path) or visit(neighbour):
                    return True
            path.remove(vertex)
            return False

        return any(visit(v) for v in g)

    def __handler(self, signum=None, frame=None):
        print('Signal handler called with signal ', signum)

    def parse_config(self):
        self.CONFIG = fxlib.CONFIG

        parser = argparse.ArgumentParser()
        parser.add_argument("-c", help="load configuration from a file",
                            dest="config_file", metavar="config_file")
        parser.add_argument("-u", help="update configuration file if needed",
                            dest="update_config", action="store_true")
        parser.add_argument("-p", help="load remote ip configuration file",
                            dest="ip_config", metavar="ip_config")
        parser.add_argument("-s", help="configuration as json string"
                            " (overrides configuration from file)",
                            dest="config_string", metavar="config_string")
        parser.add_argument("--pwdstdout", help="use stdout as "
                            "password stream",
                            dest="pwdstdout", action="store_true")

        args = parser.parse_args()

        if args.config_file:
            # load the configuration file
            with open(args.config_file) as f:
                # load the configuration file into an OrderedDict with the
                # modules in the order in which they appear
                self.json_data = json.load(f, object_pairs_hook=OrderedDict)
                for key in self.json_data:
                    if self.CONFIG.get(key, False):
                        self.CONFIG[key].update(self.json_data[key])
                    else:
                        self.CONFIG[key] = self.json_data[key]

        if args.config_string:
            loaded_config = json.loads(args.config_string)
            for key in loaded_config:
                if self.CONFIG.get(key, None):
                    self.CONFIG[key].update(loaded_config[key])


    def SetNodeId(self,):
        config = self.CONFIG["CFx"]
        # if NodeId is not specified in Config file, generate NodeId
        nodeid = config.get("NodeId", None)
        if nodeid is None or len(nodeid) == 0:
            try:
                with open("nid","r") as f:
                    nodeid = f.read()
            except IOError:
                pass
        if nodeid is None or len(nodeid) == 0:
            nodeid = str(uuid.uuid4())
            with open("nid","w") as f:
                f.write(nodeid)
        return nodeid

    def waitForShutdownEvent(self):
        self.event = threading.Event()

        # Since signal.pause() is not avaialble on windows, use event.wait()
        # with a timeout to catch KeyboardInterrupt. Without timeout, it's
        # not possible to catch KeyboardInterrupt because event.wait() is
        # a blocking call without timeout. The if condition checks if the os
        # is windows.
        if os.name == 'nt':
            while True:
                try:
                    self.event.wait(1)
                except (KeyboardInterrupt, SystemExit) as e:
                    print("Controller shutdown event: {0}".format(str(e)))
                    break
        else:
            for sig in [signal.SIGINT]:
                signal.signal(sig, self.__handler)

            # signal.pause() sleeps until SIGINT is received
            signal.pause()

    def terminate(self):
        for key in self.CFxHandleDict:
            # create a special terminate CBT to terminate all the CMs
            terminateCBT = self.createCBT('CFx', key, 'CFX_TERMINATE', '')

            # clear all the queues and put the terminate CBT in all the queues
            self.CFxHandleDict[key].CMQueue.queue.clear()

            self.submitCBT(terminateCBT)

        # wait for the threads to process their current CBTs and exit
            print("waiting for timer threads to exit gracefully...")
        for handle in self.CFxHandleDict:
            if self.CFxHandleDict[handle].joinEnabled:
                self.CFxHandleDict[handle].CMThread.join()
                if self.CFxHandleDict[handle].timer_thread:
                    self.CFxHandleDict[handle].timer_thread.join()
        sys.exit(0)

    def queryParam(self, ModuleName, ParamName=""):
        try:
            if ModuleName in [None, ""]:
                return None
            elif ModuleName == "CFx":
                if ParamName == "NodeId":
                    return self.NodeId
            else:
                if ParamName == "":
                    return None
                else:
                    return self.CONFIG[ModuleName][ParamName]
        except Exception as error:
            print("Exception occurred while querying data." + str(error))
            return None

    # Caller is the subscription source
    def PublishSubscription(self, OwnerName, SubscriptionName, Owner):
        sub = CFxSubscription(OwnerName, SubscriptionName)
        sub.Owner = Owner
        if sub.OwnerName not in self.Subscriptions:
            self.Subscriptions[sub.OwnerName] = []
        self.Subscriptions[sub.OwnerName].append(sub)
        return sub

    def RemoveSubscription(self, sub):
        sub.PostUpdate("SUBSCRIPTION_SOURCE_TERMINATED")
        if sub.OwnerName not in self.Subscriptions:
            raise NameError("Failed to remove the subscription source. No such provider name exists")
        self.Subscriptions[sub.OwnerName].remove(sub)

    def findSubscription(self, OwnerName, SubscriptionName):
        sub = None
        if OwnerName not in self.Subscriptions:
            raise NameError("The specified subscription provider was not found. No such name exists")
        for sub in self.Subscriptions[OwnerName]:
            if sub.SubscriptionName == SubscriptionName:
                return sub
        return None

    # Caller is the subscription sink
    def StartSubscription(self, OwnerName, SubscriptionName, Sink):
        sub = self.findSubscription(OwnerName, SubscriptionName)
        if sub is not None:
            sub.AddSubscriber(Sink)
        else:
            raise NameError("The specified subscription name was not found")

    def EndSubscription(self, OwnerName, SubscriptionName, Sink):
        sub = self.findSubscription(OwnerName, SubscriptionName)
        if sub is not None:
            sub.RemoveSubscriber(Sink)


if __name__ == "__main__":
    cf = CFX()
    cf.initialize()
