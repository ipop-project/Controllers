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
        self._config = {}
        self.parse_config()
        """
        CFxHandleDict is a dict containing the references to CFxHandles of all CMs with key as the module name and
        value as the CFxHandle reference
        """
        self._cfx_handle_dict = {}
        self.model = self._config["CFx"]["Model"]
        self._event = None
        self._subscriptions = {}
        self._node_id = self.set_node_id()
        self._load_order = [] #OrderedDict()

    def submit_cbt(self, cbt):
        recipient = cbt.request.recipient
        if cbt.op_type == "Response":
            recipient = cbt.response.recipient
        self._cfx_handle_dict[recipient]._cm_queue.put(cbt)

    def initialize(self,):
        # check for circular dependencies in the configuration file
        dependency_graph = {}
        for key in self._config:
            if key != "CFx":
                try:
                    dependency_graph[key] = self._config[key]["Dependencies"]
                except Exception as error:
                    pass

        if self.detect_cyclic_dependency(dependency_graph):
            print("Circular dependency detected in config.json. Exiting")
            sys.exit()

        self.build_load_order()
        # iterate and load the modules specified in the configuration file
        for module_name in self._load_order:
            self.load_module(module_name)

        # intialize all the CFxHandles which in turn initialize the CMs
        for module_name in self._load_order:
            self._cfx_handle_dict[module_name].initialize()

        # start all the worker and timer threads
        for module_name in self._cfx_handle_dict:
            self._cfx_handle_dict[module_name]._cm_thread.start()
            if self._cfx_handle_dict[module_name]._timer_thread:
                self._cfx_handle_dict[module_name]._timer_thread.start()

    def load_module(self, module_name):
        # import the modules dynamically
        try:
            module = importlib.import_module("controller.modules.{0}" \
                    .format(module_name))
        except ImportError as error:
            # NOTE: this bit is important as importing a module may fail
            # because an import inside it failed. If we don't handle this
            # corner case, we will get an import error with the
            # (incorrect) message that module_name does not exist.
            if not module_name in error.message:
                failed_dep_name = error.message.split(" ")[-1]
                raise ImportError("Failed to load module \"{}\" due to an" \
                                  " ImportError on dependency \"{}\"" \
                                  .format(module_name, failed_dep_name))
            module = importlib.import_module("controller.modules.{0}.{1}" \
                    .format(self.model, module_name))

        # get the class with name key from module
        module_class = getattr(module, module_name)

        # create a CFxHandle object for each module
        handle = CFxHandle(self)
        self._config[module_name]["NodeId"] = self._node_id
        instance = module_class(handle, self._config[module_name], module_name)

        handle._cm_instance = instance
        handle._cm_config = self._config[module_name]

        # store the CFxHandle object references in the
        # dict with module name as the key
        self._cfx_handle_dict[module_name] = handle

    def add_dependencies(self, module_name):
        dependencies = self._config[module_name].get("Dependencies", {})
        for dep in dependencies:
            if dep not in self._load_order:
                self.add_dependencies(dep)
        if module_name not in self._load_order:
            self._load_order.append(module_name)

    def build_load_order(self,):
        # creates a module load order based on how they are listed in the
        # config file and their dependency list
        try:
            for module_name in self._config:
                module_enabled = self._config[module_name].get("Enabled", True)
                if module_enabled and module_name != "CFx":
                    self.add_dependencies(module_name)
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
        print("Signal handler called with signal ", signum)

    def parse_config(self):
        self._config = fxlib.CONFIG

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
                    if self._config.get(key, False):
                        self._config[key].update(self.json_data[key])
                    else:
                        self._config[key] = self.json_data[key]

        if args.config_string:
            loaded_config = json.loads(args.config_string)
            for key in loaded_config:
                if self._config.get(key, None):
                    self._config[key].update(loaded_config[key])

    def set_node_id(self,):
        config = self._config["CFx"]
        # if NodeId is not specified in Config file, generate NodeId
        nodeid = config.get("NodeId", None)
        if nodeid is None or len(nodeid) == 0:
            try:
                with open("nid","r") as f:
                    nodeid = f.read()
            except IOError:
                pass
        if nodeid is None or len(nodeid) == 0:
            nodeid = str(uuid.uuid4().hex)
            with open("nid","w") as f:
                f.write(nodeid)
        return nodeid

    def wait_for_shutdown_event(self):
        self._event = threading.Event()

        # Since signal.pause() is not avaialble on windows, use event.wait()
        # with a timeout to catch KeyboardInterrupt. Without timeout, it"s
        # not possible to catch KeyboardInterrupt because event.wait() is
        # a blocking call without timeout. The if condition checks if the os
        # is windows.
        if os.name == "nt":
            while True:
                try:
                    self._event.wait(1)
                except (KeyboardInterrupt, SystemExit) as e:
                    print("Controller shutdown event: {0}".format(str(e)))
                    break
        else:
            for sig in [signal.SIGINT]:
                signal.signal(sig, self.__handler)

            # signal.pause() sleeps until SIGINT is received
            signal.pause()

    def terminate(self):
        for key in self._cfx_handle_dict:
            # create a special terminate CBT to terminate all the CMs
            terminate_cbt = self.create_cbt("CFx", key, "CFX_TERMINATE", "")

            # clear all the queues and put the terminate CBT in all the queues
            self._cfx_handle_dict[key]._cm_queue.queue.clear()

            self.submit_cbt(terminate_cbt)

        # wait for the threads to process their current CBTs and exit
            print("waiting for timer threads to exit gracefully...")
        for handle in self._cfx_handle_dict:
            if self._cfx_handle_dict[handle]._join_enabled:
                self._cfx_handle_dict[handle]._cm_thread.join()
                if self._cfx_handle_dict[handle]._timer_thread:
                    self._cfx_handle_dict[handle]._timer_thread.join()
        sys.exit(0)

    def query_param(self, module_name, param_name=""):
        try:
            if module_name in [None, ""]:
                return None
            elif module_name == "CFx":
                if param_name == "ipopVerRel":
                    return self._config["ipopVerRel"]
                if param_name == "NodeId":
                    return self._node_id
            else:
                if param_name == "":
                    return None
                else:
                    return self._config[module_name][param_name]
        except Exception as error:
            print("Exception occurred while querying data." + str(error))
            return None

    # Caller is the subscription source
    def publish_subscription(self, owner_name, subscription_name, owner):
        sub = CFxSubscription(owner_name, subscription_name)
        sub._owner = owner
        if sub._owner_name not in self._subscriptions:
            self._subscriptions[sub._owner_name] = []
        self._subscriptions[sub._owner_name].append(sub)
        return sub

    def remove_subscription(self, sub):
        sub.post_update("SUBSCRIPTION_SOURCE_TERMINATED")
        if sub._owner_name not in self._subscriptions:
            raise NameError("Failed to remove the subscription source. No such provider name exists")
        self._subscriptions[sub._owner_name].remove(sub)

    def find_subscription(self, owner_name, subscription_name):
        sub = None
        if owner_name not in self._subscriptions:
            raise NameError("The specified subscription provider was not found. No such name exists")
        for sub in self._subscriptions[owner_name]:
            if sub._subscription_name == subscription_name:
                return sub
        return None

    # Caller is the subscription sink
    def start_subscription(self, owner_name, subscription_name, Sink):
        sub = self.find_subscription(owner_name, subscription_name)
        if sub is not None:
            sub.add_subscriber(Sink)
        else:
            raise NameError("The specified subscription name was not found")

    def end_subscription(self, owner_name, subscription_name, Sink):
        sub = self.find_subscription(owner_name, subscription_name)
        if sub is not None:
            sub.remove_subscriber(Sink)


if __name__ == "__main__":
    cf = CFX()
    cf.initialize()
