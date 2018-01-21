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

import logging
import logging.handlers as lh
import os
import sys
from controller.framework.ControllerModule import ControllerModule


class Logger(ControllerModule):
    def __init__(self, cfx_handle, module_config, module_name):
        super(Logger, self).__init__(cfx_handle, module_config, module_name)

    def initialize(self):
        # Extracts the controller Log Level from the ipop-config file,
        # If nothing is provided the default is INFO
        if "LogLevel" in self._cm_config:
            level = getattr(logging, self._cm_config["LogLevel"])
        else:
            level = getattr(logging, "info")
        # Check whether the Logging is set to File by the User
        if self._cm_config["Device"] == "Console":
            # Console logging
            logging.basicConfig(format="[%(asctime)s.%(msecs)03d] %(levelname)s: %(message)s\n", datefmt="%H:%M:%S",
                                level=level)
            self.logger = logging.getLogger("IPOP console logger");
        else:
            # Extracts the filepath else sets logs to current working directory
            filepath = self._cm_config.get("Directory", "./")
            fqname = filepath + \
                self._cm_config.get("CtrlLogFileName", "ctrl.log")
            if not os.path.isdir(filepath):
              os.mkdir(filepath)
            self.logger = logging.getLogger("IPOP Rotating Log")
            self.logger.setLevel(level)
            # Creates rotating filehandler
            handler = lh.RotatingFileHandler(filename=fqname, maxBytes=self._cm_config["MaxFileSize"],
                                             backupCount=self._cm_config["MaxArchives"])
            formatter = logging.Formatter(
                "[%(asctime)s.%(msecs)03d] %(levelname)s:%(message)s", datefmt="%Y%m%d %H:%M:%S")
            handler.setFormatter(formatter)
            # Adds the filehandler to the Python logger module
            self.logger.addHandler(handler)

        self.logger.info("Logger: Module loaded")
        # PKTDUMP mode dumps packet information
        logging.addLevelName(5, "PKTDUMP")
        logging.PKTDUMP = 5

    def process_cbt(self, cbt):
        if cbt.op_type == "Request":
            log_entry = "{0}: {1}".format(cbt.request.initiator, cbt.request.params)
            # Extracting the logging level information from the CBT action tag
            if cbt.request.action == "LOG_DEBUG" or cbt.request.action == "debug":
                self.logger.debug(log_entry)
                cbt.set_response(None, True)
            elif cbt.request.action == "LOG_INFO" or cbt.request.action == "info":
                self.logger.info(log_entry)
                cbt.set_response(None, True)
            elif cbt.request.action == "LOG_WARNING" or cbt.request.action == "warning":
                self.logger.warning(log_entry)
                cbt.set_response(None, True)
            elif cbt.request.action == "LOG_ERROR" or cbt.request.action == "error":
                self.logger.error(log_entry)
                cbt.set_response(None, True)
            elif cbt.request.action == "pktdump":
                self.pktdump(message=cbt.request.params.get("message"),
                             dump=cbt.request.params.get("dump"))
                cbt.set_response(None, True)
            elif cbt.request.action == "LOG_QUERY_CONFIG":
                cbt.set_response(self._cm_config, True)
            else:
                log = "Unsupported CBT action in request {0}".format(cbt)
                self.logger.warning("{0}: {1}".format(self._module_name, log))
            self._cfx_handle.complete_cbt(cbt)
        elif cbt.op_type == "Response":
            print(cbt) #TODO remove before release
            self.free_cbt(cbt)

    def timer_method(self):
        pass

    def pktdump(self, message, dump=None, *args, **argv):
        hext = ""
        if dump:
            for i in range(0, len(dump), 2):
                hext += dump[i:i+2].encode("hex")
                hext += " "
                if i % 16 == 14:
                    hext += "\n"
            logging.log(5, message + "\n" + hext)
        else:
            logging.log(5, message, *args, **argv)

    def terminate(self):
      pass
