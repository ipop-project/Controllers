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
            logging.basicConfig(format='[%(asctime)s.%(msecs)03d] %(levelname)s: %(message)s\n', datefmt='%H:%M:%S',
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
                "[%(asctime)s.%(msecs)03d] %(levelname)s:%(message)s", datefmt='%Y%m%d %H:%M:%S')
            handler.setFormatter(formatter)
            # Adds the filehandler to the Python logger module
            self.logger.addHandler(handler)

        self.logger.info("Logger: Module loaded")
        # PKTDUMP mode dumps packet information
        logging.addLevelName(5, "PKTDUMP")
        logging.PKTDUMP = 5

    def process_cbt(self, cbt):
        if cbt.OpType == "Request":
            # Extracting the logging level information from the CBT action tag
            if cbt.Request.Action == "LOG_DEBUG" or cbt.Request.Action == "debug":
                self.logger.debug("{0}: {1}".format(cbt.Request.Initiator, cbt.Request.Params))
                cbt.SetResponse(None, True)
            elif cbt.Request.Action == "LOG_INFO" or cbt.Request.Action == "info":
                self.logger.info(cbt.Request.Initiator + ": " + cbt.Request.Params)
                cbt.SetResponse(None, True)
            elif cbt.Request.Action == "LOG_WARNING" or cbt.Request.Action == 'warning':
                self.logger.warning(cbt.Request.Initiator + ": " + cbt.Request.Params)
                cbt.SetResponse(None, True)
            elif cbt.Request.Action == "LOG_ERROR" or cbt.Request.Action == 'error':
                self.logger.error(cbt.Request.Initiator + ": " + cbt.Request.Params)
                cbt.SetResponse(None, True)
            elif cbt.Request.Action == "pktdump":
                self.pktdump(message=cbt.Request.Params.get('message'),
                             dump=cbt.Request.Params.get('dump'))
                cbt.SetResponse(None, True)
            elif cbt.Request.Action == "LOG_QUERY_CONFIG":
                cbt.SetResponse(self._cm_config, True)
            else:
                log = "Unsupported CBT action in request {0}".format(cbt)
                self.logger.warning("{0}: {1}".format(self._module_name, log))
            self._cfx_handle.complete_cbt(cbt)
        elif cbt.OpType == "Response":
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
