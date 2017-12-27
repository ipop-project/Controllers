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
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(Logger, self).__init__(CFxHandle, paramDict, ModuleName)

    def initialize(self):
        # Extracts the controller Log Level from the ipop-config file,
        # If nothing is provided the default is INFO
        if "LogLevel" in self.CMConfig:
            level = getattr(logging, self.CMConfig["LogLevel"])
        else:
            level = getattr(logging, "info")
        # Check whether the Logging is set to File by the User
        if self.CMConfig["LogOption"] == "Console":
            # Console logging
            logging.basicConfig(format='[%(asctime)s.%(msecs)03d] %(levelname)s: %(message)s\n', datefmt='%H:%M:%S',
                                level=level)
            self.logger = logging.getLogger("IPOP console logger");
        else:
            # Extracts the filepath else sets logs to current working directory
            filepath = self.CMConfig.get("LogFilePath", "./")
            fqname = filepath + \
                self.CMConfig.get("CtrlLogFileName", "ctrl.log")
            if not os.path.isdir(filepath):
              os.mkdir(filepath)
            self.logger = logging.getLogger("IPOP Rotating Log")
            self.logger.setLevel(level)
            # Creates rotating filehandler
            handler = lh.RotatingFileHandler(filename=fqname, maxBytes=self.CMConfig["LogFileSize"],
                                             backupCount=self.CMConfig["BackupLogFileCount"])
            formatter = logging.Formatter(
                "[%(asctime)s.%(msecs)03d] %(levelname)s:%(message)s", datefmt='%Y%m%d %H:%M:%S')
            handler.setFormatter(formatter)
            # Adds the filehandler to the Python logger module
            self.logger.addHandler(handler)

        self.logger.info("Logger: Module loaded")
        # PKTDUMP mode dumps packet information
        logging.addLevelName(5, "PKTDUMP")
        logging.PKTDUMP = 5

    def processCBT(self, cbt):
        # Extracting the logging level information from the CBT action tag
        if cbt.action == "LOG_DEBUG" or cbt.action == "debug":
            self.logger.  debug(cbt.initiator + ": " + cbt.data)
        elif cbt.action == "LOG_INFO" or cbt.action == "info":
            self.logger.info(cbt.initiator + ": " + cbt.data)
        elif cbt.action == "LOG_WARNING" or cbt.action == 'warning':
                self.logger.warning(cbt.initiator + ": " + cbt.data)
        elif cbt.action == "LOG_ERROR" or cbt.action == 'error':
                self.logger.error(cbt.initiator + ": " + cbt.data)
        elif cbt.action == "pktdump":
            self.pktdump(message=cbt.data.get('message'),
                         dump=cbt.data.get('dump'))
        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

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
