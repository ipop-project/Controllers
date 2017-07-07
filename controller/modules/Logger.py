#!/usr/bin/env python
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
            logging.basicConfig(format='[%(asctime)s.%(msecs)03d] %(levelname)s:\n%(message)s\n', datefmt='%H:%M:%S',
                                level=level)
            logging.info("Logger Module Loaded")
        else:
            # Extracts the filepath else sets logs to current working directory
            filepath = self.CMConfig.get("LogFilePath", "./")
            fqname = filepath + \
                self.CMConfig.get("CtrlLogFileName", "ctrl.log")
            if not os.path.isdir(filepath):
              os.mkdir(filepath)
            self.logfilehandle = open(fqname, 'w+')
            sys.stderr = self.logfilehandle   # Set the standard error log to write to file
            sys.stdout = self.logfilehandle   # Set the standard output log to write to file
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

        # PKTDUMP mode dumps packet information
        logging.addLevelName(5, "PKTDUMP")
        logging.PKTDUMP = 5

    def processCBT(self, cbt):
        # Extracting the logging level information from the CBT action tag
        if cbt.action == 'debug':
            if self.CMConfig["LogOption"] == "File":
                self.logger.debug(cbt.data)
            else:
                logging.debug(cbt.data)
        elif cbt.action == 'info':
            if self.CMConfig["LogOption"] == "File":
                self.logger.info(cbt.data)
            else:
                logging.info(cbt.data)
        elif cbt.action == 'warning':
            if self.CMConfig["LogOption"] == "File":
                self.logger.warning(cbt.data)
            else:
                logging.warning(cbt.data)
        elif cbt.action == 'error':
            if self.CMConfig["LogOption"] == "File":
                self.logger.error(cbt.data)
            else:
                logging.error(cbt.data)
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
      self.logfilehandle.close()

