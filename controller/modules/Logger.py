#!/usr/bin/env python
import logging
import logging.handlers as lh
from controller.framework.ControllerModule import ControllerModule


class Logger(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(Logger, self).__init__(CFxHandle, paramDict, ModuleName)

    def initialize(self):
        if "LogLevel" in self.CMConfig:
            level = getattr(logging, self.CMConfig["LogLevel"])
        else:
            level = getattr(logging, "info")

        if self.CMConfig["LogOption"] == "File":
            self.logger = logging.getLogger("IPOP Rotating Log")
            self.logger.setLevel(level)
            if "LogFilePath" in self.CMConfig:
                filepath = self.CMConfig["LogFilePath"]
            else:
                filepath = "./"
            filepath+=self.CMConfig["LogFileName"]
            handler = lh.RotatingFileHandler(filename=filepath,maxBytes=self.CMConfig["LogFileSize"],\
                                   backupCount=self.CMConfig["BackupLogFileCount"])
            self.logger.addHandler(handler)
        else:

            logging.basicConfig(format='[%(asctime)s.%(msecs)03d] %(levelname)s:%(message)s', datefmt='%Y%m%d %H:%M:%S',
                                level=level)
            logging.info("Logger Module Loaded")

        # PKTDUMP mode dumps packet information
        logging.addLevelName(5, "PKTDUMP")
        logging.PKTDUMP = 5

    def processCBT(self, cbt):
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
        pass
