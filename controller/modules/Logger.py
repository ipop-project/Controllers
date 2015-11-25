import logging
from controller.framework.ControllerModule import ControllerModule


class Logger(ControllerModule):

    def __init__(self, CFxHandle, paramDict):

        super(Logger, self).__init__()
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict

    def initialize(self):

        if "controller_logging" in self.CMConfig:
            level = getattr(logging, self.CMConfig["controller_logging"])
            logging.basicConfig(format='[%(asctime)s.%(msecs)03d] %(levelname)s:%(name)s:%(message)s',datefmt='%m/%d %H:%M:%S',level=level)

        logging.info("Logger Module Loaded")

        # PKTDUMP mode is for more detailed than debug logging,
        # especially for dump packet contents in hexadecimal to log
        logging.addLevelName(5, "PKTDUMP")
        logging.PKTDUMP = 5

    def processCBT(self, cbt):

        if(cbt.action == 'debug'):
            logging.debug(cbt.data)
        elif(cbt.action == 'info'):
            logging.info(cbt.data)
        elif(cbt.action == 'warning'):
            logging.warning(cbt.data)
        elif(cbt.action == 'error'):
            logging.error(cbt.data)
        elif(cbt.action == "pktdump"):
            self.pktdump(message=cbt.data.get('message'),
                         dump=cbt.data.get('dump'))
        else:
            logging.warning("Unrecognized CBT from "+cbt.initiator)

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
