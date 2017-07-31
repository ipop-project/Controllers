#!/usr/bin/env python
from controller.framework.CFx import CFX
import psutil
import time


# Function checks the system process table for Tincan process
def checktincanstate():
    # Iterates across process table to find Tincan process
    for process in psutil.process_iter():
        if type(process) is str:
            if process.find("tincan") != -1 or process == "ipop-tincan":
                return True
        else:
            if process.name().find("tincan") != -1 or process.name() == "ipop-tincan":
                return True
    return False


def main():
    stime = time.time()
    # Loop till Tincan not in running state
    while checktincanstate() is False:
        # Print warning message to the console that the Tincan process has not been started every 10 sec interval
        if time.time()-stime > 10:
            print("Waiting on IPOP Tincan to start...")
            stime = time.time()
    # Create CFX object that initializes internal datastructure of all the controller modules
    cfx = CFX()
    cfx.initialize()
    cfx.waitForShutdownEvent()
    cfx.terminate()

if __name__ == "__main__":
    main()
