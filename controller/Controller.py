#!/usr/bin/env python
from controller.framework.CFx import CFX


def main():

    CFx = CFX()
    CFx.initialize()
    CFx.waitForShutdownEvent()
    CFx.terminate()

if __name__ == "__main__":
    main()
