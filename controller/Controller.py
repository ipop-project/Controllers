#!/usr/bin/env python
from framework.CFx import CFX


def main():

    CFx = CFX()
    CFx.initialize()
    CFx.waitForShutdownEvent()
    CFx.terminate()

if __name__ == "__main__":
    main()
