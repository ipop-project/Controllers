#!/usr/bin/env python

import socket
import time

#cc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
cc_sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
time.sleep(1)

#while True:
#   try:
#       time.sleep(3)
#       self.cc_sock.bind((10.0.3.3, 30000]))
#   except Exception as e:
#       logging.debug("Wait till ipop up ::: {0}".format(e))
#       continue
#   else:
#       break

a="Hello World !"

#cc_sock.sendto(a, ("10.0.3.4", 30000))
cc_sock.sendto(a, ("fd50:dbc:41f2:4a3c:424e:9e68:d4aa:b48a", 30000))
