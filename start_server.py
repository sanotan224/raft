import sys

from src.server import Server
name = str(sys.argv[1])
port = int(sys.argv[2])
otherServers = {'node1': 10000, 'node2': 10001, 'node3': 10002}

Server(name=name, port=port, otherServers=otherServers).start()