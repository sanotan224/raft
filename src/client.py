import socket

from src.message_pass import *


class Client:
    def __init__(self, server_port=10000):
        self.sock = None
        self.server_port = server_port
        self.changedPort = True
        self.oldMsg = None

    def start(self):
        while self.changedPort:
            self.changedPort = False
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            server_address = ('localhost', self.server_port)
            print(f"connecting to {server_address[0]} port {server_address[1]}")
            self.sock.connect(server_address)

            running = True
            while running:
                try:
                    if self.oldMsg != None:
                        message = self.oldMsg
                    else:
                        message = input("Type your message:\n")
                        message = 'client|@' + message
                    print(f"sending {message}")

                    send_message(self.sock, message.encode('utf-8'))
                    self.oldMsg = None
                    data = receive_message(self.sock)
                    print(f"received {data}")
                    dataDec = data.decode("utf-8")
                    print(dataDec)
                    if "I am not the leader. The last leader I heard from is" in dataDec:
                        self.server_port = int(dataDec[dataDec.find("[") + 1:dataDec.find("]")])
                        self.changedPort = True
                        self.oldMsg = message
                        running = False
                except:
                    print(f"closing socket")
                    self.sock.close()
                    running = False
