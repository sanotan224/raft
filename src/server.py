import threading
from socket import *
import random

from src.message_pass import *

from src.log_manager import LogManager
from src.append_entries_call import AppendEntriesCall
from src.append_entries_response import AppendEntriesResponse
from src.request_vote_call import RequestVoteCall
from src.request_vote_response import RequestVoteResponse

import ast

class Server:
    def __init__(self, name, port=10000, otherServers=None):
        if otherServers is None:
            otherServers = {}
        self.port = port
        self.name = name
        self.log_manager = LogManager(server_name=name, otherServers=otherServers)
        self.log_manager.clearFile()
        self.latest_leader = "Yet unelected"
        self.latest_leader_port = 0

        self.leader = False
        self.voting = True
        self.heartbeat_timer = None

        self.followers_with_update_status = {}
        self.current_operation = ''
        self.current_operation_committed = False

        self.timeout = float(random.randint(10, 18))
        self.election_countdown = threading.Timer(self.timeout, self.start_election)
        print("Server started with timeout of : " + str(self.timeout))
        self.election_countdown.start()

        self.election_allowed = True
        self.allow_election_countdown = threading.Timer(float(10), self.allow_election)
        self.allow_election_countdown.start()

        for server_name in self.log_manager.other_server_names(name):
            self.followers_with_update_status[server_name] = False

        if self.voting:
            self.log_manager.voted_for_me[self.name] = False

    def allow_election(self):
        self.election_allowed = True

    def start_election(self):
        if not self.leader:
            self.log_manager.current_term += 1
            self.timeout = float(random.randint(10, 18))
            self.election_countdown = threading.Timer(self.timeout, self.start_election)
            print("Server reset election timeout to : " + str(self.timeout))
            self.election_countdown.start()
            self.log_manager.voted_for_me[self.name] = True
            self.broadcast(self, self.with_return_address(
                self,
                RequestVoteCall(
                    for_term=str(self.log_manager.current_term),
                    candidate_port=str(self.port),
                    latest_log_index=str(self.log_manager.highest_index),
                    latest_log_term=str(self.log_manager.latest_term_in_logs)
                ).to_message()
            ))

    def send(self, message, to_port):
        print(f"connecting to port {to_port}")
        to_address = ("localhost", int(to_port))
        peer_socket = socket(AF_INET, SOCK_STREAM)
        try:
            peer_socket.connect(to_address)
            encoded_message = message.encode('utf-8')
            try:
                print(f"sending {encoded_message} to {to_port}")
                send_message(peer_socket, encoded_message)
                peer_socket.close()
            except Exception as e:
                print(f"closing socket due to {str(e)}")
                peer_socket.close()
        except ConnectionRefusedError as e:
            print(f"Port {to_port} isn't up right now")

    def start(self):
        server_address = ('localhost', self.port)
        print("starting up on " + str(server_address[0]) + " port " + str(server_address[1]))
        print("my index and term " + str(self.log_manager.highest_index) + " " + str(self.log_manager.current_term))
        self.server_socket = socket(AF_INET, SOCK_STREAM)
        self.server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_socket.bind(server_address)
        self.server_socket.listen(6000)
        if self.leader:
            self.prove_aliveness()
        while True:
            connection, client_address = self.server_socket.accept()
            print("connection from " + str(client_address))
            threading.Thread(target=self.manage_messaging, args=(connection, self.log_manager)).start()

    def prove_aliveness(self):
        print("Sending Heartbeat!")
        if self.leader:
            self.broadcast(self, self.with_return_address(
                self,
                AppendEntriesCall(
                    in_term=self.log_manager.current_term,
                    previous_index=self.log_manager.highest_index + 1,
                    previous_term=self.log_manager.latest_term_in_logs,
                    entries=[]
                ).to_message()
            ))
            self.heartbeat_timer = threading.Timer(5.0, self.prove_aliveness)
            self.heartbeat_timer.start()

    def mark_updated(self, server_name):
        print("MARK UPDATED")
        self.followers_with_update_status[server_name] = True
        if self.current_operation == '':
            return
        trues = len(list(filter(lambda x: x is True, self.followers_with_update_status.values())))
        falses = len(list(filter(lambda x: x is False, self.followers_with_update_status.values())))
        if trues >= falses:
            print("Committing entry: " + self.current_operation)
            self.current_operation_committed = True
            self.log_manager.highest_index = self.log_manager.highest_index + 1
            self.broadcast(self, self.with_return_address(
                self,
                AppendEntriesCall(
                    in_term=self.log_manager.current_term,
                    previous_index=self.log_manager.highest_index,
                    previous_term=self.log_manager.latest_term_in_logs,
                    entries=[]
                ).to_message()
            ))
            self.current_operation = ''
            self.current_operation_committed = False
            for server_name in self.log_manager.other_server_names(self.name):
                self.followers_with_update_status[server_name] = False

    def mark_voted(self, server_name):
        self.log_manager.voted_for_me[server_name] = True
        trues = len(list(filter(lambda x: x is True, self.log_manager.voted_for_me.values())))
        falses = len(list(filter(lambda x: x is False, self.log_manager.voted_for_me.values())))
        if trues >= falses:
            print("I win the election for term " + str(self.log_manager.current_term) + "!")
            self.leader = True
            self.prove_aliveness()
            for server_name in self.log_manager.voted_for_me.keys():
                self.log_manager.voted_for_me[server_name] = False

    def manage_messaging(self, connection, kvs):
        try:
            while True:
                operation = receive_message(connection)
                if operation:
                    destination_name, destination_port, response = self.respond(kvs, operation)
                    if response == '':
                        break
                    if destination_name == "client":
                        send_message(connection, response.encode('utf-8'))
                    else:
                        self.send(response, to_port=int(destination_port))
                else:
                    print("no more data")
                    break
        finally:
            connection.close()

    def broadcast(self, server, message):
        print("Broadcasting " + message)
        for other_server_address in self.log_manager.destination_addresses(server.name):
            server.send(message, to_port=other_server_address)

    def with_return_address(self, server, response):
        return server.name + "|" + str(server.port) + "@" + response

    def return_address_and_message(self, string_request):
        address_with_message = string_request.split("@")
        name, port = address_with_message[0].split("|")
        return name, port, "@".join(address_with_message[1:])

    def respond(self, log_manager, operation):
        send_pending = True
        string_request = operation.decode("utf-8")
        server_name, server_port, string_operation = self.return_address_and_message(string_request)
        print("from " + server_name + ": received " + string_operation)

        response = ''

        if string_operation.split(" ")[0] == "append_entries_call":
            call = AppendEntriesCall.from_message(string_operation)
            print("INDEX IN APDCALL " + str(call.previous_index) + " " + str(self.log_manager.highest_index))
            '''if call.previous_index != self.log_manager.highest_index:
                response = AppendEntriesResponse(self.log_manager.current_term, self.log_manager.highest_index, False).to_message()
            elif call.in_term < self.log_manager.current_term:
                 response = AppendEntriesResponse(self.log_manager.current_term + 1, self.log_manager.highest_index, False).to_message()
            else:
            '''
            if True:
                self.election_allowed = False
                self.election_countdown.cancel()
                self.election_countdown = threading.Timer(self.timeout, self.start_election)
                self.election_countdown.start()

                self.leader = False
                if self.heartbeat_timer:
                    self.heartbeat_timer.cancel()
                self.log_manager.current_term = call.in_term

                self.latest_leader = server_name
                self.latest_leader_port = server_port


                log_manager.remove_logs_after_index(call.previous_index)
                [log_manager.write_to_log(log, term_absent=False) for log in call.entries]
                [log_manager.write_to_state_machine(command, term_absent=False) for command in call.entries]
                response = AppendEntriesResponse(self.log_manager.current_term + 1, self.log_manager.highest_index, True).to_message()
                print("State machine after committing: " + str(log_manager.data))

        elif string_operation.split(" ")[0] == "append_entries_response":
            call = AppendEntriesResponse.from_message(string_operation)
            if call.success:
                if self.leader:
                    self.mark_updated(server_name)
                    self.log_manager.current_term = call.term
                send_pending = False
            elif self.log_manager.highest_index != call.index:
                if self.leader:
                    response = AppendEntriesCall(
                        in_term=self.log_manager.current_term,
                        previous_index=call.index - 1,
                        previous_term=self.log_manager.current_term,
                        entries=self.log_manager.get_logs_after_index(call.index - 1)
                    ).to_message()

        elif string_operation.split(" ")[0] == "request_vote_call":
            if self.allow_election:
                request_vote_call = RequestVoteCall.from_message(string_operation)
                if request_vote_call.for_term > self.log_manager.current_term \
                        and request_vote_call.latest_log_term >= self.log_manager.latest_term_in_logs \
                        and request_vote_call.latest_log_index >= self.log_manager.highest_index \
                        and self.voting:

                    response = RequestVoteResponse(self.log_manager.current_term, True).to_message()
                else:
                    response = RequestVoteResponse(self.log_manager.current_term, False).to_message()
            else:
                response = RequestVoteResponse(self.log_manager.current_term, False).to_message()

        elif string_operation.split(" ")[0] == "request_vote_response":
            print("RequestVoteResponse!!!!!!")
            call = RequestVoteResponse.from_message(string_operation)
            if call.voteGranted:
                self.mark_voted(server_name)
                self.log_manager.current_term += 1
                send_pending = False
        else:
            if self.leader:
                self.current_operation = string_operation

                if self.current_operation.split(" ")[0] in ["set", "delete"]:
                    log_manager.write_to_state_machine(string_operation, term_absent=True)
                    string_operation_with_term = log_manager.write_to_log(string_operation, term_absent=True)
                    print(string_operation_with_term)
                    self.broadcast(self, self.with_return_address(
                        self,
                        AppendEntriesCall(
                            in_term=self.log_manager.current_term,
                            previous_index=self.log_manager.highest_index - 1,
                            previous_term=self.log_manager.latest_term_in_logs,
                            entries=[string_operation_with_term]
                        ).to_message()
                    ))

                    response = "OK leader!"
                else:
                    response = log_manager.read(self.current_operation)
            else:
                response = "I am not the leader. The last leader I heard from is " + str(
                    self.latest_leader) + " [" + str(self.latest_leader_port) + "],"

        if send_pending:
            response = self.with_return_address(self, response)

        return server_name, server_port, response
