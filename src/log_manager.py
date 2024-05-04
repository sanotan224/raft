import threading
import os
from src.log_data_access_object import LogDataAccessObject


class LogManager:
    client_lock = threading.Lock()

    def __init__(self, server_name, otherServers={}):
        self.server_name = server_name
        self.data = {}
        self.server_cluster = otherServers
        self.voted_for_me = {}
        self.log = []
        self.catch_up_successful = False
        self.current_term = 0
        self.latest_term_in_logs = 0
        self.highest_index = 1
        self.log_recently_changed = False

    def get(self, key):
        return self.data.get(key, '')

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        del self.data[key]

    def remove_logs_after_index(self, index, path_to_logs=''):
        if path_to_logs == '':
            path_to_logs = "logs/" + self.server_name + "_log.txt"

        if os.path.exists(path_to_logs):
            f = open(path_to_logs, "r+")
            log_list = f.readlines()

            while '' in log_list:
                log_list.remove('')
            f.seek(0)
            for line_to_keep in log_list[0:index + 1]:
                f.write(line_to_keep)
            f.truncate()
            f.close()
            self.highest_index = index
            self.log_recently_changed = True

    def get_logs_after_index(self, index, path_to_logs=''):
        if path_to_logs == '':
            path_to_logs = "logs/" + self.server_name + "_log.txt"

        if os.path.exists(path_to_logs):
            f = open(path_to_logs, "r+")
            log_list = f.readlines()
            while '' in log_list:
                log_list.remove('')
            f.close()
            logList = [' '.join(string.rstrip("\n").split(" ")[2:]) for string in log_list[index + 1:]]
            return logList


    def log_access_object(self, path_to_logs=''):
        if not self.log_recently_changed:
            pass
        else:
            as_dict = {}
            ordered_logs = []
            if path_to_logs == '':
                path_to_logs = "logs/" + self.server_name + "_log.txt"

            if os.path.exists(path_to_logs):
                f = open(path_to_logs, "r")
                log = f.read()
                f.close()

                for command in log.split('\n'):
                    if command != '':
                        operands = command.split(" ")

                        as_dict[" ".join(operands[:2])] = " ".join(operands)
                        ordered_logs.append(" ".join(operands[:2]))

            self.cached_log_access_object = LogDataAccessObject(array=ordered_logs, dict=as_dict)
            self.log_recently_changed = False

        return self.cached_log_access_object

    def command_at(self, previous_index, previous_term):
        return self.log_access_object().term_indexed_logs. \
            get(str(previous_index) + " " + str(previous_term))

    def get_latest_term(self):
        if not self.catch_up_successful:
            print("This store isn't caught up, so it doesn't know what term we're in!")
            return

        return self.latest_term_in_logs

    def write_to_state_machine(self, string_operation, term_absent):
        if len(string_operation) == 0:
            return
            
        print("Writing to state machine: " + string_operation)
        if term_absent:
            string_operation = str(self.highest_index) + " " + str(self.latest_term_in_logs) + " " + string_operation

        operands = string_operation.split(" ")
        index, term, command, key, values = 0, 1, 2, 3, 4

        response = "Sorry, I don't understand that command."

        with self.client_lock:
            self.latest_term_in_logs = int(operands[term])

            if operands[command] == "set":
                value = " ".join(operands[values:])
                if not self.log.__contains__(string_operation):
                    self.log.append(string_operation)
                    self.set(operands[key], value)
                    response = f"key {operands[key]} set to {value}"
            elif operands[command] == "delete":
                if not self.log.__contains__(string_operation):
                    self.log.append(string_operation)
                    self.delete(operands[key])
                    response = f"key {key} deleted"
            else:
                pass

        print("WRITE TO STATE " + str(string_operation))
        print("DATA " + str(self.data))
        return response

    def read(self, string_operation):
        print(string_operation)
        if len(string_operation) == 0:
            return
        operands = string_operation.split(" ")
        command, key, values = 0, 1, 2
        response = "Sorry, I don't understand that command."
        with self.client_lock:
            if operands[command] == "get":
                response = self.get(operands[key])
            elif operands[command] == "show":
                response = str(self.data)
            else:
                pass
        return response

    def write_to_log(self, string_operation, term_absent, path_to_logs=''):
        print("Writing to log: " + string_operation)
        self.log_recently_changed = True
        if len(string_operation) == 0:
            return ''
        operands = string_operation.split(" ")
        if term_absent:
            command, key, values = 0, 1, 2
        else:
            index, term, command, key, values = 0, 1, 2, 3, 4
        if operands[command] in ["set", "delete"]:
            if term_absent:
                self.highest_index += 1
                string_operation = str(self.highest_index) + " " + str(self.current_term) + " " + string_operation
            else:
                self.highest_index = index + 1
                self.latest_term_in_logs = term
            if path_to_logs == '':
                path_to_logs = "logs/" + self.server_name + "_log.txt"
            f = open(path_to_logs, "a+")
            f.write(string_operation + '\n')
            f.close()
            self.latest_term_in_logs = self.current_term
            return string_operation
        return ''

    def clearFile(self, path_to_logs=''):
        if path_to_logs == '':
            path_to_logs = "logs/" + self.server_name + "_log.txt"
        f = open(path_to_logs, "w")
        f.close()

    def destination_addresses(self, server_name):
        other_servers = {k: v for (k, v) in self.server_cluster.items() if k != server_name}
        return list(other_servers.values())

    def other_server_names(self, server_name):
        other_servers = {k: v for (k, v) in self.server_cluster.items() if k != server_name}
        return list(other_servers.keys())
