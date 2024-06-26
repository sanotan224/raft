import ast


class AppendEntriesCall:
    def __init__(self, in_term, previous_index, previous_term, entries, leaderCommit):
        self.in_term = in_term
        self.previous_index = previous_index
        self.previous_term = previous_term
        self.entries = entries
        self.leaderCommit = leaderCommit

    def to_message(self):
        return "append_entries_call in_term " \
            + str(self.in_term) + \
            " after " + \
            str(self.previous_index) + " " + \
            str(self.previous_term) + " " + \
            str(self.leaderCommit) + " @" + \
            str(self.entries) + " "

    @classmethod
    def from_message(cls, message):
        context, entries_as_string = message.split("@")
        numeric_markers = context.replace("append_entries_call in_term ", "").split(" ")
        current_term, index, latest_log_term, leaderCommit = (int(numeric_markers[0]), int(numeric_markers[2]),
                                                              int(numeric_markers[3]), int(numeric_markers[4]))
        entries = ast.literal_eval(entries_as_string)

        return AppendEntriesCall(
            in_term=current_term,
            previous_index=index,
            previous_term=latest_log_term,
            entries=entries,
            leaderCommit=leaderCommit
        )
