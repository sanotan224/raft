class AppendEntriesResponse:
    def __init__(self, term, index, success):
        self.term = term
        self.index = index
        self.success = success

    def to_message(self):
        return "append_entries_response " + str(self.term) + " " + str(self.index) + " " + str(self.success)

    @classmethod
    def from_message(cls, message):
        numeric_markers = message.replace("append_entries_response ", "").split(" ")
        term, index, success = int(numeric_markers[0]), int(numeric_markers[1]), bool(numeric_markers[2] == 'True')

        return AppendEntriesResponse(
            term=term,
            index=index,
            success=success
        )
