
class RequestVoteResponse:
    def __init__(self, term, voteGranted):
        self.term = term
        self.voteGranted = voteGranted

    def to_message(self):
        return "request_vote_response " + str(self.term) + " " + str(self.voteGranted)

    @classmethod
    def from_message(cls, message):
        numeric_markers = message.replace("request_vote_response ", "").split(" ")
        term, voteGranted = int(numeric_markers[0]), bool(numeric_markers[1] == 'True')

        return RequestVoteResponse(term, voteGranted)
