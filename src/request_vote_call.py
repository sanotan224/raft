
class RequestVoteCall:
    def __init__(self, for_term, candidate_port, latest_log_index, latest_log_term):
        self.for_term = for_term
        self.candidate_port = candidate_port
        self.latest_log_index = latest_log_index
        self.latest_log_term = latest_log_term

    def to_message(self):
        return "request_vote_call " \
                + str(self.for_term) \
                + " ? " \
                + "last_log_entry: " \
                + str(self.latest_log_index) \
                + " " \
                + str(self.latest_log_term) \
                + " " \
                + str(self.candidate_port)

    @classmethod
    def from_message(cls, message):
        numeric_markers = message.replace("request_vote_call ", "").split(" ")
        current_term, candidate_port, index, latest_log_term = int(numeric_markers[0]), int(numeric_markers[5]),  int(numeric_markers[3]), int(numeric_markers[4])

        return RequestVoteCall(
            for_term=current_term,
            candidate_port=candidate_port,
            latest_log_index=index,
            latest_log_term=latest_log_term,
        )
