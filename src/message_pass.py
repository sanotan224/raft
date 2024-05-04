def send_message(sock, msg):
    send_size(sock, len(msg))
    sock.sendall(msg)

def receive_message(sock):
    size = receive_size(sock)
    try:
        msg = receive_exactly(sock, size)
    except IOError as e:
        raise IOError("Exception occurred in receive_message: " + str(e)) from e
    return msg


def send_size(sock, size):
    sock.sendall(size.to_bytes(8, "big"))


def receive_size(sock):
    try:
        message = receive_exactly(sock, 8)
    except IOError as e:
        raise IOError("Exception occurred in receive_size: " + str(e)) from e

    return int.from_bytes(message, "big")


def receive_exactly(sock, nbytes):
    msg = b''
    while nbytes > 0:
        chunk = sock.recv(nbytes)
        msg += chunk
        nbytes -= len(chunk)
    return msg
