import struct
import json
import socket

def send_msg(sock, msg):
    """
    Sends a JSON-serializable message over the socket.
    Format: [4-byte length prefix][JSON bytes]
    """
    msg_json = json.dumps(msg).encode('utf-8')
    msg_len = struct.pack('>I', len(msg_json))
    sock.sendall(msg_len + msg_json)

def recv_msg(sock):
    """
    Receives a JSON message from the socket.
    Returns the parsed Python object or None if connection closed.
    """
    # Read message length (4 bytes)
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    
    # Read the message data
    raw_msg = recvall(sock, msglen)
    if not raw_msg:
        return None
    
    return json.loads(raw_msg.decode('utf-8'))

def recvall(sock, n):
    """Helper to receive exactly n bytes."""
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data
