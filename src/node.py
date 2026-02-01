import socket
import threading
import argparse
from utils import send_msg, recv_msg

class DataNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.store = {}  # The key-value store
        self.locks = {}  # Map key -> lock owner (transaction_id) for 2PL
        self.lock_obj = threading.Lock() # Protects access to store and locks
        
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def start(self):
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(5)
        print(f"DataNode listening on {self.host}:{self.port}")
        
        while True:
            conn, addr = self.server_sock.accept()
            t = threading.Thread(target=self.handle_client, args=(conn, addr))
            t.start()

    def handle_client(self, conn, addr):
        print(f"Accepted connection from {addr}")
        try:
            while True:
                req = recv_msg(conn)
                if req is None:
                    break
                
                resp = self.process_request(req)
                send_msg(conn, resp)
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()

    def process_request(self, req):
        """
        Request format: { "cmd": <COMMAND>, "key": <KEY>, "value": <VALUE>, "tx_id": <ID> }
        """
        cmd = req.get("cmd")
        key = req.get("key")
        value = req.get("value")
        tx_id = req.get("tx_id")
        
        with self.lock_obj:
            if cmd == "GET":
                return {"status": "OK", "value": self.store.get(key)}
            
            elif cmd == "PUT":
                self.store[key] = value
                return {"status": "OK"}
            
            elif cmd == "DELETE":
                if key in self.store:
                    del self.store[key]
                return {"status": "OK"}
            
            # --- 2PL Locking Commands ---
            elif cmd == "LOCK":
                # Simple exclusive lock attempt
                if key in self.locks and self.locks[key] != tx_id:
                    return {"status": "LOCKED", "owner": self.locks[key]}
                self.locks[key] = tx_id
                return {"status": "OK"}
            
            elif cmd == "UNLOCK":
                if key in self.locks and self.locks[key] == tx_id:
                    del self.locks[key]
                return {"status": "OK"}
            
            else:
                return {"status": "ERROR", "msg": "Unknown command"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Node for Distributed DB")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8001, help="Port to bind to")
    args = parser.parse_args()
    
    node = DataNode(args.host, args.port)
    node.start()
