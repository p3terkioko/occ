import socket
import threading
import argparse
import time
from utils import send_msg, recv_msg

class TransactionManager:
    def __init__(self, host, port, nodes):
        self.host = host
        self.port = port
        self.nodes_config = nodes # List of (host, port) tuples
        self.nodes = [] # List of sockets connected to nodes
        
        # OCC State
        self.global_ts = 0
        self.ts_lock = threading.Lock()
        
        # History of committed transactions for backward validation
        # Format: { commit_ts: { "write_set": [keys] } }
        self.committed_txs = [] 
        self.history_lock = threading.Lock()

        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def connect_to_nodes(self):
        """Establish connections to all data nodes."""
        for host, port in self.nodes_config:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((host, port))
                self.nodes.append(s)
                print(f"Connected to node {host}:{port}")
            except Exception as e:
                print(f"Failed to connect to node {host}:{port} - {e}")

    def get_node_index(self, key):
        """Simple hash-based sharding."""
        return hash(key) % len(self.nodes)

    def start(self):
        self.connect_to_nodes()
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(10)
        print(f"Coordinator listening on {self.host}:{self.port}")
        
        while True:
            conn, addr = self.server_sock.accept()
            t = threading.Thread(target=self.handle_client, args=(conn, addr))
            t.start()

    def handle_client(self, conn, addr):
        try:
            while True:
                req = recv_msg(conn)
                if req is None:
                    break
                
                cmd = req.get("cmd")
                if cmd == "START_TX":
                    with self.ts_lock:
                        start_ts = self.global_ts
                    send_msg(conn, {"status": "OK", "tx_id": start_ts})
                
                elif cmd == "COMMIT_OCC":
                    # req: {cmd, tx_id, read_set, write_set_diff}
                    # write_set_diff: {key: value}
                    self.handle_commit_occ(conn, req)
                
                else:
                    send_msg(conn, {"status": "ERROR", "msg": "Unknown command"})
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()

    def handle_commit_occ(self, conn, req):
        start_ts = req["tx_id"]
        read_set = set(req["read_set"])
        write_set_diff = req["write_set"] # Dictionary {key: val}
        write_keys = set(write_set_diff.keys())
        
        with self.history_lock:
            # VALIDATION PHASE
            # Check if any transaction committed AFTER start_ts has modified any key in our read_set
            # Implementation of Backward Validation
            valid = True
            for tx in self.committed_txs:
                if tx["commit_ts"] > start_ts:
                    # Check intersection
                    if not read_set.isdisjoint(tx["write_keys"]):
                        valid = False
                        break
            
            if valid:
                # WRITE PHASE
                # 1. Update global timestamp
                with self.ts_lock:
                    self.global_ts += 1
                    commit_ts = self.global_ts
                
                # 2. Apply writes to nodes
                success = self.apply_writes(write_set_diff, commit_ts)
                
                if success:
                     # 3. Add to history
                    self.committed_txs.append({
                        "commit_ts": commit_ts,
                        "write_keys": write_keys
                    })
                    # Prune history? (Optional optimization, skip for now or keep last N)
                    send_msg(conn, {"status": "COMMITTED", "tx_id": commit_ts})
                else:
                     # If writing to nodes failed, we have a system error, but for OCC logical validity it's tricky.
                     # Assuming node writes don't fail typically.
                     send_msg(conn, {"status": "ERROR", "msg": "Node write failure"})
            else:
                send_msg(conn, {"status": "ABORTED"})

    def apply_writes(self, write_dict, commit_ts):
        """Distribute writes to appropriate nodes."""
        # Simple stop-and-wait for each write (could be parallelized)
        for key, value in write_dict.items():
            node_idx = self.get_node_index(key)
            node_sock = self.nodes[node_idx]
            msg = {
                "cmd": "PUT",
                "key": key,
                "value": value,
                "tx_id": commit_ts
            }
            try:
                send_msg(node_sock, msg)
                resp = recv_msg(node_sock)
                if resp.get("status") != "OK":
                    return False
            except:
                return False
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--nodes", type=str, required=True, help="Comma-separated list of host:port for nodes")
    args = parser.parse_args()
    
    # Parse nodes config
    nodes_list = []
    for s in args.nodes.split(","):
        h, p = s.split(":")
        nodes_list.append((h, int(p)))
        
    coordinator = TransactionManager("0.0.0.0", args.port, nodes_list)
    coordinator.start()
