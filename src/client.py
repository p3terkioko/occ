import socket
import uuid
from utils import send_msg, recv_msg

class DBClient:
    def __init__(self, coordinator_host="localhost", coordinator_port=8000, nodes_config=None):
        self.coord_addr = (coordinator_host, coordinator_port)
        self.nodes_config = nodes_config if nodes_config else []
        self.node_socks = {} # Map node_index -> socket
        
        self.mode = "OCC" # "OCC" or "2PL"
        self.held_locks = set() # Set of keys locked in 2PL
        
        # Transaction State
        self.tx_id = None
        self.read_set = set() # Keys read
        self.write_set = {}   # Keys locally written {key: value}
        self.local_cache = {} # Cache for reads (Read-your-writes)

    def connect_node(self, idx):
        if idx not in self.node_socks:
            host, port = self.nodes_config[idx]
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            self.node_socks[idx] = s
        return self.node_socks[idx]

    def get_node_index(self, key):
        return hash(key) % len(self.nodes_config)

    def begin(self, mode="OCC"):
        """Start a new transaction."""
        self.mode = mode
        if self.mode == "OCC":
            # Contact Coordinator for a start timestamp
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(self.coord_addr)
            send_msg(s, {"cmd": "START_TX"})
            resp = recv_msg(s)
            s.close()
            
            if resp and resp["status"] == "OK":
                self.tx_id = resp["tx_id"]
            else:
                raise Exception("Failed to start transaction")
        elif self.mode == "2PL":
             self.tx_id = str(uuid.uuid4())
        
        # Reset state for both modes
        self.read_set = set()
        self.write_set = {}
        self.local_cache = {}
        self.held_locks = set()
        return self.tx_id

    def read(self, key):
        """Read a value. Returns local write if exists, else fetches from node."""
        if key in self.write_set:
            return self.write_set[key]
        
        if key in self.local_cache:
            return self.local_cache[key]
            
        if self.mode == "2PL":
            # 2PL READ: Must acquire lock first if not already held
            if key not in self.held_locks:
                node_idx = self.get_node_index(key)
                sock = self.connect_node(node_idx)
                send_msg(sock, {"cmd": "LOCK", "key": key, "tx_id": self.tx_id or 0}) # Use 0 or dummy if no tx_id needed yet
                resp = recv_msg(sock)
                if resp["status"] != "OK":
                     # Failed to lock (simplistic: blocking or abort? The node returns LOCKED if held)
                     # For this demo, let's say if we can't get lock, we return None (Abort)
                     # Real 2PL would wait.
                     # Let's Implement WAIT logic in Node or Retry here? 
                     # For simplicity: If LOCKED, we just fail/abort (No-Wait 2PL)
                     return None
                self.held_locks.add(key)

        # READ PHASE: Read from actual node

        # Note: In pure OCC, we read from snapshot or current state.
        # Here we read from current state of Node.
        # We must add to read_set.
        node_idx = self.get_node_index(key)
        sock = self.connect_node(node_idx)
        
        send_msg(sock, {"cmd": "GET", "key": key})
        resp = recv_msg(sock)
        
        if resp and resp["status"] == "OK":
            val = resp["value"]
            self.read_set.add(key)
            self.local_cache[key] = val
            return val
        else:
            return None

    def write(self, key, value):
        """Buffer write locally."""
        if self.mode == "2PL":
             if key not in self.held_locks:
                node_idx = self.get_node_index(key)
                sock = self.connect_node(node_idx)
                send_msg(sock, {"cmd": "LOCK", "key": key, "tx_id": self.tx_id or 0})
                resp = recv_msg(sock)
                if resp["status"] != "OK":
                    return False # Write failed to acquire lock
                self.held_locks.add(key)
        
        self.write_set[key] = value
        self.read_set.add(key) 
        return True

    def commit(self):
        """Attempt to commit the transaction."""
        if self.mode == "2PL":
             # Apply all writes, then release locks
             try:
                 for key, val in self.write_set.items():
                     node_idx = self.get_node_index(key)
                     sock = self.connect_node(node_idx)
                     send_msg(sock, {"cmd": "PUT", "key": key, "value": val})
                     recv_msg(sock) # Consume response
                     
                 return True
             finally:
                 self.unlock_all()

        if not self.write_set: # Read-only optimization
            return True

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.coord_addr)
        
        req = {
            "cmd": "COMMIT_OCC",
            "tx_id": self.tx_id,
            "read_set": list(self.read_set),
            "write_set": self.write_set
        }
        send_msg(s, req)
        resp = recv_msg(s)
        s.close()
        
        return resp and resp.get("status") == "COMMITTED"

    def abort(self):
        """Discard local changes."""
        self.tx_id = None
        self.read_set.clear()
        self.write_set.clear()
        if self.mode == "2PL":
            self.unlock_all()

    def unlock_all(self):
        for key in self.held_locks:
            node_idx = self.get_node_index(key)
            sock = self.connect_node(node_idx)
            send_msg(sock, {"cmd": "UNLOCK", "key": key, "tx_id": self.tx_id or 0})
            recv_msg(sock) # Consume response
        self.held_locks.clear()

    def close(self):
        for s in self.node_socks.values():
            s.close()
