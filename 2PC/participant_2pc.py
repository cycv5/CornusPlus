import random
import redis
import sys
import time
import socket 
from utils_2pc import *
import threading 
import select

class Participant:
    def __init__(self, participant_port):
        # Connect to Redis (acts as a local DB)
        self.db = redis.Redis(host=HOST, port=REDIS_PORT, decode_responses=True)
        self.port = participant_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((HOST, self.port))
        self.socket.listen(1)  # Listen for Coordinator connections
        print(f"[Participant {self.port}] Waiting for Coordinator...")

        self.conn, self.addr = self.socket.accept()
        print(f"[Participant {self.port}] Connected to Coordinator {self.addr}")

        # Clear the participant log file on first run
        with open(f"participant_{self.port}_log.txt", "w") as log_file:
            log_file.write("")
    
        self.termination_protocol = 'naive'
        # self.termination_protocol = 'cooperative'
        self.pending_transactions = {}  # txn_id -> start_time
        self.timeout_seconds = 10

        threading.Thread(target=self.listen_for_peer_queries, daemon=True).start()
        
        # Recover unfinished transactions
        self.recover_unfinished_transaction()
        
    def listen_for_messages(self):
        """Listens for messages from the Coordinator and responds accordingly."""
        while True:
            # =====================
            # Timeout / Termination Protocol
            # =====================
            to_abort = []
            current_time = time.time()
            for txn_id, start_time in list(self.pending_transactions.items()):
                # print(f"[Participant {self.port}] Waiting for final decision of transaction {txn_id} from coordinator...")
                time.sleep(1)
                if self.termination_protocol == 'naive':
                    continue
                if current_time - start_time > self.timeout_seconds:
                    '''
                    if self.termination_protocol == 'naive':
                        print(f"[Participant {self.port}] Timeout reached for {txn_id} — no coordinator response.")
                        self.abort_transaction(txn_id, STATUS_TIMEOUT_ABORTED)
                    '''
                    print(f"[Participant {self.port}] Timeout for {txn_id}. Querying peers before aborting.")
                    decision = self.query_peers_for_decision(txn_id)
                    if decision == STATUS_COMMITTED:
                        self.commit_transaction(txn_id)
                    elif decision == STATUS_ABORTED:
                        self.abort_transaction(txn_id, STATUS_ABORTED)
                    else:
                        print(f"[Participant {self.port}] No peer knew the decision. Aborting {txn_id}.")
                        self.abort_transaction(txn_id, STATUS_TIMEOUT_ABORTED)
                    to_abort.append(txn_id)

            for txn_id in to_abort:
                self.pending_transactions.pop(txn_id, None)

            # =====================
            # Non-blocking socket read from Coordinator
            # =====================
            try:
                ready_to_read, _, _ = select.select([self.conn], [], [], 0.1)  # 0.1 sec timeout
                if ready_to_read:
                    message = self.conn.recv(1024).decode()
                    if not message:
                        continue  # Ignore empty messages

                    msg_type, txn_id = message.split(":")
                    
                    if self.db.exists("log:" + txn_id) and self.db.get('log:' + txn_id) == STATUS_TIMEOUT_ABORTED:
                        print(f"[Participant {self.port}] Transaction {txn_id} already aborted. Ignore Coordinator.")
                        self.db.delete("log:" + txn_id)
                        continue
                    
                    if msg_type == "VOTE-REQ":
                        self.handle_vote_request(txn_id)

                    elif msg_type == "GLOBAL-COMMIT":
                        self.commit_transaction(txn_id)

                    elif msg_type == "GLOBAL-ABORT":
                        self.abort_transaction(txn_id, STATUS_ABORTED)

            except Exception as e:
                print(f"[Participant {self.port}] Error receiving message: {e}")
                break  # Exit on error
            
                
        # self.conn.close()
        # self.socket.close()
    def handle_vote_request(self, txn_id):
        """Handles the VOTE-REQ message by deciding to vote YES or NO."""
        # vote = random.choice(["VOTE-YES", "VOTE-NO"])  # Simulate decision
        vote = "VOTE-YES"
        # print(f"[Participant {self.port}] Received VOTE-REQ for {txn_id}. Voting {vote}")

        # Store the transaction state in Redis as "PENDING"
        txn_key = f"{TXN_PREFIX}{txn_id}"
        self.db.set(txn_key, "PENDING")
        self.pending_transactions[txn_id] = time.time()

        self.conn.sendall(f"{vote}:{txn_id}".encode())  # Send vote back to Coordinator
        
    def commit_transaction(self, txn_id):
        """Performs the commit operation after Coordinator's decision."""
        # print(f"[Participant {self.port}] COMMITTING Transaction {txn_id} ✅")
        time.sleep(1)  # Simulate commit operation

        # Update transaction status in Redis
        txn_key = f"{TXN_PREFIX}{txn_id}"
        self.db.set(txn_key, STATUS_COMMITTED)

        self.log_decision(txn_id, STATUS_COMMITTED)
        # print(f"[Participant {self.port}] Transaction {txn_id} COMMITTED successfully.")
        
        # Clean up
        self.pending_transactions.pop(txn_id, None)


    def abort_transaction(self, txn_id, status):
        """Rolls back any changes if the transaction is aborted."""
        print(f"[Participant {self.port}] ABORTING Transaction {txn_id} ❌")
        time.sleep(1)  # Simulate rollback operation

        # Update transaction status in Redis
        txn_key = f"{TXN_PREFIX}{txn_id}"
        self.db.set(txn_key, status)

        self.log_decision(txn_id, status)
        print(f"[Participant {self.port}] Transaction {txn_id} ABORTED successfully.")
        
        # Clean up
        self.pending_transactions.pop(txn_id, None)


    def recover_unfinished_transaction(self):
        """Checks Redis for unfinished transactions and recovers them if needed."""
        for key in self.db.scan_iter(f"{TXN_PREFIX}*"):
            status = self.db.get(key)
            txn_id = key.split(":")[-1]
            if status == "PENDING":
                print(f"[Participant {self.port}] Recovering unfinished transaction {txn_id}")
                self.abort_transaction(txn_id, STATUS_ABORTED)  # Default action: Abort

    def log_decision(self, txn_id, decision):
        """Logs the commit/abort decision in Redis and a local file."""
        log_entry = f"Transaction {txn_id}: {decision}\n"

        # Log in Redis
        self.db.set(f"log:{txn_id}", decision)

        # Log in a local file
        with open(f"participant_{self.port}_log.txt", "a") as log_file:
            log_file.write(log_entry)

        print(f"[Participant {self.port}] Logged Transaction {txn_id}: {decision}")

    def listen_for_peer_queries(self):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.bind((HOST, self.port + 100))
        peer_socket.listen(5)

        while True:
            conn, addr = peer_socket.accept()
            with conn:
                message = conn.recv(1024).decode()
                if message.startswith("QUERY:"):
                    txn_id = message.split(":")[1]
                    txn_key = f"{TXN_PREFIX}{txn_id}"
                    status = self.db.get(txn_key)

                    if status in [STATUS_COMMITTED, STATUS_ABORTED, STATUS_TIMEOUT_ABORTED]:
                        reply = f"DECISION:{txn_id}:{status}"
                    else:
                        reply = f"DECISION:{txn_id}:UNKNOWN"

                    conn.sendall(reply.encode())

    def query_peers_for_decision(self, txn_id):
        for peer_port in PARTICIPANT_PORTS:
            if peer_port == self.port:
                continue
            try:
                with socket.create_connection((HOST, peer_port + 100), timeout=2) as peer_conn:
                    peer_conn.sendall(f"QUERY:{txn_id}".encode())
                    response = peer_conn.recv(1024).decode()
                    if response.startswith("DECISION:"):
                        _, _, status = response.split(":")
                        if status in [STATUS_COMMITTED, STATUS_ABORTED]:
                            print(f"[Participant {self.port}] Learned from peer {peer_port}: {txn_id} → {status}")
                            return status
            except Exception as e:
                print(f"[Participant {self.port}] Could not contact peer {peer_port}: {e}")

        return "UNKNOWN"
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python participant_2pc.py <participant_num>")
        sys.exit(1)

    participant_port = PARTICIPANT_PORTS[int(sys.argv[1])]

    participant = Participant(participant_port)
    participant.listen_for_messages()

    
    