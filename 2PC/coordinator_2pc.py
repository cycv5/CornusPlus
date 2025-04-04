import redis
import time
import socket
import sys
from utils_2pc import *

class Coordinator:
    def __init__(self, participant_ports, coordinator_port):
        self.participant_ports = participant_ports
        self.coordinator_port = coordinator_port
        self.participants_sockets = {}
        self.coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.coordinator_socket.bind(("localhost", COORDINATOR_PORT))
        self.coordinator_socket.listen()
        
        # Clear the coordinator log file on first run
        with open(f"coordinator_log.txt", "w") as log_file:
            log_file.write("") 
    
        print(f"[Coordinator] Listening on port {self.coordinator_port}")

        # Attempt to connect to participants at startup
        self.connect_to_participants()

        
    def connect_to_participants(self, retries=3, delay=2):
        """Tries to establish connections with participants, with retries."""
        for port in self.participant_ports:
            for attempt in range(retries):
                p_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                p_s.settimeout(5)  # Prevent infinite waiting
                
                if port in self.participants_sockets:
                    self.participants_sockets[port].close()  # Close old connection
                
                try:
                    p_s.connect(('localhost', port))  # Try to connect
                    self.participants_sockets[port] = p_s
                    print(f"[Coordinator] Connected to Participant on port {port}")
                    break  # Exit retry loop if successful
                
                except socket.timeout:
                    print(f"[Coordinator] Attempt {attempt+1}/{retries} - Timeout connecting to {port}")
                
                except ConnectionRefusedError:
                    print(f"[Coordinator] Attempt {attempt+1}/{retries} - Connection refused on {port}")

                # Wait before retrying
                time.sleep(delay)  
            
            # If we failed all retries, remove the participant
            if port not in self.participants_sockets:
                print(f"[Coordinator] Failed to connect to Participant {port} after {retries} retries.")
        
        if len(self.participants_sockets) == 0:
            print("Failed to connect to all participants. Closing Coordinator...")
            sys.exit()

    def start_transaction(self, txn_id):
        """Begins a 2PC transaction."""
        print(f"\n[Coordinator] Starting Transaction {txn_id}")
        self.send_prepare_request(txn_id)
        votes = self.collect_votes(txn_id)
        decision = self.evaluate_votes(votes)
        # sys.exit()    # Simulate failure before sending final decisions
        # time.sleep(20)
        self.send_final_decision(txn_id, decision)
        
        # Log the decision
        self.log_decision(txn_id, decision)
    
    def send_prepare_request(self, txn_id):
        """Send VOTE-REQ to all Participants."""
        # print(f"[Coordinator] Sending VOTE-REQ for Transaction {txn_id}")
        for port, socket in self.participants_sockets.items():
            try:
                message = f"VOTE-REQ:{txn_id}"
                socket.sendall(message.encode())
                # print(f"[Coordinator] Sent VOTE-REQ to Participant {port}")
            except Exception as e:
                print(f"[Coordinator] Error sending VOTE-REQ to {port}: {e}")
        
    def collect_votes(self, txn_id, timeout=5):
        """Collect votes from Participants."""
        # print(f"[Coordinator] Collecting votes for Transaction {txn_id}")
        votes = {}
        deadline = time.time() + timeout

        while time.time() < deadline and len(votes) < len(self.participants_sockets):
            for port, socket in self.participants_sockets.items():
                # Check if we already received a vote
                if port not in votes:  
                    try:
                        socket.settimeout(1)  # Short timeout for each vote
                        response = socket.recv(1024).decode()
                        vote_type, received_txn_id = response.split(":")
                        if received_txn_id == txn_id:
                            votes[port] = vote_type
                            # print(f"[Coordinator] Received {vote_type} from Participant {port}")
                    except socket.timeout:
                        continue  # Keep checking for other votes
                    except Exception as e:
                        print(f"[Coordinator] Error receiving vote from {port}: {e}")
        
        return votes
    
    
    def evaluate_votes(self, votes):
        for vote in votes.values():
            if vote != "VOTE-YES":
                return 'GLOBAL-ABORT'
        return 'GLOBAL-COMMIT'
    
    def send_final_decision(self, txn_id, decision):
        """Broadcasts final COMMIT/ABORT decision."""
        # print(f"[Coordinator] Sending FINAL DECISION: {decision} for Transaction {txn_id}")
        for port, socket in self.participants_sockets.items():
            try:
                message = f"{decision}:{txn_id}"
                socket.sendall(message.encode())
                # print(f"[Coordinator] Sent {decision} to Participant {port}")
                # break # For simulating cooperative termination protocol, make one participant not receive decision
            except Exception as e:
                print(f"[Coordinator] Error sending final decision to {port}: {e}")
    
    
    def log_decision(self, txn_id, decision):
        """Logs the final decision to a file for record-keeping."""
        log_entry = f"Transaction {txn_id}: {decision}\n"
        
        with open("coordinator_log.txt", "a") as log_file:
            log_file.write(log_entry)
        
        print(f"[Coordinator] Logged: {log_entry.strip()}")
        
if __name__ == "__main__":
    coordinator = Coordinator(PARTICIPANT_PORTS, COORDINATOR_PORT)
    while True:
        txn_id = input("Enter a new transaction ID (3 digits, e.g., 001): ")
        if len(txn_id) == 3 and txn_id.isdigit():
            start = time.time()
            coordinator.start_transaction(txn_id)
            '''
            with open("participant_7001_log.txt", "r") as file:
                # Go to the end of the file
                file.seek(0, 2)

                while True:
                    print(1)
                    line = file.readline()
                    if not line:
                        time.sleep(0.1)  # Wait for new data
                        continue
                    #print(f"New line: {line.strip()}")
                    txn = "Transaction " + txn_id
                    if txn in line.strip():
                        break
                    
            with open("participant_7002_log.txt", "r") as file:
                # Go to the end of the file
                file.seek(0, 2)

                while True:
                    print(2)
                    line = file.readline()
                    if not line:
                        time.sleep(0.1)  # Wait for new data
                        continue
                    #print(f"New line: {line.strip()}")
                    txn = "Transaction " + txn_id
                    if txn in line.strip():
                        break
            '''
            
            end = time.time()
            print(f"[Coordinator] Total time: {end - start}s")
        else:
            print("Invalid transaction ID! Must be exactly 3 digits.")