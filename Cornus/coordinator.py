import socket
import queue
import threading
import select
import time
import atexit
from utils import send_message, receive_message, logOnce, write_local, read_local, termination_protocol
from utils import HOST, COORDINATOR_PORT, REDIS_PORT, PARTICIPANT_PORTS, NUM_PARTICIPANTS
from utils import TXN_INCOME, VOTE_REQ, VOTE_YES, VOTE_NO, ABORT, COMMIT, MSG_LEN

# --- Coordinator ---
class Coordinator:
    def __init__(self, participant_ports, coordinator_port):
        self.participant_ports = participant_ports
        self.coordinator_port = coordinator_port
        self.participants_sockets = {}
        self.coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.coordinator_socket.bind(("localhost", COORDINATOR_PORT))
        self.coordinator_socket.listen()
        # The participants are consistent without coordinator, no recovery needed for data


    def start_txn(self, txn):
        for port in self.participant_ports:
            p_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if port in self.participants_sockets:
                self.participants_sockets[port].close()  # close previous
            self.participants_sockets[port] = p_s
            p_s.settimeout(5)
            try:
                p_s.connect(('localhost', port))  # assumes localhost for this emulation
            except socket.timeout:
                print(f"Cannot connect to participant on {port}")
                print(f"Transaction {txn}: {ABORT}")
                self.participants_sockets.pop(port)
                p_s.close()
                return ABORT
            try:
                send_message(p_s, {'type': TXN_INCOME, 'txn_id': txn})
            except (socket.error, OSError):  # participant dead or recovered from death
                print(f"Send to participant on {port} failed")
                print(f"Transaction {txn}: {ABORT}")
                self.participants_sockets.pop(port)
                p_s.close()
                return ABORT

        # starting Cornus
        start_time = time.time()
        ret = self.start_cornus(txn)
        time_taken = time.time() - start_time
        print(f"Transaction {txn}: {ret}. Time Taken: {(time_taken*1000):.4f}ms")

        return ret


    def start_cornus(self, txn):
        # Case 1 failure
        # time.sleep(10)

        # Use thread to Send VOTE-REQ to all participants
        for socket in self.participants_sockets.values():
            thread = threading.Thread(target=send_message, args=(socket, {'type': VOTE_REQ, 'txn_id': txn}), daemon=False)
            thread.start()
            # Case 2 failure

        # Case 3 failure

        # Paritipants wanting to connect will be blocked if not selected
        # Coordinator chooses ready participants and read the results
        # Note that participant could have sent the msg before the 
        # coordinator here is ready to recv - but this will be handled
        # by OS network stack where the sender will have a buffer and
        # send it when recv ready.
        sockets_list = [self.coordinator_socket]
        start_time = time.time()
        timeout = 3  # Set a timeout value
        vote_yes_resp = 0  # Count total VOTE_YES
        decision = ""
        while time.time() - start_time < timeout:
            # Use select to monitor sockets for readability
            read_sockets, _, _ = select.select(sockets_list, [], [], 1)

            for notified_socket in read_sockets:
                if notified_socket == self.coordinator_socket:  # New connection
                    client_socket, client_address = self.coordinator_socket.accept()
                    sockets_list.append(client_socket)
                    # print(f"Accepted connection from {client_address}")
                else:  # Existing connection
                    try:
                        message = receive_message(notified_socket, length=MSG_LEN)
                        if not message or message['txn_id'] != txn:
                            continue  # notification for connection closing, nothing to read
                        elif message['type'] == VOTE_YES:
                            vote_yes_resp += 1
                            if vote_yes_resp == NUM_PARTICIPANTS:
                                decision = COMMIT
                                break
                        else:
                            decision = ABORT
                            break
                    except (Exception) as e:
                        print("Error receiving votes, ABORT.")
                        sockets_list.remove(notified_socket)
                        notified_socket.close()
                        decision = ABORT
                        break
            if decision:
                break
        
        if decision == "":  # timed out waiting, no results
            decision = termination_protocol(-1, txn, coord=True)

        # decision made

        # Broadcast decision async
        for socket in self.participants_sockets.values():
            threading.Thread(target=send_message, args=(socket, {'type': decision, 'txn_id': txn}), daemon=False).start()
            # Case 4 failure

        # close temp client socket for receiving votes
        for s in sockets_list:
            if s != self.coordinator_socket:  # coordinator socket still needed for next txn
                s.close()

        return decision

def cleanup(c):
    print("Program exit - Clean up")
    for s in c.participants_sockets.values():
        s.close()
    c.coordinator_socket.close()

if __name__ == '__main__':
    c = Coordinator(PARTICIPANT_PORTS, COORDINATOR_PORT)
    # Cleanup at the end
    atexit.register(cleanup, c)
    # Start a new txn
    while True:
        txn = input("Enter a new transaction number (must be 3 digits, e.g., 001):\n")
        if len(txn) == 3:
            # can dynamically assign new participant ports for the txn
            # c.participant_ports = [...]
            # need to modify start_txn to account for new ports every run
            ret = c.start_txn(txn)
        else:
            print("Transaction number must be 3 digits")
