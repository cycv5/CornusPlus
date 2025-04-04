import redis
import socket
import sys
import atexit
import time
from utils_plus import send_message, receive_message, logOnce, write_local, read_local, is_socket_closed, termination_protocol
from utils_plus import HOST, COORDINATOR_PORT, REDIS_PORT, PARTICIPANT_PORTS, NUM_PARTICIPANTS
from utils_plus import TXN_INCOME, VOTE_REQ, VOTE_YES, VOTE_NO, ABORT, COMMIT, MSG_LEN


# --- Participant ---
class Participant:
    def __init__(self, redis_port, participant_port, partition):
        # local db
        self.db = redis.Redis(host=HOST, port=redis_port)

        # If design change to a single redis server, partition can be represented
        # as self.db.set(f"log-{self.port}-{txn}", decision) to differentiate.
        #
        # For pubsub, all channels can subscribe to the same channel name f"txn-{txn}"
        # Then participants will publish their results all on this channel. Every
        # single participant can read all the votes including their own.
        # If the VOTE-YES == NUM_PARTICIPANTS then a commit can be determined without
        # the coordinator.

        self.pubsub = self.db.pubsub(ignore_subscribe_messages=False)

        self.redis_port = redis_port
        self.port = participant_port
        self.partition = partition
        # recover if needed
        retrieved_started_txn = read_local(f"{self.port}_started_txn.txt")
        if retrieved_started_txn:  # unresolved problem
            print(f"Resolving unfinished txn: {retrieved_started_txn}")
            self.recover(retrieved_started_txn)
        # set up network with coordinator
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('localhost', participant_port))
        self.socket.listen()
        self.conn = None

    def kill_participant(self, participant_num):
        port_to_kill = PARTICIPANT_PORTS[participant_num]
        if self.port == port_to_kill:
            exit()

    def stall_participant(self, participant_num, length):
        port_to_stall = PARTICIPANT_PORTS[participant_num]
        if self.port == port_to_stall:
            time.sleep(length)

    def recover(self, retrieved_started_txn):
        if self.db.exists(f"log-{self.partition}-{retrieved_started_txn}"):
            value = self.db.get(f"log-{self.partition}-{retrieved_started_txn}")
            if value == ABORT or value == COMMIT:
                write_local(f"{self.port}_started_txn.txt", "")  # already determined
            else:
                self.terminate(retrieved_started_txn)
        else:
            self.terminate(retrieved_started_txn)

    def terminate(self, txn):
        decision = termination_protocol(self.partition, txn)
        if decision == COMMIT:
            self.db.set(f"{txn}-{self.partition}", 1)  # dummy operation
        print(f"Participant decision for Transaction {txn}: {decision} ")
        self.db.set(f"log-{self.partition}-{txn}", decision)
        write_local(f"{self.port}_started_txn.txt", "")


    def process_txn(self):
        self.conn, self.addr = self.socket.accept()
        with self.conn:
            message = receive_message(self.conn, length=MSG_LEN)
            if not message:
                return
            if message['type'] == TXN_INCOME:
                txn = message['txn_id']
                write_local(f"{self.port}_started_txn.txt", txn)
                self.pubsub.unsubscribe()
                self.pubsub.subscribe(f"resp-{txn}", f"decision-{txn}")
                self.start_cornus(txn)


    def start_cornus(self, txn):
        self.conn.settimeout(3)  # timeout for VOTE_REQ
        try:
            message = receive_message(self.conn, length=MSG_LEN)['type']
        except (socket.timeout, Exception) as e:
            print("Error waiting for VOTE_REQ")
            print(f"Participant decision for Transaction {txn}: {ABORT} ")
            self.db.set(f"log-{self.partition}-{txn}", ABORT)
            write_local(f"{self.port}_started_txn.txt", "")
            return
        
        # socket for reply to coordinator
        socket_coord = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_coord.settimeout(3)
        if message == VOTE_REQ:
            # Simulate local processing and vote
            # ... (Simplified example)
            vote = VOTE_YES  # Or VOTE_NO based on local state
            if vote == VOTE_YES:
                # Log vote using LogOnce()
                resp = logOnce(self.db, self.partition, txn, VOTE_YES)
                # publish own decision
                ret = self.db.publish(f"resp-{txn}", resp)
                # print(f"Published resp to {ret} subscribed channels.")
                
                try:
                    socket_coord.connect(("localhost", COORDINATOR_PORT))
                except (socket.timeout, Exception) as e:
                    self.terminate(txn)
                    socket_coord.close()
                    return
                # print("Connected to Coordinator for reply.")
                if resp == ABORT:
                    send_message(socket_coord, {'type': ABORT, 'txn_id': txn})
                    print("Replied to Coordinator with ABORT, connection closed")
                    write_local(f"{self.port}_started_txn.txt", "")  # delete from started_txn record
                    socket_coord.close()
                    return
                else:  # VOTE_YES

                    # Try get all decisions
                    count = 0
                    msg = self.pubsub.get_message()
                    while msg:
                        if msg['type'] == "message" and msg['channel'].decode("utf-8") == f"resp-{txn}":
                            if msg['data'].decode("utf-8") == VOTE_YES:
                                count += 1
                            elif msg['data'].decode("utf-8") == ABORT:
                                send_message(socket_coord, {'type': ABORT, 'txn_id': txn})
                                write_local(f"{self.port}_started_txn.txt", "")  # delete from started_txn record
                                socket_coord.close()
                                return
                        msg = self.pubsub.get_message()
                    # # print(f"Received count of {count}")
                    if count == NUM_PARTICIPANTS:
                        send_message(socket_coord, {'type': COMMIT, 'txn_id': txn})
                        self.db.publish(f"decision-{txn}", COMMIT)
                        self.db.set(f"{txn}-{self.partition}", 1)  # dummy
                        # print(f"Participant express commmit Transaction {txn}: {COMMIT}")
                        self.db.set(f"log-{self.partition}-{txn}", COMMIT)
                        write_local(f"{self.port}_started_txn.txt", "")
                        return
                    else:
                        send_message(socket_coord, {'type': VOTE_YES, 'txn_id': txn})
                        # print("Replied to Coordinator with VOTE_YES, waiting for final decision")
                    
                    decision = ""
                    start_time = time.time()
                    self.conn.setblocking(False)
                    while (not decision) and (time.time() - start_time < 3):
                        try:
                            msg = self.pubsub.get_message()
                            if msg and msg['type'] == "message" and msg['channel'].decode("utf-8") == f"decision-{txn}":
                                if msg['data'].decode("utf-8") == COMMIT:
                                    self.db.set(f"{txn}-{self.partition}", 1)  # dummy
                                    # print(f"Fast participant decision for Transaction {txn}: {COMMIT} ")
                                    self.db.set(f"log-{self.partition}-{txn}", COMMIT)
                                    write_local(f"{self.port}_started_txn.txt", "")
                                    return
                            decision = receive_message(self.conn, length=MSG_LEN)['type']
                        except BlockingIOError:
                            continue
                        except TypeError:
                            break
                    self.conn.setblocking(True)
                    
                    if decision == "":
                        print("Timeout or error waiting for decision, termination protocol starts")
                        decision = termination_protocol(self.partition, txn)
                    
                    if decision == COMMIT:
                        self.db.set(f"{txn}-{self.partition}", 1)  # dummy operation
                    print(f"Participant decision for Transaction {txn}: {decision} ")
                    self.db.set(f"log-{self.partition}-{txn}", decision)
                    write_local(f"{self.port}_started_txn.txt", "")  # delete from started_txn record
                socket_coord.close()
            else: # VOTE_NO
                # Switch the order from the paper for async-like performance
                # coordinator gets the abort message asap while the participant
                # deal with it afterwards.
                try:
                    socket_coord.connect(("localhost", COORDINATOR_PORT))
                    # print("Connected to Coordinator for reply.")
                    send_message(socket_coord, {'type': ABORT, 'txn_id': txn})
                    socket_coord.close()
                except socket.timeout:
                    socket_coord.close()

                print("Replied to Coordinator with ABORT (VOTE_NO), connection closed")
                print(f"Participant decision for Transaction {txn}: {ABORT} ")
                self.db.set(f"log-{self.partition}-{txn}", ABORT)
                write_local(f"{self.port}_started_txn.txt", "")  # delete from started_txn record


if __name__ == '__main__':
    # python participant.py 0
    participant_num = int(sys.argv[1])
    port_redis = REDIS_PORT
    port_participant = PARTICIPANT_PORTS[participant_num]
    p = Participant(port_redis, port_participant, participant_num)
    while True:
        p.process_txn()
