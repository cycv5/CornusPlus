import redis
import socket
import json
import time
import threading, queue

# --- Configuration ---
HOST = 'localhost'
COORDINATOR_PORT = 6000  # Port for coordinator communication

# Unique ports for each Redis instance and participant communication
REDIS_PORT = 6380  
PARTICIPANT_PORTS = [7000, 7001, 7002, 7003] 
NUM_PARTICIPANTS = len(PARTICIPANT_PORTS)

# Cornus Log Types, same length messages for consistency in reading
TXN_INCOME = 'TXN_INC'
VOTE_REQ = 'VOTE_RQ'
VOTE_YES = 'VOTE_YE'
VOTE_NO = 'VOTE_NO'
ABORT = 'TXN_ABT'
COMMIT = 'TXN_CMT'
# note txn are 3 btyes long, so messages are fixed length
# e.g. {'type': 'TXN_INC', 'txn_id': '001'} <- 36 btyes
MSG_LEN = 36

# CAS Lua Script - not used
CAS_SCRIPT = """
local current_value = redis.call('GET', KEYS[1])
if current_value == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2])
  return ARGV[2]
else
  return current_value
end
"""

def send_message(s, message):
    msg = json.dumps(message).encode()
    s.sendall(msg)
    # print(f"Sending {message}")

def receive_message(conn, length=1024):
    data = conn.recv(length).decode()
    if not data:
        return None  # connection closed
    ret = json.loads(data)
    return ret

def logOnce(db, partition, txn, new_value):
    success = db.set(f"log-{partition}-{txn}", new_value, nx=True)  # set if not exist
    if success:
        # print("LogOnce Success!")
        return new_value
    else:
        # print("LogOnce key already exists")
        return db.get(f"log-{partition}-{txn}").decode('utf-8')
    # return self.cas(keys=[txn], args=[expected_value, new_value])

def is_socket_closed(sock: socket.socket):
    try:
        # this will try to read bytes without blocking and also without removing them from buffer (peek only)
        data = sock.recv(16, socket.MSG_DONTWAIT | socket.MSG_PEEK)
        if not data:
            return True
    except BlockingIOError:
        return False  # socket is open and reading from it would block
    except ConnectionResetError:
        return True  # socket was closed for some other reason
    except Exception as e:
        return False
    return False

def write_local(filename, data):
    with open(filename, "w") as file:
        file.write(data)

def read_local(filename):
    try:
        with open(filename, "r") as file:
            return file.read()
    except FileNotFoundError:
        return ""

def termination_protocol(own_partition, txn, coord=False, retry=0):
        if retry == 3:
            print("Termination Protocol Retry Limit Reached. ABORT.")
            return ABORT
        print("Termination Protocol Initiated.")
        result_queue = queue.Queue()  # Queue to store results from threads
        # Asynchronously set ABORT in other participants' Redis instances
        db = redis.Redis(host=HOST, port=REDIS_PORT)
        def log_abort_async(db, partition):
            try:
                ret = logOnce(db, partition, txn, ABORT)
                result_queue.put({'partition': partition, 'result': ret})
            except Exception as e:
                result_queue.put({'partition': partition, 'result': f'error: {e}'})

        threads = []
        for part in range(NUM_PARTICIPANTS):
            if part != own_partition:
                thread = threading.Thread(target=log_abort_async, args=(db, part), daemon=False)
                thread.start()
                threads.append(thread)

        # Wait for responses or timeout (with non-blocking check for results)
        start_time = time.time()
        timeout = (2 ** retry) + 1  # exponential backoff
        vote_yes_resp = 0  # Count total VOTE_YES
        while time.time() - start_time < timeout:
            try:
                result = result_queue.get_nowait()  # Try to get a result from the queue
                res_result = result['result']
                if res_result == ABORT:
                    return ABORT  # seen an ABORT, return immediately. Threads keep running.
                elif res_result == COMMIT:
                    return COMMIT
                elif res_result == VOTE_YES:
                    print(f"Partition {result['partition']}, result {res_result}")
                    vote_yes_resp += 1
                    if (not coord and vote_yes_resp == (NUM_PARTICIPANTS - 1)) or \
                        (coord and vote_yes_resp == NUM_PARTICIPANTS):
                        # VOTE_YES from all other participants
                        print("Commit through concensus")
                        return COMMIT
            except queue.Empty:
                pass  # No results available yet

        # Timeout
        return termination_protocol(own_partition, txn, coord=coord, retry=retry+1)  # Retry, could block