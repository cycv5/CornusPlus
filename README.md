# CornusPlus

This project explores the design and implementation of Cornus, an optimized atomic commit protocol for cloud-native databases with storage disaggregation, in particular, with partitioned databases. Traditional Two-Phase Commit (2PC) protocol suffers from high latency due to coordinator logging and possible blocking during coordinator failures. Cornus addresses these limitations by leveraging the capabilities of the highly-available disaggregated storage, eliminating coordinator logging through a novel LogOnce API based on compare-and-swap semantics. This project implements the Cornus protocol in an emulated environment on a single device with a Redis backend database. Coordinator and participants of the protocol are modeled with Python. All parties communicate with each other through network protocol simulating the real-world scenario. We have found up to 1.32x latency reduction compared to the conventional 2PC, which we also implemented in this project for comparison. Cornus+ further improves the Cornus protocol by: (1) expanding the failure recovery analysis, enabling continuous issue of transactions as well as system stall scenarios (in addition to system failure), (2) utilizing the implementation-specific feature of the Python socket so that some transactions can be aborted immediately without waiting, (3) creating a non-blocking exponential back-off Termination Protocol that does not block even during a backend database failure, (4) using the Pub/Sub feature of Redis to implement and test an express commit algorithm, saving network communication and further reducing the running time by 21.9%.

## Running Instructions
Each folder contains a version of the commit protocol, by default 4 participants and 1 coordinator. Ports for Redis and the participants/coordinator are specified in the utils files.
To run:
Install Redis server and run:
```
redis-server --port 6380
```
Start participants with different indices, e.g., 0-3 for 4 participants:
```
python participant.py <participant_index>
```
Start the coordinator:
```
python coordinator.py
```
In the coordinator window, you may enter a 3 digit transaction number to simulate an issued transaction. Time taken for the transaction will be displayed.
