# Core Principles

There are certain constraints that must be respected by the application core implementation. Apart from that there is
significant freedom in the way how to implement it and enough room for imagination.

## Principle 1: Must be deterministic

Applications cores are Finite State Machines from Raft perspective. When a message is committed Raft applies this 
message to its FSM, on each node independently. Also, when a new node joins a Raft group (or a node is far behind on the 
committed log) the node is being restored from a snapshot and replaying all other messages that happened after the 
snapshot was taken. Therefore, the same sequence of messages must transition all machines to exactly the same state 
(even days after they actually happened). 

Practically that means there should be no `time.Now()`, `math/rand` or anything like that in the code of the 
application. Current time should be passed from outside in the message along with other inputs. Random values such as 
ids and crypto keys also should be passed as inputs. There are also time-based tools in some popular libraries, such as
TTL on keys in embedded databases or cache libraries. Those must be avoided. If there is a need in expiration or garbage
collection of data inside a core, it should be implemented controllably. All time-based operation should be performed
with time provided from outside. Also, be aware of maps and other datastructures that have non-deterministic iteration
order.

## Principle 2: Must have no side effects

Application cores must not make requests to external services or emit messages. Its job is to take an input and modify
its state based on that input and its current state, and nothing else. There are several reasons for that:

* No error handling from calling an external service (see Principle 3).
* Non-deterministic behavior based on the state of the external service (see Principle 1).
* The same message is applied to all nodes in the group, so side effects would be executed several times.
* Raft can replay messages after recovering from a snapshot, so side effects would be also replayed.

Writing to a local log file or collecting a metric for development purposes is ok. Those are not parts of the business 
logic.

## Principle 3: Must explode on internal errors

Raft is not designed to handle errors from FSMs. After a message is committed it must be applied to the FSM. If FSM fails
to apply there is no backpressure, or intermediate state, or any other mechanism to handle that error. If FSM fails to 
apply this Raft node is declared dead.

Depending on the implementation of an application core there only few valid reasons to fail: out of memory errors,
disk out of space errors, etc. Application level errors (i.e. object not found, invalid request, etc.) should be 
returned from FSM as a part of the response.

## Principle 4: Must allow sequential writes and parallel reads

Raft applies messages to its FSM in a single thread. However, Monstera performs reads bypassing Raft directly on the 
application core, for performance reasons. Therefore, writes are always sequential, and reads can happen in parallel
with writes or other reads.

## Principle 5: Must take snapshots concurrently

Snapshotting is performed in two steps. First, `Snapshot()` is called to take a snapshot of the current state. Then, 
this snapshot is written to a sink. Snapshot creation is happening between FSM writes, but actual snapshot writing 
to a sink happens in parallel with other FSM writes. This way large snapshots or slow sinks do not interrupt FSM updates. 
Implementation of `Snapshot()` must be fast, typically it is a copy operation for copy-on-write data structures, or a 
new transaction in database with `snapshot` isolation level or higher. 

## Principle 6: Must not change behavior with code changes

When a Monstera cluster is deployed each node is restarted one by one to keep the cluster available. That means there will 
be two versions of the code running at the same time and two different versions of FSM will apply Raft messages. This 
must lead to the same state on each node regardless of the code version running there.

To achieve that FSM implementation must keep the same behavior even after code changes. If there is a need to change 
behavior a flag should be added to branch between old and new behaviors first. It should default to the old behavior. The 
flag state is stored in the FSM itself and switched by sending a special migration input some time after the deployment
finished. This way the behavior can be switched synchronously for all nodes.
