# Testing

Development of stateful applications requires a diligent and thoughtful approach. A small bug can lead to data 
corruptions. Monstera framework ensures that it is hard to make a mistake in trivial pieces, and critical pieces
are easily testable with full coverage.

## Application Cores

Application cores are State Machines with no side effects. This makes them perfectly testable! A core can be implemented
completely in-memory, or can be backed by an embedded database, either way this core does not need any mocks and can be
tested in-process as it is.

In my opinion, the best way to test an application core is by changing and observing its state via its public interface 
only. This is an integration testing level, and unit testing can be skipped entirely or done for a few most complicated 
things only. There are two main reasons for that:

* When tests interact with a core only via its public interface they are free from its implementation details. The
  common practice of unit testing assumes that all dependencies are mocked and only one class is being tested at a time, 
  or the state can be observed by accessing private fields or methods. This makes every refactoring a nightmare because 
  mocks should also be fixed according to the new implementation. Public interface is stable and the overall behavior of
  an application core should not change after a refactoring.
* In Monstera Read and Update messages are the public contract for interacting with cores. Update messages are stored in
  Raft log and can be replayed later. All cores should be deterministic and stable (see 
  [Core Principles](/docs/core-principles.md)). So it is very important to make sure that this behavior is unchanged.

Test scenarios would look like a sequence of commands to bring a machine to a specific expected state following with a
verification of that state. Since updates are applied sequentially in Monstera we do not need to accommodate for race
conditions and a simple sequential test is enough. Also, it is prohibited to access system time or randomness inside a 
core, and all non-deterministic values should be passed as inputs. That means all time-sensitive scenarios can also be
tested by a simple sequence of commands without any mocking.

Additional scenarios should focus on snapshots:

* A core should be able to take a snapshot and recover from it.
* A core should be able to apply updates in parallel with writing a snapshot to a sink.
* A code is still compatible with old snapshots.

## Integration testing

Not all the business logic lives inside application cores, there are also clients of them. In production environment
clients and cores run on different machines in different processes. But the framework principles make testing simple
even here. Take a look at [Single Node Mode](/docs/single-node-mode.md) on how to build a 
single-process version of the system. The same approach applies to Integration testing. A nonclustered stub can be 
injected into a client, and integration tests will test real client's logic interacting with real application core 
without any mock.

## Performance testing

Updates are applied sequentially to a core. That means the whole throughput of a shard is limited by a single-thread 
performance of its application core. That also means that a setup for performance testing is relatively simple. A core
can be benchmarked in the traditional Go style. Different states or different amounts of data can be easily prepared for
a test in the same way that was described above.

Aim for a few nanoseconds for in-memory application cores, and for sub-millisecond for disk-backed cores. 

## Load testing

A full load test of a cluster is necessary for a few reasons:

* To determine overall disk load. For each replica on the node Raft stores its log on disk. Plus application 
  cores backed by embedded databases also write to disk. Plus periodic snapshots.
* To highlight bottlenecks. For example, depending on the implementation, application cores can share a common instance 
  of an embedded database (1 database per disk, many application cores per Go process). Even if each core is fast by 
  itself, under load the common storage can become a bottleneck.
* To determine proportion of CPU load to disk load. Some applications are data-heavy (message ingestion or large 
  payloads), some applications are compute-heavy (correctness checks, searches, tree traversing, etc).
* To detect performance degradation over time. For example, some embedded databases store each version of a key until
  they are garbage collected, and performance of scans goes down when it scans through thousands of versions that are 
  not the latest anymore.   

A load end-to-end test is performed by synthesizing traffic. It should not fall on a single hot shard, and should be 
diverse (if applicable: many users, many entities, different sizes of batches, different sizes of payloads, etc).

## Sharding

It is a best practice to test sharding related functions. Shard key calculation is used both for routing (each sharded 
request exposes a `ShardKey()` method) and often inside application cores, so it is a good idea to share the same 
functions for shard key calculations. The main property of it
is to be stable over time, meaning that a given unit of work `x` should have a shard key `key := shardFunc(x)` today,
tomorrow, and five years from today. Otherwise the same message could be routed to a wrong shard. A simple test with a 
hardcoded list of inputs and outputs can help to ensure that.
