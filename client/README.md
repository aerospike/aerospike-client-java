Aerospike Java Client Library
=============================

This project contains the files necessary to build the Java client library 
interface to Aerospike database servers. 

AerospikeClient now supports synchronous and asynchronous methods. Asynchronous 
methods can utilize either Netty event loops or direct NIO event loops.

The Netty library artifacts (netty-transport and netty-handler) are declared optional.
If your application's build file (pom.xml) declares these Netty library artifacts as 
dependencies, then the Netty libraries will be included in your application's jar.
Otherwise, you application's jar will not include any Netty code.

The source code can be imported into your IDE and/or built using Maven.

    mvn install

Canonical Reference Application
-------------------------------

[SubMilliPost](https://github.com/aerospike/aerospike-submillipost) is the
canonical reference application for this client library — a stripped-down social
newsletter that teaches Aerospike data modeling through a realistic read-heavy
social graph (users, posts, notes, comments, likes, feeds, notifications, and
more) rather than isolated API samples.

* Repository: https://github.com/aerospike/aerospike-submillipost
* Java source: [`implementations/java`](https://github.com/aerospike/aerospike-submillipost/tree/main/implementations/java)

The programs in `../examples/` show *what* a single API call does. SubMilliPost
shows *when* and *why* to use it — record and bin layout, how operations compose
for a feature, and how client APIs fit real access paths. Aerospike code lives
under `submillipost-api/.../repository/`:

| Operation | Example usage | Where to look |
|---|---|---|
| CRUD | User and post records; conditional create | `UserRepository`, `PostRepository` |
| `operate()` | Atomic likes, counter updates | `ContentLikeRepository` |
| CDTs | Feed lists, nested comment maps | `CommentRepository` |
| Batch reads | Feed hydration across many keys | `AbstractAerospikeRepository` |
| Expressions | Idempotent like guards | `ContentLikeRepository` |
| Queries & TTL | Notification listing, expiration | `NotificationRepository` |

Start with `UserRepository` and `PostRepository`, then follow repositories as
complexity grows. Each feature maps an action to an access path — point read,
list append, atomic update, batch load, or indexed query — so you can see which
client operation fits a given pattern.

Use `examples/` to try an API quickly; use SubMilliPost to learn how those calls
compose in a service. See
[`implementations/java/README.md`](https://github.com/aerospike/aerospike-submillipost/blob/main/implementations/java/README.md)
for build instructions and a full capability map.
