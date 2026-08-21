# Aerospike Java Client Package

## AI coding agent entry point

The Aerospike Java client — Maven artifact `com.aerospike:aerospike-client-jdk21`.
Authoritative version: `<revision>` in the root `pom.xml`.
Requires Java 21+ and Maven 3.9.5+.
The `stage-jdk8` branch builds the Java 8 artifact.

### What to read, by task

| Task | Read first | Authoritative for |
|---|---|---|
| First read/write | `examples/README.md` → `PutGet`, `StoreKey` | what one call does |
| Policies, timeouts, retries | `client/src/com/aerospike/client/policy/` | parameter semantics |
| Single-record and `operate()` | `examples/.../Operate.java`, `OperateList.java` | operation composition |
| Batch commands | `examples/.../Batch.java`, `test/.../sync/basic/` | behavior, edge cases |
| Collection data types | `client/src/com/aerospike/client/cdt/` | operation set |
| Expressions, path expressions | `client/src/com/aerospike/client/exp/` | operation set |
| Queries, secondary indexes | `examples/.../Query*.java`, `test/.../sync/query/` | behavior |
| Transactions | `test/.../sync/basic/` | behavior |
| Async, event loops | `client/src/com/aerospike/client/async/`, `test/.../async/` | behavior |
| Errors and result codes | `client/src/com/aerospike/client/{AerospikeException,ResultCode}.java` | codes |
| Idiomatic use at feature scale | SubMilliPost — see below | when and why |

### Repository map

```
aerospike-client-java/
├── README.md              entry point (this file)
├── AGENTS.md              pointer to the section above
├── pom.xml                parent POM — modules and the authoritative <revision>
├── CLIENT-DEV.md          contributor-facing: branch porting between stage and stage-jdk8
├── client/                client library source — the API surface
│   ├── README.md          build notes + SubMilliPost capability map
│   └── src/com/aerospike/client/
│       ├── policy/        read, write, batch, query, txn policies
│       ├── cdt/           list and map operations
│       ├── exp/           filter and operation expressions
│       ├── query/         queries, statements, filters
│       ├── operation/     bit, HLL operations
│       ├── async/         async client and event loops
│       ├── admin/         user and role management
│       ├── metrics/       client metrics
│       ├── configuration/ dynamic client configuration
│       ├── cluster/, command/, listener/, task/, lua/, util/  internals
│       └── AerospikeClient.java, AerospikeException.java, ResultCode.java, ...
├── examples/              ~34 runnable single-purpose programs
│   ├── README.md          example → description table
│   └── src/com/aerospike/examples/
├── test/                  JUnit suite — ~78 test classes
│   ├── README.md          ./run_tests and its options
│   ├── run_tests
│   └── src/com/aerospike/test/{sync/basic,sync/query,async,util}
├── benchmarks/            load generator, not an API reference
└── .claude/skills/        contributor-facing: port-to-jdk21, port-to-jdk8
```

Key docs: [client/README.md](client/README.md) · [examples/README.md](examples/README.md) · [test/README.md](test/README.md) · [CLIENT-DEV.md](CLIENT-DEV.md) (contributor-facing)

Generated Javadoc is published at https://javadoc.io/doc/com.aerospike/aerospike-client-jdk21/latest/index.html
and can be built locally with `mvn javadoc:javadoc` in `client/` (output lands in `client/apidocs`).

### Canonical reference application

[SubMilliPost](https://github.com/aerospike/aerospike-submillipost) is the canonical reference application — a realistic social newsletter, not isolated API samples.
Java implementation: [`implementations/java`](https://github.com/aerospike/aerospike-submillipost/tree/main/implementations/java).
Full capability map and the operation-to-repository table: [client/README.md](client/README.md).

`examples/` shows *what a single API call does*.
SubMilliPost shows *when and why* — record and bin layout, how operations compose into a feature, how client APIs map onto real access paths.

### Precedence when sources disagree

1. Javadoc for signatures and parameter semantics
2. `test/` for actual behavior, including edge cases
3. SubMilliPost for idiomatic composition at feature scale
4. `examples/` for single-call usage
5. aerospike.com/docs for server-side semantics and version gates

Anything contradicting the Javadoc on a signature is stale. Report it rather than following it.

### Known traps

* **Do not hand-roll batch fan-out.** Batch commands already switch to the single-record path when a node's sub-batch has size 1. Use `BatchRead`/`operate` batch APIs, not a loop.
* **Do not loop single-record calls where a batch counterpart exists.** `get`, `delete`, `operate`, and `exists` all have batch forms.
* **`modifyByPath` can remove matching elements.** It is not read-only.

### Verifying generated code

`./build_all` from the repo root, then `cd test && ./run_tests` against a local server.
A passing run proves the client builds and the suite agrees with the installed server version; it does not prove your application logic is correct.

### Aerospike agent skills

[aerospike/agent-skills](https://github.com/aerospike/agent-skills) carries core-database and data-modeling guidance — key design, record sizing, collection choice, indexing.
Complementary to this repo, which is authoritative for the Java API surface.

## Package contents

This package contains full source code for these projects:

* `client` — Java native client library
* `examples` — Java client examples
* `benchmarks` — Java client benchmarks
* `test` — Java client unit tests

### Prerequisites

* Java 21+
* Maven 3.9.5+

The source code can be imported into any Java IDE.
Maven build scripts are also provided.

### Build

    ./build_all
