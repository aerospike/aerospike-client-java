# AI_PIPELINE.md — aerospike-client-java (Java Legacy client)

Durable, repo-level facts for automated agents: how to build it, how to test it,
where things live, and what the house rules are. Task-specific analysis belongs
in the task spec, not here. Add to this file when you learn something that will
still be true for the *next* task.

## Project shape

Maven multi-module, Java 8 (`<java.version>1.8</java.version>`), version
`10.4.0` carried by the `${revision}` property in the root `pom.xml`.

| Module | Contents |
| --- | --- |
| `client` | The client library — the shipped artifact. Public API lives here. |
| `test` | JUnit 4 integration suites. Requires a live Aerospike server. |
| `examples` | Standalone example programs. |
| `benchmarks` | Load-generation / benchmarking tool. |

## Build

Run from the repo root.

```bash
# Compile everything
mvn compile -q

# Build and install the client into the local repo (what the test module needs)
mvn -q -pl client -am install -DskipTests

# Offline (no network): add -o
mvn -q -o -pl client -am install -DskipTests
```

Build the client before running the test module — `test` resolves
`com.aerospike:aerospike-client-jdk8` from the local repository, not from the reactor.

## Test

JUnit 4 + Maven Surefire. **A live Aerospike server is required**; the suites
default to `127.0.0.1:3000`.

Two properties control what runs, both defined in `test/pom.xml`:

* `skipTests` — defaults to **`true`**. Tests do not run unless you pass
  `-DskipTests=false`. A plain `mvn test` reports `BUILD SUCCESS` having run
  nothing; that is not a passing test run.
* `runSuite` — surefire `<include>` selector, defaults to `**/SuiteAll.class`.

```bash
# From test/ — run one suite
mvn -o test -DskipTests=false -DrunSuite='**/SuiteErrorDetail.class'

# Run a single test class
mvn -o test -DskipTests=false -DrunSuite='**/TestExpErrorDetail.class'

# Everything
mvn -o test -DskipTests=false
```

Available suites (`test/src/com/aerospike/test/`): `SuiteAll`, `SuiteSync`,
`SuiteAsync`, `SuiteErrorDetail`, `SuiteEmpty`.

Server connection and auth flags are passed as one space-separated string in the
`args` system property, parsed by `test/src/com/aerospike/test/util/Args.java`:

```bash
mvn -o test -DskipTests=false -Dargs="-h 127.0.0.1 -p 3000 -n test"
```

`-h host` `-p port` (default 3000) `-n namespace` `-U user` `-P password`
`--auth <mode>` plus TLS options. The `run_tests` wrapper script at the repo
root splits `-D…` from ordinary flags and builds this invocation for you.

`Args` also detects the connected server's version into `args.serverVersion`;
suites gate version-dependent tests with
`org.junit.Assume.assumeTrue(args.serverVersion.isGreaterOrEqual(...))`.

**Verify results, not exit codes.** `-DskipTests` defaulting to true means an
untouched `mvn test` exits 0 with zero tests executed. Always confirm the
`Tests run: N` line, and that N is what you expected.

## Architecture

Public API under `client/src/com/aerospike/client/`:

| Package | Role |
| --- | --- |
| *(root)* | `AerospikeClient`, `AerospikeException`, `Bin`, `Key`, `Record`, `Value`, batch types |
| `policy` | Read/write/batch/query policies |
| `exp` | Filter and operation expressions (`Exp`, `ListExp`, `MapExp`, `BitExp`, `HLLExp`) |
| `cdt` | Collection data types — list/map operations, contexts, policies |
| `operation` | Operation builders |
| `command` | Wire protocol encode/decode, msgpack packing (`util/Packer`) |
| `cluster` | Node discovery, partition map, connection pooling |
| `async` | Async/NIO/Netty event loops and listeners |
| `query`, `task`, `admin`, `metrics`, `configuration`, `lua`, `listener`, `util` | Queries, long-running tasks, security admin, metrics, dynamic config, UDF support, callbacks, helpers |

Tests under `test/src/com/aerospike/test/`: `sync/basic`, `sync/query`,
`async`, with shared helpers in `util`.

## Conventions

* **Additive and binary-compatible.** This is a released, versioned public API.
  Do not remove or change existing public constructors, method signatures or
  field types. New public methods and fields are fine.
* **Javadoc on public methods and fields.**
* Minimal, focused diff — do not touch unrelated files, do not reformat.
* Match the existing style of the file you are editing, including comment
  density (generally sparse). Do not add commentary restating what the code does.
* Do not reference ticket numbers in source.
* Tests are ported across clients (Go, C, C#, Python). When changing a test that
  has counterparts, note the divergence rather than silently letting it drift.
* Read `README.md`'s "AI coding agent entry point" section before generating
  code that *uses* the client (see `AGENTS.md`).
