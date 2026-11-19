Aerospike Java Client Tests
===========================

This project contains junit tests for the Aerospike Java client.
The client library should be built/installed before running these tests.
  
Usage:

    ./run_tests <options>

    options:
    -h,--host <arg>       Server hostname (default: localhost)
    -U,--user <arg>       User name. Use for servers that require authentication.
    -P,--password <arg>   Password. Use for servers that require authentication.
    -n,--namespace <arg>  Namespace (default: test)
    -p,--port <arg>       Server port (default: 3000)
    -s,--set <arg>        Set name. Use 'empty' for empty set (default: test)
    -tls,--tlsEnable      Use TLS/SSL sockets
    -tlsCiphers,--tlsCipherSuite <arg>
                          Allow TLS cipher suites
                          Values:  cipher names defined by JVM separated by comma
                          Default: null (default cipher list provided by JVM)
    -tp,--tlsProtocols <arg>
                          Allow TLS protocols
                          Values:  TLSv1.1,TLSv1.2 separated by comma
                          Default: TLSv1.2
    -tr,--tlsRevoke <arg> 
                          Revoke certificates identified by their serial number
                          Values:  serial numbers separated by comma
                          Default: null (Do not revoke certificates)
    -d,--debug            Run in debug mode.
    -u,--usage            Print usage.

Examples:

    ./run_tests 
    ./run_tests -h host1
    ./run_tests -h host2 -p 3000 -n myns -s myset

Run a specific test:

    # TestQueryFilterExp is the test class name and queryNot is the test method.
    ./run_tests -Dtest=TestQueryFilterExp#queryNot

TLS Examples:

    ./run_tests -Djavax.net.ssl.trustStore=TrustStorePath -Djavax.net.ssl.trustStorePassword=TrustStorePassword -DrunSuite="**/SuiteSync.class" -h hostname:tlsname:tlsport -tls

    ./run_tests -Djavax.net.ssl.trustStore=TrustStorePath -Djavax.net.ssl.trustStorePassword=TrustStorePassword -DrunSuite="**/SuiteAsync.class" -h hostname:tlsname:tlsport -tls -netty

Code coverage:

    # Run from the repository root, not from this directory.
    mvn -Pcoverage clean test -Dargs="-h host1 -p 3000"

The opt-in `coverage` profile measures JaCoCo line coverage of the client
library's classes. It runs the client module's unit tests and the `SuiteAll`
integration suite (a live server is required; `-Dargs` takes the same options
as `run_tests` and may be omitted for the defaults), then merges both into one
report:

    test/target/site/jacoco-aggregate/index.html    (HTML)
    test/target/site/jacoco-aggregate/jacoco.xml    (XML)
    test/target/site/jacoco-aggregate/jacoco.csv    (CSV)

The profile only measures; it sets no coverage threshold. A failing test still
fails the build before the report is written. Add
`-Dmaven.test.failure.ignore=true` to get a report from such a run.

Tests that are skipped because the server lacks a capability (enterprise
edition, security, strong consistency, TLS) lower the figure, so only compare
results measured against the same kind of server. Use `clean` so that execution
data from an earlier run is not merged in.

Merged coverage from several server configurations:

    ./run_coverage [-D...] ["<options>" ...]

    ./run_coverage -Djavax.net.ssl.trustStore=TrustStorePath -Djavax.net.ssl.trustStorePassword=TrustStorePassword \
        "-h host1 -p 3000" \
        "-h host2 -p 3000 -U user -P pass" \
        "-h host3:tlsname:tlsport -tls -netty -U user -P pass" \
        "-h host1 -p 3000 -n sc_namespace"

`run_coverage` runs the `coverage` profile once per server configuration and
writes one merged report to the location above. Each configuration is one
quoted argument holding the same options `run_tests` accepts; `-D` arguments
apply to every pass. With no configuration it runs a single pass against the
default server. It can be run from any directory.

The first pass starts from a clean build; later passes add to the same
execution data (`client/target/jacoco.exec` and `test/target/jacoco.exec`).
Test failures do not stop later passes or the report. After the last pass the
script prints the tests run, failures, errors and skipped tests of each pass
(passes are numbered in command line order and include the client module's
unit tests) and the merged line coverage, overall and per package. It exits
non-zero if any pass had a failure or error. Each pass's surefire reports are
kept in `test/target/coverage-passes/<pass>/`.

Requirements found when measuring:

* The async suite cannot use TLS on the default event loops. A TLS pass must
  add `-netty`.
* A strong consistency namespace must set `strong-consistency-allow-expunge true`,
  because most tests begin with a plain delete.

Merging execution data collected elsewhere:

    ./run_coverage --merge pass1-client.exec pass1-test.exec pass2-client.exec pass2-test.exec

`--merge` builds the same report and coverage summary from existing JaCoCo
execution data files without running any tests. Use it when the passes run on
separate machines, for example as separate CI jobs: on each machine run one
pass (`./run_coverage "<options>"`), keep `client/target/jacoco.exec` and
`test/target/jacoco.exec` under names that are unique to the pass, then check
out the same commit on one machine and give `--merge` all of the files.
Execution data already under `client/target` and `test/target` is discarded
first. The data only matches classes compiled from the same source with the
same JDK version; other classes are reported as not covered.
