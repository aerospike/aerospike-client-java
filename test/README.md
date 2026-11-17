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

TLS name validation:

`TestTlsName` (in `SuiteSync`) checks server certificate validation against the tlsName on the
sync and netty paths: the configured tlsName is accepted and a wrong tlsName is rejected without
retry. It runs only when `-tls` is set and `-h` includes a tlsName, and is skipped otherwise.

    ./run_tests -Djavax.net.ssl.trustStore=TrustStorePath -Djavax.net.ssl.trustStorePassword=TrustStorePassword -Dtest=TestTlsName -h hostname:tlsname:tlsport -tls

Certificate matching cases (DNS and IP subject alternative names, wildcards, legacy CN fallback,
revoked serials) need a different server certificate per case, so they do not run against a
shared server. They run against the `tls-matching` environment described below: `SuiteTlsMatching`
runs the case named by `TLS_CASE`, starting its server with the environment's `serve` verb. The
legacy match warning is logged once per tlsName per JVM, so each case runs in its own JVM.
`TestTlsProxy` reaches a two node cluster through a TCP pass-through listener per node, and learns
the second node and its tls-name from peers-*. From the repository root, with the environment up and
its values loaded:

    for c in ${TLS_CASES//,/ }; do
        TLS_CASE="$c" mvn -pl test test -DskipTests=false -DrunSuite='**/SuiteTlsMatching.class'
    done
    mvn -pl test test -DskipTests=false -Dtest=TestTlsProxy -DfailIfNoTests=false

Discovery provider, address translation and TLS name matching suites:

These suites run against the test environment of the same name in the `envs/` directory of
[aerospike-client-conformance](https://github.com/aerospike/aerospike-client-conformance/tree/main/envs),
which starts the servers, writes a values file and removes everything on `down`. The tests read the
values from the environment and run the environment's verbs, such as starting a node or stopping
the DNS server, through `"$ENV_CTL" <verb>`.

| Suite                              | Environment    |
| ---------------------------------- | -------------- |
| `SuiteDnsDiscovery`                | `dns`          |
| `SuiteAwsTags`                     | `aws-tags`     |
| `SuitePortMap`                     | `port-map`     |
| `SuiteTranslationCollision`        | `collision`    |
| `SuiteYamlTranslation`             | `yaml`         |
| `SuitePassThrough`                 | `pass-through` |
| `SuiteTranslationMiss`             | `on-miss`      |
| `SuiteAlternateMissing`            | `alt-missing`  |
| `SuiteTlsMatching`, `TestTlsProxy` | `tls-matching` |

For example, from the conformance checkout and then this repository's root:

    export AEROSPIKE_IMAGE=aerospike/aerospike-server-enterprise:8.1
    export FEATURES_CONF_PATH=/path/to/features.conf
    values="$(envs/dns.sh up)"
    set -a; . "$values"; set +a
    mvn -pl test test -DskipTests=false -DrunSuite='**/SuiteDnsDiscovery.class'
    "$ENV_CTL" down
