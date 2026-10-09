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
shared server. `tls/run_tls_matching.sh` generates each certificate, starts a single node
Aerospike Enterprise container with it, runs `SuiteTlsMatching` and removes the container.
It requires docker, openssl, an Enterprise image and a feature-key file:

    AEROSPIKE_IMAGE=aerospike/aerospike-server-enterprise:8.1 \
    AEROSPIKE_FEATURES_FILE=/path/to/features.conf \
    tls/run_tls_matching.sh [case ...]

With no case names every case runs. Case names and the optional environment variables are listed
at the top of the script. Per case output is written to `target/tls-matching/<case>/mvn.log`.
