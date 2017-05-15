Aerospike Java Client Tests
===========================

This project contains junit tests for the Aerospike Java client.
The client library should be built/installed before running these tests.
  
Usage:

    mvn test [-Dargs="<options>"]
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
                          Values:  SSLv3,TLSv1,TLSv1.1,TLSv1.2 separated by comma
                          Default: TLSv1.2
    -tr,--tlsRevoke <arg> 
                          Revoke certificates identified by their serial number
                          Values:  serial numbers separated by comma
                          Default: null (Do not revoke certificates)
    -d,--debug            Run in debug mode.
    -u,--usage            Print usage.

Examples:

    mvn test 
    mvn test -Dargs="-h host1"
    mvn test -Dargs="-h host2 -p 3000 -n myns -s myset"

Run a specific test:

    # TestQueryPredExp is the test class name and queryPredicate2 is
    # the test method.
    #
    mvn test -Dtest=TestQueryPredExp#queryPredicate2

TLS Examples:

    mvn test -Djavax.net.ssl.trustStore=TrustStorePath -Djavax.net.ssl.trustStorePassword=TrustStorePassword -DrunSuite="**/SuiteSync.class" -Dargs="-h hostname:tlsname:tlsport -tls"
    mvn test -Djavax.net.ssl.trustStore=TrustStorePath -Djavax.net.ssl.trustStorePassword=TrustStorePassword -DrunSuite="**/SuiteAsync.class" -Dargs="-h hostname:tlsname:tlsport -tls -netty"
