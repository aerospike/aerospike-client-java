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
    -tls,--tls            Use TLS/SSL sockets
    -d,--debug            Run in debug mode.
    -u,--usage            Print usage.

Examples:

    mvn test 
    mvn test -Dargs="-h host1"
    mvn test -Dargs="-h host2 -p 3000 -n myns -s myset"

TLS Example:

    mvn test -Djavax.net.ssl.trustStore=KeyStorePath -Djavax.net.ssl.trustStorePassword=KeyStorePassword -DrunSuite="**/SuiteSync.class" -Dargs="-h hostname:tlsname:tlsport -tls"
