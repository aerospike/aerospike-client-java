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
    -u,--usage            Print usage.

Examples:

    mvn test 
    mvn test -Dargs="-h host1"
    mvn test -Dargs="-h host2 -p 3000 -n myns -s myset"
