Aerospike Java Client Examples
==============================

This project contains source code examples for Aerospike Java client 
library functionality.  The source code can be imported into your IDE 
and/or built using Maven.

    mvn package

There are two scripts to run example code:

* run_examples_swing - Run examples with a graphical user interface.
* run_examples - Run examples on the command line.
  
Usage:

    ./run_examples [<options>] all|(<example1> <example2> ...)
    options:
    -g,--gui              Invoke GUI to selectively run tests
    -h,--host <arg>       Server hostname (default: localhost)
    -U,--user <arg>       User name. Use for servers that require authentication.
    -P,--password <arg>   Password. Use for servers that require authentication.
    -n,--namespace <arg>  Namespace (default: test)
    -p,--port <arg>       Server port (default: 3000)
    -s,--set <arg>        Set name. Use 'empty' for empty set (default: demoset)
    -u,--usage            Print usage.

    examples:
    ServerInfo
    PutGet
    Replace
    Add
    Append
    Prepend
    Batch
    Generation
    Serialize
    Expire
    Touch
    Operate
    DeleteBin
    ScanParallel
    ScanSeries
    AsyncPutGet
    AsyncBatch
    AsyncScan
    ListMap
    UserDefinedFunction
    LargeSet
    LargeStack
    QueryInteger
    QueryString
    QueryFilter
    QuerySum
    QueryAverage
    QueryExecute
    
    All examples will be run if 'all' is specified as an example.

Some sample arguments are:

    ./run_examples -h localhost -p 3000 -n test -s demoset all
    ./run_examples -h localhost -p 3000 -n test -s demoset SetGet Generation Touch
    ./run_examples -g -h localhost -p 3000 -n test -s demoset 
