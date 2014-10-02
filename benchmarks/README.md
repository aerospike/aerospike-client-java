Aerospike Java Client Benchmarks
================================

This project contains the files necessary to build Java client benchmarks. 
This program is used to insert data and generate load. 
The source code can be imported into your IDE and/or built using Maven.

    mvn package

The command line usage can be obtained by:

    ./run_benchmarks -u

Some sample arguments are:

    # Connect to localhost:3000 using test namespace.
    # Read 10% and write 90% of the time using 20 concurrent threads.
    # Use 100000000 integer keys (starting at "1") and 50 character string values.
    # Benchmark synchronous methods.
    ./run_benchmarks -h 127.0.0.1 -p 3000 -n test -k 100000000 -S 1 -o S:50 -w RU,10 -z 20

    # Connect to localhost:3000 using test namespace.
    # Read 80% and write 20% of the time using 8 concurrent threads.
    # Use 10000000 integer keys and 1400 bytes values using a single bin.
    # Timeout after 50ms for reads and writes.
    # Restrict transactions/second to 2500.
    # Benchmark synchronous methods.
    ./run_benchmarks -h 127.0.0.1 -p 3000 -n test -k 10000000 -b 1 -o B:1400 -w RU,80 -g 2500 -T 50 -z 8

    # Benchmark asynchronous methods using a single producer thread and 8 selector threads.
    # Limit the maximum number of concurrent commands to 1000.
    # Use and 50% read 50% write pattern.
    ./run_benchmarks -h 127.0.0.1 -p 3000 -n test -k 100000000 -S 1 -o S:50 -w RU,50 -z 1 -async -asyncMaxCommands 1000 -asyncSelectorThreads 8
