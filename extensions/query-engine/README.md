Aerospike Query Engine
======================

This project contains the files necessary to build the Aerospike Query Engine library. 
Aerospike Query Engine will automatically choose an index if one is available to qualify
the results, and then use PredExp further qualify the results.

Query Engine uses a `Statement` and zero or more `Qualifier` objects and produces a closable `KeyRecordIterator` to iterate over the results of the query.

Java Package `com.aerospike.helper.query`

[Documentation](doc/query.md)

### Build

The source code can be imported into your IDE and/or built using Maven.

    mvn install 

### Test    

Tests are disabled by default.  To run tests, edit host arguments in 
src/test/java/com/aerospike/helper/query/TestQueryEngine.java and execute
script: 

    ./run_tests
