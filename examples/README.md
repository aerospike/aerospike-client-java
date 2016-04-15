Aerospike Java Client Examples
==============================

This project contains source code examples using the Aerospike Java Client.

Example | Description | Link 
--- | --- | --- 
ServerInfo          | Use Aerospike info protocol to query a server node for statistics.                   | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/ServerInfo.java)
PutGet              | Write and read a record.                                                             | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/PutGet.java)
Replace             | Write bins using the replace option.                                                 | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Replace.java)
Add                 | Perform a server integer add.                                                        | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Add.java)
Append              | Perform a server string append.                                                      | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Append.java)
Prepend             | Perform a server string prepend.                                                     | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Prepend.java)
Batch               | Perform multiple record read commands in a single batch.                             | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Batch.java)
Generation          | Use record generation to ensure that a record has not changed since the last read.   | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Generation.java)
Serialize           | Use Java default serialization when writing and reading a bin.                       | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Serialize.java)
Expire              | Set the record expiration.                                                           | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Expire.java)
Touch               | Extend the life of records ready to expire.                                          | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Touch.java)
StoreKey            | Store user key on server using WritePolicy.sendKey option.                           | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/StoreKey.java)
DeleteBin           | Delete a bin in a record.                                                            | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/DeleteBin.java)
ListMap             | Write and read records containing combinations of list and map bins.                 | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/ListMap.java)
Operate             | Perform multiple operations on a single record in one database command.              | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/Operate.java)
OperateList         | Perform multiple list operations on a single record in one database command.         | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/OperateList.java)
ScanParallel        | Scan all records in a namespace/set by querying server nodes in parallel.            | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/ScanParallel.java)
ScanSeries          | Scan all records in a namespace/set by querying server nodes in series.              | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/ScanSeries.java)
UserDefinedFunction | Call UDFs on the server.                                                             | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/UserDefinedFunction.java)
QueryInteger        | Query bins using an integer index.                                                   | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryInteger.java)
QueryString         | Query bins using a string index.                                                     | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryString.java)
QueryFilter         | Query on a secondary index with a filter, and apply an additional filter in a UDF.   | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryFilter.java)
QuerySum            | Query records and calculate sum using a user-defined aggregation function.           | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QuerySum.java)
QueryAverage        | Query records and calculate average using a user-defined aggregation function.       | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryAverage.java)
QueryCollection     | Query records using a map index.                                                     | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryCollection.java)
QueryRegion         | Perform region/radius queries using a Geo index.                                     | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryRegion.java)
QueryRegionFilter   | Perform region query using a Geo index with an aggregation filter.                   | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryRegionFilter.java)
QueryGeoCollection  | Perform region queries using a Geo index on a collection.                            | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryGeoCollection.java)
QueryExecute        | Run a UDF on records matching the query filter.                                      | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/QueryExecute.java)
AsyncPutGet         | Write and read a record in asynchronous mode.                                        | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/AsyncPutGet.java)
AsyncBatch          | Perform multiple read commands in a single batch in asynchronous mode.               | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/AsyncBatch.java)
AsyncQuery          | Query records in asynchronous mode.                                                  | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/AsyncQuery.java)
AsyncScan           | Scan all records in a namespace or set in series in asynchronous mode.               | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/AsyncScan.java)
AsyncUserDefinedFunction | Call UDFs on the server in asynchronous mode.                                   | [View](https://github.com/aerospike/aerospike-client-java/blob/master/examples/src/com/aerospike/examples/AsyncUserDefinedFunction.java)

#### Build

The source code can be imported into your IDE and/or built using Maven.

    mvn package

#### Usage

There are two scripts to run example code:

* run_examples_swing - Run examples with a graphical user interface.
* run_examples - Run examples on the command line.
  
    ./run_examples [<options>] all|(<example1> <example2> ...)
    options:
    -d,--debug            Run in debug mode.
    -g,--gui              Invoke GUI to selectively run tests.
    -h,--host <arg>       Server hostname (default: localhost)
    -n,--namespace <arg>  Namespace (default: test)
    -P,--password <arg>   Password
    -p,--port <arg>       Server port (default: 3000)
    -s,--set <arg>        Set name. Use 'empty' for empty set (default: demoset)
    -U,--user <arg>       User name
    -u,--usage            Print usage.

#### Usage Examples

    ./run_examples -h localhost -p 3000 -n test -s demoset all
    ./run_examples -h localhost -p 3000 -n test -s demoset ServerInfo PutGet Generation
    ./run_examples -g -h localhost -p 3000 -n test -s demoset
