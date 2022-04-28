Aerospike Java Client Package
=============================

Aerospike Java client.  This package contains full source code for these projects.

* client:           Java client library.
* examples:         Java client examples.
* benchmarks:       Java client benchmarks.
* test:             Java client unit tests.

Prerequisites:

* Java 1.8 or greater.
* Maven 3.0 or greater.

AerospikeClient now supports synchronous and asynchronous methods. Asynchronous 
methods can utilize either Netty event loops or direct NIO event loops.

The Netty library artifacts (netty-transport and netty-handler) are declared optional.
If your application's build file (pom.xml) declares these Netty library artifacts as 
dependencies, then the Netty libraries will be included in your application's jar.
Otherwise, you application's jar will not include any Netty code.

The source code can be imported into any Java IDE.  
Maven build scripts are also provided.

Build instructions:

    ./build_all
