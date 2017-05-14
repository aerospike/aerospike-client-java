Aerospike Java Client Library
=============================

This project contains the files necessary to build the Java client library 
interface to Aerospike database servers. 

AerospikeClient now supports synchronous and asynchronous methods. Asynchronous 
methods can utilize either Netty event loops or direct NIO event loops.

The Netty library artifacts (netty-transport and netty-handler) are declared optional.
If your application's build file (pom.xml) declares these Netty library artifacts as 
dependencies, then the Netty libraries will be included in your application's jar.
Otherwise, you application's jar will not include any Netty code.

The source code can be imported into your IDE and/or built using Maven.

    mvn install
