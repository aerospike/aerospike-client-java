Aerospike Reactor Java Client Library
=============================

This project contains the files necessary to build the Java Reactor client library 
interface to Aerospike database servers. 

AerospikeReactorClient now supports reactive methods. 

The Netty library artifacts (netty-transport and netty-handler) are declared optional.
If your application's build file (pom.xml) declares these Netty library artifacts as 
dependencies, then the Netty libraries will be included in your application's jar.
Otherwise, you application's jar will not include any Netty code.

The source code can be imported into your IDE and/or built using Maven.

    mvn install
    
To run tests you need environment with aerospike running
The simplest way to get it is to install docker and run:

     docker run -tid --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike/aerospike-server
