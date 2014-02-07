Aerospike Java Client Servlet Example
=====================================

This project is a simple servlet showing interactions with the Aerospike 
database through an HTTP interface. The source code can be imported into 
your IDE and/or built using Maven.

    mvn package
    cp target/aerospike.war <tomcat directory>/webapps	

A web page that does AJAX requests to the servlet is provided. 

    http://<host>:<port>/aerospike/example.html

The servlet exports a REST interface where keys in the Aerospike database 
are expressed as namespace/set/keyname triples. 

To fetch a particular key:

    http://<host>:<port>/aerospike/client/<namespace>/<set>/<keyname>

To set a value, POST to the same URL, adding parameters 'bin' and 'value'. 
The response is sent back in JSON format:

    { "response": {<binname>:<value>, ... <binname>:<value>}, "generation":<generation>}

The servlet has been tested under Tomcat 6.
