Aerospike Blob Finder Package
=============================

This package is designed exlusively to detect language specific blobs in an
Aerospike database. It uses specialized AerospikeClient code that appears
like a client, but does not behave like a client. Examples, benchmarks and 
test are disabled.

Build instructions:

    mvn clean
    mvn package

Display command line options:

    ./blobfinder -u

Example run command:

    ./blobfinder -h localhost -o blobs.dat 

Example run command using TLS:

    # First, setup truststore 
    ./blobfinder -Djavax.net.ssl.trustStore=truststore -Djavax.net.ssl.trustStorePassword=truststorepass -h "localhost:tlsname:4333" -U myuser -P mypass -tls -o blobs.dat

Example run command using TLS with mutual authentication:

    # First, setup truststore and keystore (for mutual authentication).
    ./blobfinder -Djavax.net.ssl.trustStore=truststore -Djavax.net.ssl.trustStorePassword=truststorepass -Djavax.net.ssl.keyStore=keystore -Djavax.net.ssl.keyStorePassword=keystorepass -h localhost -U myuser -P -tls -o blobs.dat
