Aerospike Blob Finder Package
=============================

This package is designed exlusively to detect language specific blobs in an
Aerospike database. It uses specialized AerospikeClient code that appears
like a client, but does not behave like a client. Examples, benchmarks and 
test are disabled.

Build instructions:

    cd client
    mvn clean
    mvn package

See [blobfinder client](client/README.md) for run instructions.
