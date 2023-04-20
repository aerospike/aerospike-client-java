Aerospike Java Proxy Client Library
===================================

The proxy client is designed to communicate with a proxy server in dbaas
(database as a service) applications. The communication is performed via GRPC
and HTTP/2. The proxy server relays the database commands to the Aerospike
server. The proxy client does not have knowledge of Aerospike server nodes.
The proxy server communicates directly with Aerospike server nodes.

The proxy client's AerospikeClientProxy implements the same IAerospikeClient
interface as the native client's AerospikeClient. AerospikeClientProxy supports
single record, batch and most scan/query commands. AerospikeClientProxy does
not support info and user admin commands nor scan/query commands that are
directed to a single node.

The source code can be imported into your IDE and/or built using Maven.

    mvn install
