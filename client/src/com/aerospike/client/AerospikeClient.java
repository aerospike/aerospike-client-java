/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.ScanCommand;
import com.aerospike.client.command.ScanExecutor;
import com.aerospike.client.command.SingleCommand;
import com.aerospike.client.command.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RetryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Instantiate an <code>AerospikeClient</code> object to access an Aerospike
 * database cluster and perform database operations.
 * <p>
 * This client is thread-safe. One client instance should be used per cluster.
 * Multiple threads should share this cluster instance.
 * <p>
 * Your application uses this class API to perform database operations such as
 * writing and reading records, and selecting sets of records. Write operations
 * include specialized functionality such as append/prepend and arithmetic
 * addition.
 * <p>
 * Each record may have multiple bins, unless the Aerospike server nodes are
 * configured as "single-bin". In "multi-bin" mode, partial records may be
 * written or read by specifying the relevant subset of bins.
 */
public class AerospikeClient {
	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------
	
	private Cluster cluster;
	
	//-------------------------------------------------------
	// Constructors
	//-------------------------------------------------------

	/**
	 * Initialize Aerospike client.
	 * If the host connection succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * If the connection succeeds, the client is ready to process database requests.
	 * If the connection fails, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public AerospikeClient(String hostname, int port) throws AerospikeException {
		this(new ClientPolicy(), new Host(hostname, port));
	}

	/**
	 * Initialize Aerospike client.
	 * The client policy is used to set defaults and size internal data structures.
	 * If the host connection succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * If the connection succeeds, the client is ready to process database requests.
	 * If the connection fails and the policy's failOnInvalidHosts is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public AerospikeClient(ClientPolicy policy, String hostname, int port) throws AerospikeException {
		this(policy, new Host(hostname, port));
	}

	/**
	 * Initialize Aerospike client and find a suitable host to seed the cluster map.
	 * The client policy is used to set defaults and size internal data structures.
	 * For the first host connection that succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * In most cases, only one host is necessary to seed the cluster. The remaining hosts 
	 * are added as future seeds in case of a complete network failure.
	 * <p>
	 * If one connection succeeds, the client is ready to process database requests.
	 * If all connections fail and the policy's failIfNotConnected is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters
	 * @param hosts					array of potential hosts to seed the cluster
	 * @throws AerospikeException	if all host connections fail
	 */
	public AerospikeClient(ClientPolicy policy, Host... hosts) throws AerospikeException {
		cluster = new Cluster(policy, hosts);
		
		if (policy.failIfNotConnected && ! cluster.isConnected()) {
			throw new AerospikeException.Connection("Failed to connect to host(s): " + Arrays.toString(hosts));
		}
	}

	//-------------------------------------------------------
	// Compatibility Layer Initialization
	//-------------------------------------------------------

	/**
	 * Compatibility layer constructor.  Do not use.
	 */
	protected AerospikeClient() {
	}
	
	/**
	 * Compatibility layer host initialization. Do not use.
	 */
	protected final void addServer(String hostname, int port) throws AerospikeException {
		Host[] hosts = new Host[] {new Host(hostname, port)};
		
		// If cluster has already been initialized, add hosts to existing cluster.
		if (cluster != null) {
			cluster.addSeeds(hosts);
			return;
		}	
		cluster = new Cluster(new ClientPolicy(), hosts);
	}

	//-------------------------------------------------------
	// Cluster Connection Management
	//-------------------------------------------------------
		
	/**
	 * Close all client connections to database server nodes.
	 */
	public final void close() {
		cluster.close();
	}

	/**
	 * Determine if we are ready to talk to the database server cluster.
	 * 
	 * @return	<code>true</code> if cluster is ready,
	 * 			<code>false</code> if cluster is not ready
	 */
	public final boolean isConnected() {
		return cluster.isConnected();
	}

	/**
	 * Return a list of server nodes in the cluster.
	 * 
	 * @return	list of node names
	 */
	public final List<String> getNodeNames() {
		Node[] nodes = cluster.getNodes();		
		ArrayList<String> names = new ArrayList<String>(nodes.length);
		
		for (Node node : nodes) {
			names.add(node.getName());
		}
		return names;
	}

	//-------------------------------------------------------
	// Write Record Operations
	//-------------------------------------------------------
	
	/**
	 * Write record bin(s).
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * 
	 * @param policy				write configuration parameters
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if write fails
	 */
	public final void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		 SingleCommand command = new SingleCommand(cluster, key);
		 command.write(policy, Operation.WRITE, bins);
	}

	//-------------------------------------------------------
	// String Operations
	//-------------------------------------------------------
	
	/**
	 * Append bin string values to existing record bin values.
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values. 
	 * 
	 * @param policy				write configuration parameters
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if append fails
	 */
	public final void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		 SingleCommand command = new SingleCommand(cluster, key);
		 command.write(policy, Operation.APPEND, bins);
	}
	
	/**
	 * Prepend bin string values to existing record bin values.
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call works only for string values. 
	 * 
	 * @param policy				write configuration parameters
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if prepend fails
	 */
	public final void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		 SingleCommand command = new SingleCommand(cluster, key);
		 command.write(policy, Operation.PREPEND, bins);
	}
	
	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------
	
	/**
	 * Add integer bin values to existing record bin values.
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for integer values. 
	 * 
	 * @param policy				write configuration parameters
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if add fails
	 */
	public final void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		 SingleCommand command = new SingleCommand(cluster, key);
		 command.write(policy, Operation.ADD, bins);
	}
	
	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------

	/**
	 * Delete record for specified key.
	 * The policy specifies the transaction timeout.
	 * 
	 * @param policy				delete configuration parameters
	 * @param key					unique record identifier
	 * @return						whether record existed on server before deletion 
	 * @throws AerospikeException	if delete fails
	 */
	public final boolean delete(WritePolicy policy, Key key) throws AerospikeException {
		SingleCommand command = new SingleCommand(cluster, key);
		command.setWrite(Command.INFO2_WRITE | Command.INFO2_DELETE);
		command.begin();
		command.writeHeader(policy, 0);
		command.writeKey();
		command.execute(policy);
		return command.getResultCode() == ResultCode.OK;
	}
	
	//-------------------------------------------------------
	// Touch Operations
	//-------------------------------------------------------

	/**
	 * Create record if it does not already exist.  If the record exists, the record's 
	 * time to expiration will be reset to the policy's expiration. 
	 * 
	 * @param policy				write configuration parameters
	 * @param key					unique record identifier
	 * @throws AerospikeException	if touch fails
	 */
	public final void touch(WritePolicy policy, Key key) throws AerospikeException {
		SingleCommand command = new SingleCommand(cluster, key);
		command.setWrite(Command.INFO2_WRITE);
		command.estimateOperationSize();
		command.begin();
		command.writeHeader(policy, 1);
		command.writeKey();
		command.writeOperation(Operation.TOUCH);
		command.execute(policy);
	}

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------

	/**
	 * Determine if a record key exists.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters
	 * @param key					unique record identifier
	 * @return						whether record exists or not 
	 * @throws AerospikeException	if command fails
	 */
	public final boolean exists(Policy policy, Key key) throws AerospikeException {
		SingleCommand command = new SingleCommand(cluster, key);
		command.setRead(Command.INFO1_READ | Command.INFO1_NOBINDATA);
		command.begin();
		command.writeHeader(0);
		command.writeKey();
		command.execute(policy);
		return command.getResultCode() == ResultCode.OK;
	}
	
	/**
	 * Check if multiple record keys exist in one batch call.
	 * The returned boolean array is in positional order with the original key array order.
	 * The policy can be used to specify timeouts.
	 *  
	 * @param policy				generic configuration parameters
	 * @param keys					array of unique record identifiers
	 * @return						array key/existence status pairs
	 * @throws AerospikeException	if command fails
	 */
	public final boolean[] exists(Policy policy, Key[] keys) throws AerospikeException {
		boolean[] existsArray = new boolean[keys.length];
		BatchExecutor.executeBatch(cluster, policy, keys, existsArray, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		return existsArray;
	}

	//-------------------------------------------------------
	// Read Record Operations
	//-------------------------------------------------------

	/**
	 * Read entire record for specified key.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	public final Record get(Policy policy, Key key) throws AerospikeException {
		SingleCommand command = new SingleCommand(cluster, key);
		command.setRead(Command.INFO1_READ | Command.INFO1_GET_ALL);
		command.begin();
		command.writeHeader(0);
		command.writeKey();
		command.execute(policy);
		return command.getRecord();
	}
	
	/**
	 * Read record header and bins for specified key.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	public final Record get(Policy policy, Key key, String... binNames) throws AerospikeException {	
		SingleCommand command = new SingleCommand(cluster, key);
		command.setRead(Command.INFO1_READ);
		
		for (String binName : binNames) {
			command.estimateOperationSize(binName);
		}
		command.begin();
		command.writeHeader(binNames.length);
		command.writeKey();
		
		for (String binName : binNames) {
			command.writeOperation(binName, Operation.READ);
		}
		command.execute(policy);
		return command.getRecord();
	}
	
	/**
	 * Read record generation and expiration only for specified key.  Bins are not read.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	public final Record getHeader(Policy policy, Key key) throws AerospikeException {
		SingleCommand command = new SingleCommand(cluster, key);
		command.setRead(Command.INFO1_READ | Command.INFO1_NOBINDATA);
		command.begin();
		command.writeHeader(0);
		command.writeKey();
		command.execute(policy);
		return command.getRecord();
	}

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	/**
	 * Read multiple records for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters
	 * @param keys					array of unique record identifiers
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public final Record[] get(Policy policy, Key[] keys) throws AerospikeException {
		Record[] records = new Record[keys.length];
		BatchExecutor.executeBatch(cluster, policy, keys, null, records, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
		return records;
	}
	
	/**
	 * Read multiple record headers and bins for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public final Record[] get(Policy policy, Key[] keys, String... binNames) 
		throws AerospikeException {
		Record[] records = new Record[keys.length];
		
		// Create lookup table for bin name filtering.
		HashSet<String> names = new HashSet<String>(binNames.length);
		
		for (String binName : binNames) {
			names.add(binName);
		}
		BatchExecutor.executeBatch(cluster, policy, keys, null, records, names, Command.INFO1_READ);
		return records;
	}
	
	/**
	 * Read multiple record header data for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters
	 * @param keys					array of unique record identifiers
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public final Record[] getHeader(Policy policy, Key[] keys) throws AerospikeException {
		Record[] records = new Record[keys.length];
		BatchExecutor.executeBatch(cluster, policy, keys, null, records, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		return records;
	}
	
	//-------------------------------------------------------
	// Generic Database Operations
	//-------------------------------------------------------

	/**
	 * Perform multiple read/write operations on a single key in one batch call.
	 * An example would be to add an integer value to an existing record and then
	 * read the result, all in one database call.
	 * 
	 * @param policy				write configuration parameters
	 * @param key					unique record identifier
	 * @param operations			list of database operations
	 * @return						record if there is a read in the operations list
	 * @throws AerospikeException	if command fails
	 */
	public final Record operate(WritePolicy policy, Key key, Operation... operations) 
		throws AerospikeException {
		
		SingleCommand command = new SingleCommand(cluster, key);
		Value[] values = new Value[operations.length];
		int readAttr = 0;
		int writeAttr = 0;
					
		for (int i = 0; i < operations.length; i++) {
			Operation operation = operations[i];
			
			if (operation.type == Operation.READ) {
				readAttr |= Command.INFO1_READ;
				
				// Overloaded bin value means read record header only (no bins). 
				if (operation.binValue != null) {
					readAttr |= Command.INFO1_NOBINDATA;
				}
				// Read all bins if no bin is specified.
				else if (operation.binName == null) {
					readAttr |= Command.INFO1_GET_ALL;
				}
			}
			else {
				writeAttr = Command.INFO2_WRITE;
			}
			Value value = Value.getValue(operation.binValue);
			command.estimateOperationSize(operation.binName, value);
			values[i] = value;
		}
		command.setRead(readAttr);
		command.setWrite(writeAttr);
		command.begin();
		
		if (writeAttr != 0) {
			command.writeHeader(policy, operations.length);
		}
		else {
			command.writeHeader(operations.length);			
		}
		command.writeKey();
						
		for (int i = 0; i < operations.length; i++) {
			Operation operation = operations[i];
			command.writeOperation(operation.binName, values[i], operation.type);
		}
		command.execute(policy);
		return command.getRecord();
	}
	
	//-------------------------------------------------------
	// Scan Operations
	//-------------------------------------------------------
	
	/**
	 * Read all records in specified namespace and set.  If the policy's 
	 * <code>concurrentNodes</code> is specified, each server node will be read in
	 * parallel.  Otherwise, server nodes are read in series.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param policy				scan configuration parameters
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @throws AerospikeException	if scan fails
	 */
	public final void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback)
		throws AerospikeException {
		
		if (policy == null) {
			policy = new ScanPolicy();
		}
		
		// Retry policy must be one-shot for scans.
		policy.retryPolicy = RetryPolicy.ONCE;
		
		Node[] nodes = cluster.getNodes();

		if (policy.concurrentNodes) {
			ScanExecutor executor = new ScanExecutor(policy, namespace, setName, callback);
			executor.scanParallel(nodes);
		}
		else {
			for (Node node : nodes) {
				scanNode(policy, node, namespace, setName, callback);
			}
		}
	}

	/**
	 * Read all records in specified namespace and set for one node only.
	 * The node is specified by name.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param policy				scan configuration parameters
	 * @param nodeName				server node name;
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @throws AerospikeException	if scan fails
	 */
	public final void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback) 
		throws AerospikeException {
		
		Node node = cluster.getNode(nodeName);
		scanNode(policy, node, namespace, setName, callback);
	}

	/**
	 * Read all records in specified namespace and set for one node only.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param policy				generic configuration parameters
	 * @param node					server node
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @throws AerospikeException	if transaction fails
	 */
	private final void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback) 
		throws AerospikeException {
		
		if (policy == null) {
			policy = new ScanPolicy();
		}
		
		// Retry policy must be one-shot for scans.
		policy.retryPolicy = RetryPolicy.ONCE;
		
		ScanCommand command = new ScanCommand(node, callback);
		command.scan(policy, namespace, setName);
	}
}
