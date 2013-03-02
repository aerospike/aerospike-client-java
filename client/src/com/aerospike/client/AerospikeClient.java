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
import java.util.Map;

import com.aerospike.client.Info.NameValueParser;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.command.ScanCommand;
import com.aerospike.client.command.ScanExecutor;
import com.aerospike.client.command.SingleCommand;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.QueryExecutor;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.MsgPack;
import com.aerospike.client.util.Util;

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
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public AerospikeClient(ClientPolicy policy, String hostname, int port) throws AerospikeException {
		this(policy, new Host(hostname, port));
	}

	/**
	 * Initialize Aerospike client with suitable hosts to seed the cluster map.
	 * The client policy is used to set defaults and size internal data structures.
	 * For each host connection that succeeds, the client will:
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
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hosts					array of potential hosts to seed the cluster
	 * @throws AerospikeException	if all host connections fail
	 */
	public AerospikeClient(ClientPolicy policy, Host... hosts) throws AerospikeException {
		if (policy == null) {
			policy = new ClientPolicy();
		}
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
	 * Return array of active server nodes in the cluster.
	 * 
	 * @return	array of active nodes
	 */
	public final Node[] getNodes() {
		return cluster.getNodes();
	}

	/**
	 * Return list of active server node names in the cluster.
	 * 
	 * @return	list of active node names
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
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if write fails
	 */
	public final void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		 SingleCommand command = new SingleCommand(cluster, key);
		 command.write(policy, Operation.Type.WRITE, bins);
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
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if append fails
	 */
	public final void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		 SingleCommand command = new SingleCommand(cluster, key);
		 command.write(policy, Operation.Type.APPEND, bins);
	}
	
	/**
	 * Prepend bin string values to existing record bin values.
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call works only for string values. 
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if prepend fails
	 */
	public final void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		 SingleCommand command = new SingleCommand(cluster, key);
		 command.write(policy, Operation.Type.PREPEND, bins);
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
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if add fails
	 */
	public final void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		 SingleCommand command = new SingleCommand(cluster, key);
		 command.write(policy, Operation.Type.ADD, bins);
	}
	
	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------

	/**
	 * Delete record for specified key.
	 * The policy specifies the transaction timeout.
	 * 
	 * @param policy				delete configuration parameters, pass in null for defaults
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
	 * @param policy				write configuration parameters, pass in null for defaults
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
		command.writeOperation(Operation.Type.TOUCH);
		command.execute(policy);
	}

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------

	/**
	 * Determine if a record key exists.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
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
	 * @param policy				generic configuration parameters, pass in null for defaults
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
	 * @param policy				generic configuration parameters, pass in null for defaults
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
	 * @param policy				generic configuration parameters, pass in null for defaults
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
			command.writeOperation(binName, Operation.Type.READ);
		}
		command.execute(policy);
		return command.getRecord();
	}
	
	/**
	 * Read record generation and expiration only for specified key.  Bins are not read.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	public final Record getHeader(Policy policy, Key key) throws AerospikeException {
		SingleCommand command = new SingleCommand(cluster, key);
		// The server does not currently return record header data with INFO1_NOBINDATA attribute set.
		// The workaround is to request a non-existent bin.
		// TODO: Fix this on server.
		//command.setRead(Command.INFO1_READ | Command.INFO1_NOBINDATA);
		command.setRead(Command.INFO1_READ);
		command.estimateOperationSize((String)null);
		command.begin();
		command.writeHeader(0);
		command.writeKey();
		command.writeOperation((String)null, Operation.Type.READ);
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
	 * @param policy				generic configuration parameters, pass in null for defaults
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
	 * @param policy				generic configuration parameters, pass in null for defaults
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
	 * @param policy				generic configuration parameters, pass in null for defaults
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
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @return						record if there is a read in the operations list
	 * @throws AerospikeException	if command fails
	 */
	public final Record operate(WritePolicy policy, Key key, Operation... operations) 
		throws AerospikeException {
		
		SingleCommand command = new SingleCommand(cluster, key);
		int readAttr = 0;
		int writeAttr = 0;
		boolean readHeader = false;
					
		for (Operation operation : operations) {
			switch (operation.type) {
			case READ:
				readAttr |= Command.INFO1_READ;
				
				// Read all bins if no bin is specified.
				if (operation.binName == null) {
					readAttr |= Command.INFO1_GET_ALL;
				}
				break;
				
			case READ_HEADER:
				// The server does not currently return record header data with INFO1_NOBINDATA attribute set.
				// The workaround is to request a non-existent bin.
				// TODO: Fix this on server.
				//readAttr |= Command.INFO1_READ | Command.INFO1_NOBINDATA;
				readAttr |= Command.INFO1_READ;
				readHeader = true;
				break;
				
			default:
				writeAttr = Command.INFO2_WRITE;
				break;				
			}
			command.estimateOperationSize(operation);
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
					
		for (Operation operation : operations) {
			command.writeOperation(operation);
		}
		
		if (readHeader) {
			command.writeOperation((String)null, Operation.Type.READ);
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
	 * @param policy				scan configuration parameters, pass in null for defaults
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
		policy.maxRetries = 0;
		
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
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param nodeName				server node name;
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @throws AerospikeException	if scan fails
	 */
	public final void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback) 
		throws AerospikeException {
		
		if (policy == null) {
			policy = new ScanPolicy();
		}
		
		// Retry policy must be one-shot for scans.
		policy.maxRetries = 0;

		Node node = cluster.getNode(nodeName);
		scanNode(policy, node, namespace, setName, callback);
	}

	/**
	 * Read all records in specified namespace and set for one node only.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param node					server node
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @throws AerospikeException	if transaction fails
	 */
	private final void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback) 
		throws AerospikeException {

		ScanCommand command = new ScanCommand(node, callback);
		command.scan(policy, namespace, setName);
	}

	//-------------------------------------------------------
	// User defined functions (Supported by 3.0 servers only)
	//-------------------------------------------------------
	
	/**
	 * Register package containing user defined functions with server.
	 * This method is only supported by Aerospike 3.0 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param clientPath			path of client file containing user defined functions
	 * @param serverPath			path to store user defined functions on the server
	 * @param language				language of user defined functions
	 * @throws AerospikeException	if register fails
	 */
	public final void register(Policy policy, String clientPath, String serverPath, Language language) 
		throws AerospikeException {
		
		String content = Util.readFileEncodeBase64(clientPath);
		
		StringBuilder sb = new StringBuilder(serverPath.length() + content.length() + 100);
		sb.append("udf-put:filename=");
		sb.append(serverPath);
		sb.append(";content=");
		sb.append(content);
		sb.append(";content-len=");
		sb.append(content.length());
		sb.append(";udf-type=");
		sb.append(language.id);
		sb.append(";");
		
		// Send registration command to all nodes.
		String command = sb.toString();		
		Node[] nodes = cluster.getNodes();
		int timeout = (policy == null)? 0 : policy.timeout;
		
		for (Node node : nodes) {
			Info info = new Info(node.getConnection(timeout), command);
			NameValueParser parser = info.getNameValueParser();
			
			while (parser.next()) {
				String name = parser.getName();

				if (name.equals("error")) {
					throw new AerospikeException(serverPath + " registration failed: " +  parser.getValue());
				}
			}
		}
	}
	
	/**
	 * Execute user defined function on server and return results.
	 * The function operates on a single record.
	 * This method is only supported by Aerospike 3.0 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param fileName				server file where user defined function resides
	 * @param functionName			user defined function
	 * @param args					arguments passed in to user defined function
	 * @return						return value of user defined function
	 * @throws AerospikeException	if transaction fails
	 */
	public final Object execute(Policy policy, Key key, String fileName, String functionName, Object... args) 
		throws AerospikeException {
		SingleCommand command = new SingleCommand(cluster, key);
		command.setWrite(Command.INFO2_WRITE);
		
		byte[] argBytes = MsgPack.packArray(args);
		command.estimateUdfSize(fileName, functionName, argBytes);
		
		command.begin();
		command.writeHeader(0);
		command.writeKey();
		command.writeField(fileName, FieldType.UDF_FILENAME);
		command.writeField(functionName, FieldType.UDF_FUNCTION);
		command.writeField(argBytes, FieldType.UDF_ARGLIST);
		command.execute(policy);
		
		Record record = command.getRecord();
		
		if (record == null || record.bins == null) {
			return null;
		}
		
		Map<String,Object> map = record.bins;

		Object obj = map.get("SUCCESS");
		
		if (obj != null) {
			return obj;
		}
		
		// User defined functions don't have to return a value.
		if (map.containsKey("SUCCESS")) {
			return null;
		}
		
		obj = map.get("FAILURE");
		
		if (obj != null) {
			throw new AerospikeException(obj.toString());
		}
		throw new AerospikeException("Invalid UDF return value");
	}
	
	//-------------------------------------------------------
	// Query functions (Supported by 3.0 servers only)
	//-------------------------------------------------------

	/**
	 * Execute query and return results.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @return						collection of query results
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet query(QueryPolicy policy, Statement statement) 
		throws AerospikeException {
		
		if (policy == null) {
			policy = new QueryPolicy();
		}
		
		// Retry policy must be one-shot for queries.
		policy.maxRetries = 0;
		
		return new QueryExecutor(policy, statement, cluster.getNodes());
	}
}
