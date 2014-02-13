/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.aerospike.client.Info.NameValueParser;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.DeleteCommand;
import com.aerospike.client.command.ExecuteCommand;
import com.aerospike.client.command.ExistsCommand;
import com.aerospike.client.command.OperateCommand;
import com.aerospike.client.command.ReadCommand;
import com.aerospike.client.command.ReadHeaderCommand;
import com.aerospike.client.command.ScanCommand;
import com.aerospike.client.command.ScanExecutor;
import com.aerospike.client.command.TouchCommand;
import com.aerospike.client.command.WriteCommand;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeMap;
import com.aerospike.client.large.LargeSet;
import com.aerospike.client.large.LargeStack;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.QueryAggregateExecutor;
import com.aerospike.client.query.QueryRecordExecutor;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.ServerExecutor;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.Environment;
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
public class DefaultAerospikeClient implements AerospikeClient {
	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------
	
	protected Cluster cluster;
	
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
	public DefaultAerospikeClient(String hostname, int port) throws AerospikeException {
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
	public DefaultAerospikeClient(ClientPolicy policy, String hostname, int port) throws AerospikeException {
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
	public DefaultAerospikeClient(ClientPolicy policy, Host... hosts) throws AerospikeException {
		if (policy == null) {
			policy = new ClientPolicy();
		}
		cluster = new Cluster(policy, hosts);
		cluster.initTendThread();
		
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
	protected DefaultAerospikeClient() {
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
		cluster.initTendThread();
	}

	//-------------------------------------------------------
	// Cluster Connection Management
	//-------------------------------------------------------
		
	@Override
    public final void close() {
		cluster.close();
	}

	@Override
    public final boolean isConnected() {
		return cluster.isConnected();
	}

	@Override
    public final Node[] getNodes() {
		return cluster.getNodes();
	}

	@Override
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
	
	@Override
    public final void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.WRITE);
		command.execute();
	}

	//-------------------------------------------------------
	// String Operations
	//-------------------------------------------------------
	
	@Override
    public final void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.APPEND);
		command.execute();
	}
	
	@Override
    public final void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.PREPEND);
		command.execute();
	}

	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------
	
	@Override
    public final void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.ADD);
		command.execute();
	}

	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------

	@Override
    public final boolean delete(WritePolicy policy, Key key) throws AerospikeException {
		DeleteCommand command = new DeleteCommand(cluster, policy, key);
		command.execute();
		return command.existed();
	}

	//-------------------------------------------------------
	// Touch Operations
	//-------------------------------------------------------

	@Override
    public final void touch(WritePolicy policy, Key key) throws AerospikeException {
		TouchCommand command = new TouchCommand(cluster, policy, key);
		command.execute();
	}

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------

	@Override
    public final boolean exists(Policy policy, Key key) throws AerospikeException {
		ExistsCommand command = new ExistsCommand(cluster, policy, key);
		command.execute();
		return command.exists();
	}

	@Override
    public final boolean[] exists(Policy policy, Key[] keys) throws AerospikeException {
		boolean[] existsArray = new boolean[keys.length];
		new BatchExecutor(cluster, policy, keys, existsArray, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		return existsArray;
	}

	//-------------------------------------------------------
	// Read Record Operations
	//-------------------------------------------------------

	@Override
    public final Record get(Policy policy, Key key) throws AerospikeException {
		ReadCommand command = new ReadCommand(cluster, policy, key, null);
		command.execute();
		return command.getRecord();
	}

	@Override
    public final Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
		ReadCommand command = new ReadCommand(cluster, policy, key, binNames);
		command.execute();
		return command.getRecord();
	}

	@Override
    public final Record getHeader(Policy policy, Key key) throws AerospikeException {
		ReadHeaderCommand command = new ReadHeaderCommand(cluster, policy, key);
		command.execute();
		return command.getRecord();
	}

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	@Override
    public final Record[] get(Policy policy, Key[] keys) throws AerospikeException {
		Record[] records = new Record[keys.length];
		new BatchExecutor(cluster, policy, keys, null, records, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
		return records;
	}

	@Override
    public final Record[] get(Policy policy, Key[] keys, String... binNames)
		throws AerospikeException {
		Record[] records = new Record[keys.length];
		HashSet<String> names = binNamesToHashSet(binNames);
		new BatchExecutor(cluster, policy, keys, null, records, names, Command.INFO1_READ);
		return records;
	}

	@Override
    public final Record[] getHeader(Policy policy, Key[] keys) throws AerospikeException {
		Record[] records = new Record[keys.length];
		new BatchExecutor(cluster, policy, keys, null, records, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		return records;
	}

	//-------------------------------------------------------
	// Generic Database Operations
	//-------------------------------------------------------

	@Override
    public final Record operate(WritePolicy policy, Key key, Operation... operations)
		throws AerospikeException {		
		OperateCommand command = new OperateCommand(cluster, policy, key, operations);
		command.execute();
		return command.getRecord();
	}

	//-------------------------------------------------------
	// Scan Operations
	//-------------------------------------------------------
	
	@Override
    public final void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {	
		if (policy == null) {
			policy = new ScanPolicy();
		}
		// Retry policy must be one-shot for scans.
		policy.maxRetries = 0;
		
		Node[] nodes = cluster.getNodes();		
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.");
		}

		if (policy.concurrentNodes) {
			ScanExecutor executor = new ScanExecutor(cluster, nodes, policy, namespace, setName, callback, binNames);
			executor.scanParallel();
		}
		else {
			for (Node node : nodes) {
				scanNode(policy, node, namespace, setName, callback, binNames);
			}
		}
	}

	@Override
    public final void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {		
		Node node = cluster.getNode(nodeName);
		scanNode(policy, node, namespace, setName, callback, binNames);
	}

	@Override
    public final void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = new ScanPolicy();
		}
		// Retry policy must be one-shot for scans.
		policy.maxRetries = 0;

		ScanCommand command = new ScanCommand(node, policy, namespace, setName, callback, binNames);
		command.execute();
	}

	//-------------------------------------------------------------------
	// Large collection functions (Supported by Aerospike 3 servers only)
	//-------------------------------------------------------------------

	@Override
    public final LargeList getLargeList(Policy policy, Key key, String binName, String userModule) {
		return new LargeList(this, policy, key, binName, userModule);
	}	

	@Override
    public final LargeMap getLargeMap(Policy policy, Key key, String binName, String userModule) {
		return new LargeMap(this, policy, key, binName, userModule);
	}	

	@Override
    public final LargeSet getLargeSet(Policy policy, Key key, String binName, String userModule) {
		return new LargeSet(this, policy, key, binName, userModule);
	}	

	@Override
    public final LargeStack getLargeStack(Policy policy, Key key, String binName, String userModule) {
		return new LargeStack(this, policy, key, binName, userModule);
	}	

	//---------------------------------------------------------------
	// User defined functions (Supported by Aerospike 3 servers only)
	//---------------------------------------------------------------
	
	@Override
    public final RegisterTask register(Policy policy, String clientPath, String serverPath, Language language)
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
		sb.append(language);
		sb.append(";");
		
		// Send UDF to one node. That node will distribute the UDF to other nodes.
		String command = sb.toString();
		Node node = cluster.getRandomNode();
		int timeout = (policy == null)? 0 : policy.timeout;
		Connection conn = node.getConnection(timeout);
		
		try {			
			Info info = new Info(conn, command);
			NameValueParser parser = info.getNameValueParser();
			String error = null;
			String file = null;
			String line = null;
			String message = null;
			
			while (parser.next()) {
				String name = parser.getName();

				if (name.equals("error")) {
					error = parser.getValue();
				}
				else if (name.equals("file")) {
					file = parser.getValue();				
				}
				else if (name.equals("line")) {
					line = parser.getValue();				
				}
				else if (name.equals("message")) {
					message = parser.getStringBase64();					
				}
			}
			
			if (error != null) {			
				throw new AerospikeException("Registration failed: " + error + Environment.Newline +
					"File: " + file + Environment.Newline + 
					"Line: " + line + Environment.Newline +
					"Message: " + message
					);
			}
			
			node.putConnection(conn);
			return new RegisterTask(cluster, serverPath);
		}
		catch (AerospikeException ae) {
			conn.close();
			throw ae;
		}
		catch (RuntimeException re) {
			conn.close();
			throw new AerospikeException(re);
		}
	}
	
	@Override
    public final Object execute(Policy policy, Key key, String packageName, String functionName, Value... args)
		throws AerospikeException {
		ExecuteCommand command = new ExecuteCommand(cluster, policy, key, packageName, functionName, args);
		command.execute();
		
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
	
	//----------------------------------------------------------
	// Query/Execute UDF (Supported by Aerospike 3 servers only)
	//----------------------------------------------------------

	@Override
    public final ExecuteTask execute(
            Policy policy,
            Statement statement,
            String packageName,
            String functionName,
            Value... functionArgs
    ) throws AerospikeException {
		if (policy == null) {
			policy = new Policy();
		}
		new ServerExecutor(cluster, policy, statement, packageName, functionName, functionArgs);
		return new ExecuteTask(cluster, statement);
	}

	//--------------------------------------------------------
	// Query functions (Supported by Aerospike 3 servers only)
	//--------------------------------------------------------

	@Override
    public final RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
		if (policy == null) {
			policy = new QueryPolicy();
		}
		QueryRecordExecutor executor = new QueryRecordExecutor(cluster, policy, statement);
		executor.execute();
		return executor.getRecordSet();
	}
	
	@Override
    public final ResultSet queryAggregate(
            QueryPolicy policy,
            Statement statement,
            String packageName,
            String functionName,
            Value... functionArgs
    ) throws AerospikeException {
		if (policy == null) {
			policy = new QueryPolicy();
		}
		QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, packageName, functionName, functionArgs);
		executor.execute();
		return executor.getResultSet();
	}

	@Override
    public final IndexTask createIndex(
            Policy policy,
            String namespace,
            String setName,
            String indexName,
            String binName,
            IndexType indexType
    ) throws AerospikeException {
						
		StringBuilder sb = new StringBuilder(500);
		sb.append("sindex-create:ns=");
		sb.append(namespace);
		
		if (setName != null && setName.length() > 0) {
			sb.append(";set=");
			sb.append(setName);
		}
		
		sb.append(";indexname=");
		sb.append(indexName);
		sb.append(";numbins=1");
		sb.append(";indexdata=");
		sb.append(binName);
		sb.append(",");
		sb.append(indexType);
		sb.append(";priority=normal");

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(policy, sb.toString());
		
		if (response.equalsIgnoreCase("OK")) {
			// Return task that could optionally be polled for completion.
			return new IndexTask(cluster, namespace, indexName);
		}
		
		if (response.startsWith("FAIL:200")) {
			// Index has already been created.  Do not need to poll for completion.
			return new IndexTask();
		}
			
		throw new AerospikeException("Create index failed: " + response);
	}

	@Override
    public final void dropIndex(
            Policy policy,
            String namespace,
            String setName,
            String indexName
    ) throws AerospikeException {
						
		StringBuilder sb = new StringBuilder(500);
		sb.append("sindex-delete:ns=");
		sb.append(namespace);
		
		if (setName != null && setName.length() > 0) {
			sb.append(";set=");
			sb.append(setName);
		}		
		sb.append(";indexname=");
		sb.append(indexName);
		
		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(policy, sb.toString());

		if (response.equalsIgnoreCase("OK")) {
			return;
		}
		
		if (response.startsWith("FAIL:201")) {
			// Index did not previously exist. Return without error.
			return;
		}
			
		throw new AerospikeException("Drop index failed: " + response);
	}
	
	//-------------------------------------------------------
	// Internal Methods
	//-------------------------------------------------------

	protected static HashSet<String> binNamesToHashSet(String[] binNames) {
		// Create lookup table for bin name filtering.
		HashSet<String> names = new HashSet<String>(binNames.length);
		
		for (String binName : binNames) {
			names.add(binName);
		}
		return names;
	}
	
	private String sendInfoCommand(Policy policy, String command) throws AerospikeException {		
		Node node = cluster.getRandomNode();
		int timeout = (policy == null)? 0 : policy.timeout;
		Connection conn = node.getConnection(timeout);
		Info info;
		
		try {
			info = new Info(conn, command);
			node.putConnection(conn);
		}
		catch (AerospikeException ae) {
			conn.close();
			throw ae;
		}
		catch (RuntimeException re) {
			conn.close();
			throw new AerospikeException(re);
		}
		return info.getValue();
	}
}
