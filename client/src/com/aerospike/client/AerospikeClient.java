/*
 * Copyright 2012-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client;

import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Batch;
import com.aerospike.client.command.BatchExecutor;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.DeleteCommand;
import com.aerospike.client.command.ExecuteCommand;
import com.aerospike.client.command.Executor;
import com.aerospike.client.command.ExistsCommand;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.command.OperateCommand;
import com.aerospike.client.command.ReadCommand;
import com.aerospike.client.command.ReadHeaderCommand;
import com.aerospike.client.command.RegisterCommand;
import com.aerospike.client.command.ScanCommand;
import com.aerospike.client.command.TouchCommand;
import com.aerospike.client.command.WriteCommand;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeMap;
import com.aerospike.client.large.LargeSet;
import com.aerospike.client.large.LargeStack;
import com.aerospike.client.lua.LuaCache;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.QueryAggregateExecutor;
import com.aerospike.client.query.QueryRecordExecutor;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.ServerCommand;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.RandomShift;
import com.aerospike.client.util.Util;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
public class AerospikeClient implements IAerospikeClient, Closeable {
	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------
	
	protected Cluster cluster;
	
	/**
	 * Default read policy that is used when read command policy is null.
	 */
	public final Policy readPolicyDefault;
	
	/**
	 * Default write policy that is used when write command policy is null.
	 */
	public final WritePolicy writePolicyDefault;
	
	/**
	 * Default scan policy that is used when scan command policy is null.
	 */
	public final ScanPolicy scanPolicyDefault;
	
	/**
	 * Default query policy that is used when query command policy is null.
	 */
	public final QueryPolicy queryPolicyDefault;
	
	/**
	 * Default batch policy that is used when batch command policy is null.
	 */
	public final BatchPolicy batchPolicyDefault;

	/**
	 * Default info policy that is used when info command policy is null.
	 */
	public final InfoPolicy infoPolicyDefault;

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
		this.readPolicyDefault = policy.readPolicyDefault;
		this.writePolicyDefault = policy.writePolicyDefault;
		this.scanPolicyDefault = policy.scanPolicyDefault;
		this.queryPolicyDefault = policy.queryPolicyDefault;
		this.batchPolicyDefault = policy.batchPolicyDefault;
		this.infoPolicyDefault = policy.infoPolicyDefault;
		
		cluster = new Cluster(policy, hosts);
		cluster.initTendThread(policy.failIfNotConnected);
	}

	//-------------------------------------------------------
	// Protected Initialization
	//-------------------------------------------------------

	/**
	 * Asynchronous default constructor. Do not use directly.
	 */
	protected AerospikeClient(ClientPolicy policy) {
		if (policy != null) {
			this.readPolicyDefault = policy.readPolicyDefault;
			this.writePolicyDefault = policy.writePolicyDefault;
			this.scanPolicyDefault = policy.scanPolicyDefault;
			this.queryPolicyDefault = policy.queryPolicyDefault;
			this.batchPolicyDefault = policy.batchPolicyDefault;
			this.infoPolicyDefault = policy.infoPolicyDefault;
		}
		else {
			this.readPolicyDefault = new Policy();
			this.writePolicyDefault = new WritePolicy();
			this.scanPolicyDefault = new ScanPolicy();
			this.queryPolicyDefault = new QueryPolicy();
			this.batchPolicyDefault = new BatchPolicy();
			this.infoPolicyDefault = new InfoPolicy();
		}
	}

	//-------------------------------------------------------
	// Default Policies
	//-------------------------------------------------------
	
	public final Policy getReadPolicyDefault() {
		return readPolicyDefault;
	}

	public final WritePolicy getWritePolicyDefault() {
		return writePolicyDefault;
	}

	public final ScanPolicy getScanPolicyDefault() {
		return scanPolicyDefault;
	}

	public final QueryPolicy getQueryPolicyDefault() {
		return queryPolicyDefault;
	}

	public final BatchPolicy getBatchPolicyDefault() {
		return batchPolicyDefault;
	}

	public final InfoPolicy getInfoPolicyDefault() {
		return infoPolicyDefault;
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
	
	/**
	 * Return node given its name.
	 * 
	 * @throws AerospikeException.InvalidNode	if node does not exist.
	 */
	public final Node getNode(String nodeName) throws AerospikeException.InvalidNode {
		return cluster.getNode(nodeName);
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
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.WRITE);
		command.execute();
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
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.APPEND);
		command.execute();
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
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.PREPEND);
		command.execute();
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
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.ADD);
		command.execute();
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
		if (policy == null) {
			policy = writePolicyDefault;
		}
		DeleteCommand command = new DeleteCommand(cluster, policy, key);
		command.execute();
		return command.existed();
	}

	//-------------------------------------------------------
	// Touch Operations
	//-------------------------------------------------------

	/**
	 * Reset record's time to expiration using the policy's expiration.
	 * Fail if the record does not exist.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if touch fails
	 */
	public final void touch(WritePolicy policy, Key key) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		TouchCommand command = new TouchCommand(cluster, policy, key);
		command.execute();
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
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ExistsCommand command = new ExistsCommand(cluster, policy, key);
		command.execute();
		return command.exists();
	}

	/**
	 * Check if multiple record keys exist in one batch call.
	 * The returned boolean array is in positional order with the original key array order.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 *  
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array key/existence status pairs
	 * @throws AerospikeException	if command fails
	 */
	public final boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		boolean[] existsArray = new boolean[keys.length];
		BatchExecutor.execute(cluster, policy, keys, existsArray, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
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
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ReadCommand command = new ReadCommand(cluster, policy, key, null);
		command.execute();
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
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ReadCommand command = new ReadCommand(cluster, policy, key, binNames);
		command.execute();
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
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ReadHeaderCommand command = new ReadHeaderCommand(cluster, policy, key);
		command.execute();
		return command.getRecord();
	}

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	/**
	 * Read multiple records for specified batch keys in one batch call.
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 * This method requires Aerospike Server version >= 3.6.0.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if read fails
	 */
	public final void get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		if (records.size() == 0) {
			return;
		}
		
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		
		List<BatchNode> batchNodes = BatchNode.generateList(cluster, policy, records);

		if (policy.maxConcurrentThreads == 1 || batchNodes.size() <= 1) {
			// Run batch requests sequentially in same thread.
			for (BatchNode batchNode : batchNodes) {
				if (! batchNode.node.hasBatchIndex()) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
				}			
				MultiCommand command = new Batch.ReadListCommand(batchNode, policy, records);
				command.execute();
			}
		}
		else {
			// Run batch requests in parallel in separate threads.
			//			
			// Multiple threads write to the record list, so one might think that
			// volatile or memory barriers are needed on the write threads and this read thread.
			// This should not be necessary here because it happens in Executor which does a 
			// volatile write (completedCount.incrementAndGet()) at the end of write threads
			// and a synchronized waitTillComplete() in this thread.
			Executor executor = new Executor(cluster, batchNodes.size());

			for (BatchNode batchNode : batchNodes) {
				if (! batchNode.node.hasBatchIndex()) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
				}		
				MultiCommand command = new Batch.ReadListCommand(batchNode, policy, records);
				executor.addCommand(command);
			}
			executor.execute(policy.maxConcurrentThreads);
		}		
	}

	/**
	 * Read multiple records for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public final Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		Record[] records = new Record[keys.length];
		BatchExecutor.execute(cluster, policy, keys, null, records, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
		return records;
	}

	/**
	 * Read multiple record headers and bins for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public final Record[] get(BatchPolicy policy, Key[] keys, String... binNames) 
		throws AerospikeException {
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		Record[] records = new Record[keys.length];
		BatchExecutor.execute(cluster, policy, keys, null, records, binNames, Command.INFO1_READ);
		return records;
	}

	/**
	 * Read multiple record header data for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public final Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		Record[] records = new Record[keys.length];
		BatchExecutor.execute(cluster, policy, keys, null, records, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		return records;
	}

	//-------------------------------------------------------
	// Generic Database Operations
	//-------------------------------------------------------

	/**
	 * Perform multiple read/write operations on a single key in one batch call.
	 * An example would be to add an integer value to an existing record and then
	 * read the result, all in one database call.
	 * <p>
	 * Write operations are always performed first, regardless of operation order 
	 * relative to read operations.
	 * <p>
	 * Both scalar bin operations (Operation) and list bin operations (ListOperation)
	 * can be performed in same call.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @return						record if there is a read in the operations list
	 * @throws AerospikeException	if command fails
	 */
	public final Record operate(WritePolicy policy, Key key, Operation... operations) 
		throws AerospikeException {		
		if (policy == null) {
			policy = writePolicyDefault;
		}
		OperateCommand command = new OperateCommand(cluster, policy, key, operations);
		command.execute();
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
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if scan fails
	 */
	public final void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {	
		if (policy == null) {
			policy = scanPolicyDefault;
		}
		
		Node[] nodes = cluster.getNodes();		
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.");
		}

		if (policy.concurrentNodes) {
			Executor executor = new Executor(cluster, nodes.length);
			long taskId = RandomShift.instance().nextLong();

			for (Node node : nodes)
			{
				ScanCommand command = new ScanCommand(node, policy, namespace, setName, callback, binNames, taskId);
				executor.addCommand(command);
			}

			executor.execute(policy.maxConcurrentNodes);			
		}
		else {
			for (Node node : nodes) {
				scanNode(policy, node, namespace, setName, callback, binNames);
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
	 * @param nodeName				server node name
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.  
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if scan fails
	 */
	public final void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames) 
		throws AerospikeException {		
		Node node = cluster.getNode(nodeName);
		scanNode(policy, node, namespace, setName, callback, binNames);
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
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.  
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if transaction fails
	 */
	public final void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames) 
		throws AerospikeException {
		if (policy == null) {
			policy = scanPolicyDefault;
		}
		long taskId = RandomShift.instance().nextLong();

		ScanCommand command = new ScanCommand(node, policy, namespace, setName, callback, binNames, taskId);
		command.execute();
	}

	//-------------------------------------------------------------------
	// Large collection functions
	//-------------------------------------------------------------------

	/**
	 * Initialize large list operator.  This operator can be used to create and manage a list 
	 * within a single bin.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 */
	public final LargeList getLargeList(WritePolicy policy, Key key, String binName) {
		return new LargeList(this, policy, key, binName);
	}

	/**
	 * Initialize large map operator.  This operator can be used to create and manage a map 
	 * within a single bin.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public final LargeMap getLargeMap(WritePolicy policy, Key key, String binName, String userModule) {
		return new LargeMap(this, policy, key, binName, userModule);
	}

	/**
	 * Initialize large set operator.  This operator can be used to create and manage a set 
	 * within a single bin.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public final LargeSet getLargeSet(WritePolicy policy, Key key, String binName, String userModule) {
		return new LargeSet(this, policy, key, binName, userModule);
	}

	/**
	 * Initialize large stack operator.  This operator can be used to create and manage a stack 
	 * within a single bin.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public final LargeStack getLargeStack(WritePolicy policy, Key key, String binName, String userModule) {
		return new LargeStack(this, policy, key, binName, userModule);
	}

	//---------------------------------------------------------------
	// User defined functions
	//---------------------------------------------------------------
	
	/**
	 * Register package located in a file containing user defined functions with server.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param clientPath			path of client file containing user defined functions, relative to current directory
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @param language				language of user defined functions
	 * @throws AerospikeException	if register fails
	 */
	public final RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) 
		throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		File file = new File(clientPath);
		byte[] bytes = Util.readFile(file);
		return RegisterCommand.register(cluster, policy, bytes, serverPath, language);
	}
	
	/**
	 * Register package located in a resource containing user defined functions with server.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param resourceLoader		class loader where resource is located.  Example: MyClass.class.getClassLoader() or Thread.currentThread().getContextClassLoader() for webapps
	 * @param resourcePath          class path where Lua resource is located
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @param language				language of user defined functions
	 * @throws AerospikeException	if register fails
	 */
	public final RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath, Language language) 
		throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		byte[] bytes = Util.readResource(resourceLoader, resourcePath);
		return RegisterCommand.register(cluster, policy, bytes, serverPath, language);
	}

	/**
	 * Register UDF functions located in a code string with server.  Example:
	 * <pre>
	 * {@code
	 * String code = 
	 *   "local function reducer(val1,val2)\n" + 
	 *   "  return val1 + val2\n" + 
	 *   "end\n" +
	 *   "\n" +
	 *   "function sum_single_bin(stream,name)\n" +
	 *   "  local function mapper(rec)\n" +
	 *   "    return rec[name]\n" +
	 *   "  end\n" +
	 *   "  return stream : map(mapper) : reduce(reducer)\n" +
	 *   "end\n";
	 *   
	 * client.registerUdfString(null, code, "mysum.lua", Language.LUA);
	 * }
	 * </pre>
	 * <p>
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param code					code string containing user defined functions.
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @param language				language of user defined functions
	 * @throws AerospikeException	if register fails
	 */
	public final RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) 
		throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		byte[] bytes = Buffer.stringToUtf8(code);
		return RegisterCommand.register(cluster, policy, bytes, serverPath, language);
	}
	
	/**
	 * Remove user defined function from server nodes.
	 * 
	 * @param policy				info configuration parameters, pass in null for defaults
	 * @param serverPath			location of UDF on server nodes.  Example: mylua.lua
	 * @throws AerospikeException	if remove fails
	 */
	public final void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException {
		if (policy == null) {
			policy = infoPolicyDefault;
		}
		// Send UDF command to one node. That node will distribute the UDF command to other nodes.
		String command = "udf-remove:filename=" + serverPath;
		Node node = cluster.getRandomNode();		
		String response = Info.request(policy, node, command);
		
		if (response.equalsIgnoreCase("ok")) {
			// Clear LuaCache as it still references the old UDF.
			LuaCache.clearPackageFromCache(serverPath);
			return;
		}
		
		if (response.startsWith("error=file_not_found")) {
			// UDF has already been removed.
			return;
		}
		throw new AerospikeException("Remove UDF failed: " + response);
	}
	
	/**
	 * Execute user defined function on server and return results.
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param args					arguments passed in to user defined function
	 * @return						return value of user defined function
	 * @throws AerospikeException	if transaction fails
	 */
	public final Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) 
		throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
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
	// Query/Execute UDF
	//----------------------------------------------------------

	/**
	 * Apply user defined function on records that match the statement filter.
	 * Records are not returned to the client.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * ExecuteTask instance.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param statement				record filter
	 * @param packageName			server package where user defined function resides
	 * @param functionName			function name
	 * @param functionArgs			to pass to function name, if any
	 * @throws AerospikeException	if command fails
	 */
	public final ExecuteTask execute(
		WritePolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		statement.setAggregateFunction(packageName, functionName, functionArgs);
		statement.prepare(false);

		Node[] nodes = cluster.getNodes();
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
		}

		Executor executor = new Executor(cluster, nodes.length);

		for (Node node : nodes)
		{
			ServerCommand command = new ServerCommand(node, policy, statement);
			executor.addCommand(command);
		}
		executor.execute(nodes.length);
		return new ExecuteTask(cluster, policy, statement);
	}

	//--------------------------------------------------------
	// Query functions
	//--------------------------------------------------------

	/**
	 * Execute query on all server nodes and return record iterator.  The query executor puts 
	 * records on a queue in separate threads.  The calling thread concurrently pops records off 
	 * the queue through the record iterator.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}
		QueryRecordExecutor executor = new QueryRecordExecutor(cluster, policy, statement, null);
		executor.execute();
		return executor.getRecordSet();
	}
	
	/**
	 * Execute query on a single server node and return record iterator.  The query executor puts
	 * records on a queue in a separate thread.  The calling thread concurrently pops records off 
	 * the queue through the record iterator.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @param node					server node to execute query
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}
		QueryRecordExecutor executor = new QueryRecordExecutor(cluster, policy, statement, node);
		executor.execute();
		return executor.getRecordSet();
	}

	/**
	 * Execute query, apply statement's aggregation function, and return result iterator. The query 
	 * executor puts results on a queue in separate threads.  The calling thread concurrently pops 
	 * results off the queue through the result iterator.
	 * <p>
	 * The aggregation function is called on both server and client (final reduce).  Therefore,
	 * the Lua script files must also reside on both server and client.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <udf dir>/<package name>.lua
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @param packageName			server package where user defined function resides
	 * @param functionName			aggregation function name
	 * @param functionArgs			arguments to pass to function name, if any
	 * @return						result iterator
	 * @throws AerospikeException	if query fails
	 */
	public final ResultSet queryAggregate(
		QueryPolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException {
		statement.setAggregateFunction(packageName, functionName, functionArgs);
		return queryAggregate(policy, statement);
	}

	/**
	 * Execute query, apply statement's aggregation function, and return result iterator.
	 * The aggregation function should be initialized via the statement's setAggregateFunction()
	 * and should be located in a resource or a filesystem file.
	 * <p>
	 * The query executor puts results on a queue in separate threads.  The calling thread
	 * concurrently pops results off the queue through the ResultSet iterator.
	 * The aggregation function is called on both server and client (final reduce).
	 * Therefore, the Lua script file must also reside on both server and client.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @throws AerospikeException	if query fails
	 */
	public final ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}		
		statement.prepare(true);
		QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, null);
		return executor.getResultSet();
	}

	/**
	 * Execute query on a single server node, apply statement's aggregation function, and return 
	 * result iterator.
	 * The aggregation function should be initialized via the statement's setAggregateFunction()
	 * and should be located in a resource or a filesystem file.
	 * <p>
	 * The query executor puts results on a queue in separate threads.  The calling thread
	 * concurrently pops results off the queue through the ResultSet iterator.
	 * The aggregation function is called on both server and client (final reduce).
	 * Therefore, the Lua script file must also reside on both server and client.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @param node					server node to execute query
	 * @throws AerospikeException	if query fails
	 */
	public final ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}		
		statement.prepare(true);
		QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, node);
		return executor.getResultSet();
	}

	/**
	 * Create scalar secondary index.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @throws AerospikeException	if index create fails
	 */
	public final IndexTask createIndex(
		Policy policy, 
		String namespace, 
		String setName, 
		String indexName, 
		String binName,
		IndexType indexType
	) throws AerospikeException {
		return createIndex(policy, namespace, setName, indexName, binName, indexType, IndexCollectionType.DEFAULT);	
	}

	/**
	 * Create complex secondary index to be used on bins containing collections.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @throws AerospikeException	if index create fails
	 */
	public final IndexTask createIndex(
		Policy policy, 
		String namespace, 
		String setName, 
		String indexName, 
		String binName,
		IndexType indexType,
		IndexCollectionType indexCollectionType
	) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
						
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
		
		if (indexCollectionType != IndexCollectionType.DEFAULT) {
			sb.append(";indextype=");
			sb.append(indexCollectionType);
		}

		sb.append(";indexdata=");
		sb.append(binName);
		sb.append(",");
		sb.append(indexType);
		sb.append(";priority=normal");

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(policy, sb.toString());
		
		if (response.equalsIgnoreCase("OK")) {
			// Return task that could optionally be polled for completion.
			return new IndexTask(cluster, policy, namespace, indexName);
		}
		
		if (response.startsWith("FAIL:200")) {
			// Index has already been created.  Do not need to poll for completion.
			return new IndexTask();
		}
			
		throw new AerospikeException("Create index failed: " + response);
	}

	/**
	 * Delete secondary index.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @throws AerospikeException	if index create fails
	 */
	public final void dropIndex(
		Policy policy, 
		String namespace, 
		String setName, 
		String indexName
	) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
						
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
	// User administration
	//-------------------------------------------------------

	/**
	 * Create user with password and roles.  Clear-text password will be hashed using bcrypt
	 * before sending to server.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param password				user password in clear-text format
	 * @param roles					variable arguments array of role names.  Predefined roles are listed in Role.cs
	 * @throws AerospikeException	if command fails
	 */
	public final void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {
		String hash = AdminCommand.hashPassword(password);
		AdminCommand command = new AdminCommand();
		command.createUser(cluster, policy, user, hash, roles);
	}
	
	/**
	 * Remove user from cluster.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @throws AerospikeException	if command fails
	 */
	public final void dropUser(AdminPolicy policy, String user) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.dropUser(cluster, policy, user);
	}

	/**
	 * Change user's password.  Clear-text password will be hashed using bcrypt before sending to server.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param password				user password in clear-text format
	 * @throws AerospikeException	if command fails
	 */
	public final void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
		if (cluster.getUser() == null) {
			throw new AerospikeException("Invalid user");
		}

		String hash = AdminCommand.hashPassword(password);
		AdminCommand command = new AdminCommand();
		byte[] userBytes = Buffer.stringToUtf8(user);

		if (Arrays.equals(userBytes, cluster.getUser())) {
			// Change own password.
			command.changePassword(cluster, policy, userBytes, hash);
		}
		else {
			// Change other user's password by user admin.
			command.setPassword(cluster, policy, userBytes, hash);
		}
		cluster.changePassword(userBytes, hash);
	}

	/**
	 * Add roles to user's list of roles.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param roles					role names.  Predefined roles are listed in Role.cs
	 * @throws AerospikeException	if command fails
	 */
	public final void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.grantRoles(cluster, policy, user, roles);
	}

	/**
	 * Remove roles from user's list of roles.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param roles					role names.  Predefined roles are listed in Role.cs
	 * @throws AerospikeException	if command fails
	 */
	public final void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.revokeRoles(cluster, policy, user, roles);
	}

	/**
	 * Create user defined role.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges assigned to the role.
	 * @throws AerospikeException	if command fails
	 */
	public final void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.createRole(cluster, policy, roleName, privileges);
	}

	/**
	 * Drop user defined role.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @throws AerospikeException	if command fails
	 */
	public final void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.dropRole(cluster, policy, roleName);
	}

	/**
	 * Grant privileges to an user defined role.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges assigned to the role.
	 * @throws AerospikeException	if command fails
	 */
	public final void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.grantPrivileges(cluster, policy, roleName, privileges);
	}

	/**
	 * Revoke privileges from an user defined role.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges assigned to the role.
	 * @throws AerospikeException	if command fails
	 */
	public final void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.revokePrivileges(cluster, policy, roleName, privileges);
	}

	/**
	 * Retrieve roles for a given user.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name filter
	 * @throws AerospikeException	if command fails
	 */
	public final User queryUser(AdminPolicy policy, String user) throws AerospikeException {
		AdminCommand.UserCommand command = new AdminCommand.UserCommand(1);
		return command.queryUser(cluster, policy, user);
	}

	/**
	 * Retrieve all users and their roles.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @throws AerospikeException	if command fails
	 */
	public final List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
		AdminCommand.UserCommand command = new AdminCommand.UserCommand(100);
		return command.queryUsers(cluster, policy);
	}

	/**
	 * Retrieve role definition.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name filter
	 * @throws AerospikeException	if command fails
	 */
	public final Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
		AdminCommand.RoleCommand command = new AdminCommand.RoleCommand(1);
		return command.queryRole(cluster, policy, roleName);
	}

	/**
	 * Retrieve all roles.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @throws AerospikeException	if command fails
	 */
	public final List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
		AdminCommand.RoleCommand command = new AdminCommand.RoleCommand(100);
		return command.queryRoles(cluster, policy);
	}

	//-------------------------------------------------------
	// Internal Methods
	//-------------------------------------------------------
	
	private String sendInfoCommand(Policy policy, String command) throws AerospikeException {		
		Node node = cluster.getRandomNode();
		Connection conn = node.getConnection(policy.timeout);
		Info info;
		
		try {
			info = new Info(conn, command);
			node.putConnection(conn);
		}
		catch (RuntimeException re) {
			node.closeConnection(conn);
			throw re;
		}
		return info.getValue();
	}
}
