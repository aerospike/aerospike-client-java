/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.AsyncBatch;
import com.aerospike.client.async.AsyncDelete;
import com.aerospike.client.async.AsyncExecute;
import com.aerospike.client.async.AsyncExists;
import com.aerospike.client.async.AsyncIndexTask;
import com.aerospike.client.async.AsyncInfoCommand;
import com.aerospike.client.async.AsyncOperate;
import com.aerospike.client.async.AsyncQueryExecutor;
import com.aerospike.client.async.AsyncQueryPartitionExecutor;
import com.aerospike.client.async.AsyncRead;
import com.aerospike.client.async.AsyncReadHeader;
import com.aerospike.client.async.AsyncScanExecutor;
import com.aerospike.client.async.AsyncScanPartitionExecutor;
import com.aerospike.client.async.AsyncTouch;
import com.aerospike.client.async.AsyncWrite;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
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
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.command.OperateCommand;
import com.aerospike.client.command.ReadCommand;
import com.aerospike.client.command.ReadHeaderCommand;
import com.aerospike.client.command.RegisterCommand;
import com.aerospike.client.command.ScanExecutor;
import com.aerospike.client.command.TouchCommand;
import com.aerospike.client.command.WriteCommand;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
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
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.QueryAggregateExecutor;
import com.aerospike.client.query.QueryPartitionExecutor;
import com.aerospike.client.query.QueryRecordExecutor;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.ServerCommand;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
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

	private final WritePolicy operatePolicyReadDefault;

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
		this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);

		cluster = new Cluster(policy, hosts);
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
			this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);
		}
		else {
			this.readPolicyDefault = new Policy();
			this.writePolicyDefault = new WritePolicy();
			this.scanPolicyDefault = new ScanPolicy();
			this.queryPolicyDefault = new QueryPolicy();
			this.batchPolicyDefault = new BatchPolicy();
			this.infoPolicyDefault = new InfoPolicy();
			this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);
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
	 * <p>
	 * If event loops are defined, the client will send a cluster close signal
	 * to these event loops.  The client instance does not initiate shutdown
	 * until the pending async commands complete.  The close() method, however,
	 * will return before shutdown completes if close() is called from an
	 * event loop thread.  This is done in order to prevent deadlock.
	 * <p>
	 * This close() method will wait for shutdown if the current thread is not
	 * an event loop thread.  It's recommended to call close() from a non event
	 * loop thread for this reason.
	 */
	public void close() {
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
	 */
	public final Node[] getNodes() {
		return cluster.getNodes();
	}

	/**
	 * Return list of active server node names in the cluster.
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
	 * @throws AerospikeException.InvalidNode	if node does not exist.
	 */
	public final Node getNode(String nodeName) throws AerospikeException.InvalidNode {
		return cluster.getNode(nodeName);
	}

	/**
	 * Return operating cluster statistics.
	 */
	public final ClusterStats getClusterStats() {
		return cluster.getStats();
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

	/**
	 * Asynchronously write record bin(s).
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncWrite command = new AsyncWrite(cluster, listener, policy, key, bins, Operation.Type.WRITE);
		eventLoop.execute(cluster, command);
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
	 * Asynchronously append bin string values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncWrite command = new AsyncWrite(cluster, listener, policy, key, bins, Operation.Type.APPEND);
		eventLoop.execute(cluster, command);
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

	/**
	 * Asynchronously prepend bin string values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncWrite command = new AsyncWrite(cluster, listener, policy, key, bins, Operation.Type.PREPEND);
		eventLoop.execute(cluster, command);
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

	/**
	 * Asynchronously add integer bin values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for integer values.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncWrite command = new AsyncWrite(cluster, listener, policy, key, bins, Operation.Type.ADD);
		eventLoop.execute(cluster, command);
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

	/**
	 * Asynchronously delete record for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncDelete command = new AsyncDelete(cluster, listener, policy, key);
		eventLoop.execute(cluster, command);
	}

	/**
	 * Remove records in specified namespace/set efficiently.  This method is many orders of magnitude
	 * faster than deleting records one at a time.
	 * <p>
	 * See <a href="https://www.aerospike.com/docs/reference/info#truncate">https://www.aerospike.com/docs/reference/info#truncate</a>
	 * <p>
	 * This asynchronous server call may return before the truncation is complete.  The user can still
	 * write new records after the server call returns because new records will have last update times
	 * greater than the truncate cutoff (set at the time of truncate call).
	 *
	 * @param policy				info command configuration parameters, pass in null for defaults
	 * @param ns					required namespace
	 * @param set					optional set name.  Pass in null to delete all sets in namespace.
	 * @param beforeLastUpdate		optional delete records before record last update time.
	 * 								If specified, value must be before the current time.
	 * 								Pass in null to delete all records in namespace/set.
	 * @throws AerospikeException	if truncate fails
	 */
	public final void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) throws AerospikeException {
		if (policy == null) {
			policy = infoPolicyDefault;
		}

		// Send truncate command to one node. That node will distribute the command to other nodes.
		Node node = cluster.getRandomNode();

		StringBuilder sb = new StringBuilder(200);

		if (set != null) {
			sb.append("truncate:namespace=");
			sb.append(ns);
			sb.append(";set=");
			sb.append(set);
		}
		else {
			// Servers >= 4.5.1.0 support truncate-namespace.
			if (node.hasTruncateNamespace()) {
				sb.append("truncate-namespace:namespace=");
				sb.append(ns);
			}
			else {
				sb.append("truncate:namespace=");
				sb.append(ns);
			}
		}

		if (beforeLastUpdate != null) {
			sb.append(";lut=");
			// Convert to nanoseconds since unix epoch (1970-01-01)
			sb.append(beforeLastUpdate.getTimeInMillis() * 1000000L);
		}
		else {
			// Servers >= 4.3.1.4 and <= 4.5.0.1 require lut argument.
			if (node.hasLutNow()) {
				sb.append(";lut=now");
			}
		}

		String response = Info.request(policy, node, sb.toString());

		if (! response.equalsIgnoreCase("ok")) {
			throw new AerospikeException("Truncate failed: " + response);
		}
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

	/**
	 * Asynchronously reset record's time to expiration using the policy's expiration.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Fail if the record does not exist.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncTouch command = new AsyncTouch(cluster, listener, policy, key);
		eventLoop.execute(cluster, command);
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
	 * Asynchronously determine if a record key exists.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) throws AerospikeException {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		AsyncExists command = new AsyncExists(cluster, listener, policy, key);
		eventLoop.execute(cluster, command);
	}

	/**
	 * Check if multiple record keys exist in one batch call.
	 * The returned boolean array is in positional order with the original key array order.
	 * The policy can be used to specify timeouts and maximum concurrent nodes.
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

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned boolean array is in positional order with the original key array order.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new boolean[0]);
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.ExistsArrayExecutor(eventLoop, cluster, policy, keys, listener);
	}

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each key's result is returned in separate onExists() calls.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.ExistsSequenceExecutor(eventLoop, cluster, policy, keys, listener);
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
		ReadCommand command = new ReadCommand(cluster, policy, key);
		command.execute();
		return command.getRecord();
	}

	/**
	 * Asynchronously read entire record for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		AsyncRead command = new AsyncRead(cluster, listener, policy, key, null);
		eventLoop.execute(cluster, command);
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
	 * Asynchronously read record header and bins for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames) throws AerospikeException {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		AsyncRead command = new AsyncRead(cluster, listener, policy, key, binNames);
		eventLoop.execute(cluster, command);
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

	/**
	 * Asynchronously read record generation and expiration only for specified key.  Bins are not read.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		AsyncReadHeader command = new AsyncReadHeader(cluster, listener, policy, key);
		eventLoop.execute(cluster, command);
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
				MultiCommand command = new Batch.ReadListCommand(cluster, null, batchNode, policy, records);
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
				MultiCommand command = new Batch.ReadListCommand(cluster, executor, batchNode, policy, records);
				executor.addCommand(command);
			}
			executor.execute(policy.maxConcurrentThreads);
		}
	}

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		if (records.size() == 0) {
			listener.onSuccess(records);
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.ReadListExecutor(eventLoop, cluster, policy, listener, records);
	}

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * Each record result is returned in separate onRecord() calls.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		if (records.size() == 0) {
			listener.onSuccess();
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.ReadSequenceExecutor(eventLoop, cluster, policy, listener, records);
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
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetArrayExecutor(eventLoop, cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
	}

	/**
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetSequenceExecutor(eventLoop, cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
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
	public final Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		Record[] records = new Record[keys.length];
		BatchExecutor.execute(cluster, policy, keys, null, records, binNames, Command.INFO1_READ);
		return records;
	}

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetArrayExecutor(eventLoop, cluster, policy, listener, keys, binNames, Command.INFO1_READ);
	}

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetSequenceExecutor(eventLoop, cluster, policy, listener, keys, binNames, Command.INFO1_READ);
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

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetArrayExecutor(eventLoop, cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
	}

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}
		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetSequenceExecutor(eventLoop, cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
	}

	//-------------------------------------------------------
	// Generic Database Operations
	//-------------------------------------------------------

	/**
	 * Perform multiple read/write operations on a single key in one batch call.
	 * An example would be to add an integer value to an existing record and then
	 * read the result, all in one database call.
	 * <p>
	 * The server executes operations in the same order as the operations array.
	 * Both scalar bin operations (Operation) and CDT bin operations (ListOperation,
	 * MapOperation) can be performed in same call.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @return						record if there is a read in the operations list
	 * @throws AerospikeException	if command fails
	 */
	public final Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
		OperateArgs args = new OperateArgs(cluster, policy, writePolicyDefault, operatePolicyReadDefault, key, operations);
		OperateCommand command = new OperateCommand(cluster, key, args);
		command.execute();
		return command.getRecord();
	}

	/**
	 * Asynchronously perform multiple read/write operations on a single key in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * An example would be to add an integer value to an existing record and then
	 * read the result, all in one database call.
	 * <p>
	 * The server executes operations in the same order as the operations array.
	 * Both scalar bin operations (Operation) and CDT bin operations (ListOperation,
	 * MapOperation) can be performed in same call.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
		OperateArgs args = new OperateArgs(cluster, policy, writePolicyDefault, operatePolicyReadDefault, key, operations);
		AsyncOperate command = new AsyncOperate(listener, key, args);
		eventLoop.execute(cluster, command);
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
	 * @throws AerospikeException	if scan fails
	 */
	public final void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = scanPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionScan) {
			PartitionTracker tracker = new PartitionTracker(policy, nodes);
			ScanExecutor.scanPartitions(cluster, policy, namespace, setName, binNames, callback, tracker);
		}
		else {
			ScanExecutor.scanNodes(cluster, policy, namespace, setName, binNames, callback, nodes);
		}
	}

	/**
	 * Asynchronously read all records in specified namespace and set.  If the policy's
	 * <code>concurrentNodes</code> is specified, each server node will be read in
	 * parallel.  Otherwise, server nodes are read in series.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace, String setName, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = scanPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionScan) {
			PartitionTracker tracker = new PartitionTracker(policy, nodes);
			new AsyncScanPartitionExecutor(eventLoop, cluster, policy, listener, namespace, setName, binNames, tracker);
		}
		else {
			new AsyncScanExecutor(eventLoop, cluster, policy, listener, namespace, setName, binNames, nodes);
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
	 * @throws AerospikeException	if scan fails
	 */
	public final void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = scanPolicyDefault;
		}

		if (node.hasPartitionScan()) {
			PartitionTracker tracker = new PartitionTracker(policy, node);
			ScanExecutor.scanPartitions(cluster, policy, namespace, setName, binNames, callback, tracker);
		}
		else {
			ScanExecutor.scanNodes(cluster, policy, namespace, setName, binNames, callback, new Node[] {node});
		}
	}

	/**
	 * Read records in specified namespace, set and partition filter.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 *
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param partitionFilter		filter on a subset of data partitions
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified
	 * @throws AerospikeException	if scan fails
	 */
	public final void scanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = scanPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionScan) {
			PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
			ScanExecutor.scanPartitions(cluster, policy, namespace, setName, binNames, callback, tracker);
		}
		else {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "scanPartitions() not supported");
		}
	}

	/**
	 * Asynchronously read records in specified namespace, set and partition filter.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param partitionFilter		filter on a subset of data partitions
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void scanPartitions(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = scanPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionScan) {
			PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
			new AsyncScanPartitionExecutor(eventLoop, cluster, policy, listener, namespace, setName, binNames, tracker);
		}
		else {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "scanPartitions() not supported");
		}
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
	 * @param functionArgs			arguments passed in to user defined function
	 * @return						return value of user defined function
	 * @throws AerospikeException	if transaction fails
	 */
	public final Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... functionArgs)
		throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		ExecuteCommand command = new ExecuteCommand(cluster, policy, key, packageName, functionName, functionArgs);
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

	/**
	 * Asynchronously execute user defined function on server.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void execute(
		EventLoop eventLoop,
		ExecuteListener listener,
		WritePolicy policy,
		Key key,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncExecute command = new AsyncExecute(cluster, listener, policy, key, packageName, functionName, functionArgs);
		eventLoop.execute(cluster, command);
	}

	//----------------------------------------------------------
	// Query/Execute
	//----------------------------------------------------------

	/**
	 * Apply user defined function on records that match the statement filter.
	 * Records are not returned to the client.
	 * This asynchronous server call will return before the command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * ExecuteTask instance.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param statement				record filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
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

		Node[] nodes = cluster.validateNodes();
		Executor executor = new Executor(cluster, nodes.length);

		for (Node node : nodes) {
			ServerCommand command = new ServerCommand(cluster, node, policy, statement);
			executor.addCommand(command);
		}
		executor.execute(nodes.length);
		return new ExecuteTask(cluster, policy, statement);
	}

	/**
	 * Apply operations on records that match the statement filter.
	 * Records are not returned to the client.
	 * This asynchronous server call will return before the command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * ExecuteTask instance.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param statement				record filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
	 * @param operations			list of operations to be performed on selected records
	 * @throws AerospikeException	if command fails
	 */
	public final ExecuteTask execute(
		WritePolicy policy,
		Statement statement,
		Operation... operations
	) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		statement.setOperations(operations);
		statement.prepare(false);

		Node[] nodes = cluster.validateNodes();
		Executor executor = new Executor(cluster, nodes.length);

		for (Node node : nodes) {
			ServerCommand command = new ServerCommand(cluster, node, policy, statement);
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
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		// A scan will be performed if the secondary index filter is null.
		// Check if scan and partition scan is supported.
		if (statement.getFilter() == null && cluster.hasPartitionScan) {
			PartitionTracker tracker = new PartitionTracker(policy, nodes);
			QueryPartitionExecutor executor = new QueryPartitionExecutor(cluster, policy, statement, nodes.length, tracker);
			return executor.getRecordSet();
		}
		else {
			QueryRecordExecutor executor = new QueryRecordExecutor(cluster, policy, statement, nodes);
			executor.execute();
			return executor.getRecordSet();
		}
	}

	/**
	 * Asynchronously execute query on all server nodes.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement)
		throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		// A scan will be performed if the secondary index filter is null.
		// Check if scan and partition scan is supported.
		if (statement.getFilter() == null && cluster.hasPartitionScan) {
			PartitionTracker tracker = new PartitionTracker(policy, nodes);
			new AsyncQueryPartitionExecutor(eventLoop, listener, cluster, policy, statement, tracker);
		}
		else {
			new AsyncQueryExecutor(eventLoop, listener, cluster, policy, statement, nodes);
		}
	}

	/**
	 * Execute query on a single server node and return record iterator.  The query executor puts
	 * records on a queue in a separate thread.  The calling thread concurrently pops records off
	 * the queue through the record iterator.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
	 * @param node					server node to execute query
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		// A scan will be performed if the secondary index filter is null.
		// Check if scan and partition scan is supported.
		if (statement.getFilter() == null && node.hasPartitionScan()) {
			PartitionTracker tracker = new PartitionTracker(policy, node);
			QueryPartitionExecutor executor = new QueryPartitionExecutor(cluster, policy, statement, 1, tracker);
			return executor.getRecordSet();
		}
		else {
			QueryRecordExecutor executor = new QueryRecordExecutor(cluster, policy, statement, new Node[] {node});
			executor.execute();
			return executor.getRecordSet();
		}
	}

	/**
	 * Execute query for specified partitions and return record iterator.  The query executor puts
	 * records on a queue in separate threads.  The calling thread concurrently pops records off
	 * the queue through the record iterator.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
	 * @param partitionFilter		filter on a subset of data partitions
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet queryPartitions(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter)
		throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		// A scan will be performed if the secondary index filter is null.
		// Check if scan and partition scan is supported.
		if (statement.getFilter() == null && cluster.hasPartitionScan) {
			PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
			QueryPartitionExecutor executor = new QueryPartitionExecutor(cluster, policy, statement, nodes.length, tracker);
			return executor.getRecordSet();
		}
		else {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "queryPartitions() not supported");
		}
	}

	/**
	 * Asynchronously execute query for specified partitions.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
	 * @param partitionFilter		filter on a subset of data partitions
	 * @throws AerospikeException	if query fails
	 */
	public final void queryPartitions(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement, PartitionFilter partitionFilter)
		throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		// A scan will be performed if the secondary index filter is null.
		// Check if scan and partition scan is supported.
		if (statement.getFilter() == null && cluster.hasPartitionScan) {
			PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
			new AsyncQueryPartitionExecutor(eventLoop, listener, cluster, policy, statement, tracker);
		}
		else {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "queryPartitions() not supported");
		}
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
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
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
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
	 * @throws AerospikeException	if query fails
	 */
	public final ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();
		statement.prepare(true);
		QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, nodes);
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
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query filter. Statement instance is not suitable for
	 * 								reuse since it's modified in this method.
	 * @param node					server node to execute query
	 * @throws AerospikeException	if query fails
	 */
	public final ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}
		statement.prepare(true);
		QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, new Node[] {node});
		return executor.getResultSet();
	}

	//--------------------------------------------------------
	// Secondary Index functions
	//--------------------------------------------------------

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

		String command = buildCreateIndexInfoCommand(namespace, setName, indexName, binName, indexType, indexCollectionType);

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(policy, command);

		if (response.equalsIgnoreCase("OK")) {
			// Return task that could optionally be polled for completion.
			return new IndexTask(cluster, policy, namespace, indexName, true);
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "Create index failed: " + response);
	}

	/**
	 * Asynchronously create complex secondary index to be used on bins containing collections.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @throws AerospikeException	if index create fails
	 */
	public final void createIndex(
		EventLoop eventLoop,
		IndexListener listener,
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

		String command = buildCreateIndexInfoCommand(namespace, setName, indexName, binName, indexType, indexCollectionType);
		sendIndexInfoCommand(eventLoop, listener, policy, namespace, indexName, command, true);
	}

	/**
	 * Delete secondary index.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @throws AerospikeException	if index drop fails
	 */
	public final IndexTask dropIndex(
		Policy policy,
		String namespace,
		String setName,
		String indexName
	) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}

		String command = buildDropIndexInfoCommand(namespace, setName, indexName);

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(policy, command);

		if (response.equalsIgnoreCase("OK")) {
			return new IndexTask(cluster, policy, namespace, indexName, false);
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "Drop index failed: " + response);
	}

	/**
	 * Asynchronously delete secondary index.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @throws AerospikeException	if index drop fails
	 */
	public final void dropIndex(
		EventLoop eventLoop,
		IndexListener listener,
		Policy policy,
		String namespace,
		String setName,
		String indexName
	) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}

		String command = buildDropIndexInfoCommand(namespace, setName, indexName);
		sendIndexInfoCommand(eventLoop, listener, policy, namespace, indexName, command, false);
	}

	//-----------------------------------------------------------------
	// Async Info functions (sync info functions located in Info class)
	//-----------------------------------------------------------------

	/**
	 * Asynchronously make info commands.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The info protocol is a name/value pair based system, where an individual
	 * database server node is queried to determine its configuration and status.
	 * The list of supported info commands can be found at:
	 * <a href="https://www.aerospike.com/docs/reference/info/index.html">https://www.aerospike.com/docs/reference/info/index.html</a>
	 *
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				info configuration parameters, pass in null for defaults
	 * @param node					server node to execute command, pass in null for random node
	 * @param commands				list of info commands
	 * @throws AerospikeException	if info commands fail
	 */
	public final void info(
		EventLoop eventLoop,
		InfoListener listener,
		InfoPolicy policy,
		Node node,
		String... commands
	) throws AerospikeException {
		if (policy == null) {
			policy = infoPolicyDefault;
		}

		if (node == null) {
			node = cluster.getRandomNode();
		}

		AsyncInfoCommand command = new AsyncInfoCommand(listener, policy, node, commands);
		eventLoop.execute(cluster, command);
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
	 * Change user's password.
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

		byte[] userBytes = Buffer.stringToUtf8(user);
		byte[] passwordBytes = Buffer.stringToUtf8(password);

		String hash = AdminCommand.hashPassword(password);
		byte[] hashBytes = Buffer.stringToUtf8(hash);

		AdminCommand command = new AdminCommand();

		if (Arrays.equals(userBytes, cluster.getUser())) {
			// Change own password.
			command.changePassword(cluster, policy, userBytes, hash);
		}
		else {
			// Change other user's password by user admin.
			command.setPassword(cluster, policy, userBytes, hash);
		}
		cluster.changePassword(userBytes, passwordBytes, hashBytes);
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
	 * Create user defined role with optional privileges and whitelist.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			optional list of privileges assigned to role.
	 * @param whitelist				optional list of allowable IP addresses assigned to role.
	 * 								IP addresses can contain wildcards (ie. 10.1.2.0/24).
	 * @throws AerospikeException	if command fails
	 */
	public final void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.createRole(cluster, policy, roleName, privileges, whitelist);
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
	 * Set IP address whitelist for a role.  If whitelist is null or empty, remove existing whitelist from role.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param whitelist				list of allowable IP addresses or null.
	 * 								IP addresses can contain wildcards (ie. 10.1.2.0/24).
	 * @throws AerospikeException	if command fails
	 */
	public final void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.setWhitelist(cluster, policy, roleName, whitelist);
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

	private static String buildCreateIndexInfoCommand(
		String namespace,
		String setName,
		String indexName,
		String binName,
		IndexType indexType,
		IndexCollectionType indexCollectionType
	) {
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
		return sb.toString();
	}

	private static String buildDropIndexInfoCommand(String namespace, String setName, String indexName) {
		StringBuilder sb = new StringBuilder(500);
		sb.append("sindex-delete:ns=");
		sb.append(namespace);

		if (setName != null && setName.length() > 0) {
			sb.append(";set=");
			sb.append(setName);
		}
		sb.append(";indexname=");
		sb.append(indexName);
		return sb.toString();
	}

	private String sendInfoCommand(Policy policy, String command) {
		Node node = cluster.getRandomNode();
		Connection conn = node.getConnection(policy.socketTimeout);
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

	private void sendIndexInfoCommand(
		EventLoop eventLoop,
		IndexListener listener,
		Policy policy,
		String namespace,
		String indexName,
		String command,
		boolean isCreate
	) {
		info(eventLoop, new InfoListener() {
			@Override
			public void onSuccess(Map<String,String> map) {
				String response = map.values().iterator().next();

				if (response.equalsIgnoreCase("OK")) {
					// Return task that could optionally be polled for completion.
					listener.onSuccess(new AsyncIndexTask(AerospikeClient.this, namespace, indexName, isCreate));
				}
				else {
					int code = parseIndexErrorCode(response);
					String type = isCreate ? "Create" : "Drop";
					listener.onFailure(new AerospikeException(code, type + " index failed: " + response));
				}
			}

			@Override
			public void onFailure(AerospikeException ae) {
				listener.onFailure(ae);
			}
		}, new InfoPolicy(policy), cluster.getRandomNode(), command);
	}

	private static int parseIndexErrorCode(String response) {
		int code = 0;

		try {
			String[] list = response.split(":");

			if (list.length >= 2 && list[0].equals("FAIL")) {
				code = Integer.parseInt(list[1]);
			}
		}
		catch (Exception ex) {
		}

		if (code == 0) {
			code = ResultCode.SERVER_ERROR;
		}
		return code;
	}
}
