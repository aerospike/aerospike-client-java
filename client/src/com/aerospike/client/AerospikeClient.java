/*
 * Copyright 2012-2023 Aerospike, Inc.
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
import com.aerospike.client.async.AsyncScanPartitionExecutor;
import com.aerospike.client.async.AsyncTouch;
import com.aerospike.client.async.AsyncWrite;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Batch;
import com.aerospike.client.command.Batch.BatchCommand;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.BatchExecutor;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.command.BatchStatus;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.DeleteCommand;
import com.aerospike.client.command.ExecuteCommand;
import com.aerospike.client.command.Executor;
import com.aerospike.client.command.ExistsCommand;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.command.OperateCommand;
import com.aerospike.client.command.ReadCommand;
import com.aerospike.client.command.ReadHeaderCommand;
import com.aerospike.client.command.RegisterCommand;
import com.aerospike.client.command.ScanExecutor;
import com.aerospike.client.command.TouchCommand;
import com.aerospike.client.command.WriteCommand;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ClusterStatsListener;
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
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
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
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.QueryListenerExecutor;
import com.aerospike.client.query.QueryPartitionExecutor;
import com.aerospike.client.query.QueryRecordExecutor;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.ServerCommand;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.Crypto;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;
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
	 * Default parent policy used in batch read commands. Parent policy fields
	 * include socketTimeout, totalTimeout, maxRetries, etc...
	 */
	public final BatchPolicy batchPolicyDefault;

	/**
	 * Default parent policy used in batch write commands. Parent policy fields
	 * include socketTimeout, totalTimeout, maxRetries, etc...
	 */
	public final BatchPolicy batchParentPolicyWriteDefault;

	/**
	 * Default write policy used in batch operate commands.
	 * Write policy fields include generation, expiration, durableDelete, etc...
	 */
	public final BatchWritePolicy batchWritePolicyDefault;

	/**
	 * Default delete policy used in batch delete commands.
	 */
	public final BatchDeletePolicy batchDeletePolicyDefault;

	/**
	 * Default user defined function policy used in batch UDF excecute commands.
	 */
	public final BatchUDFPolicy batchUDFPolicyDefault;

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
	public AerospikeClient(String hostname, int port)
		throws AerospikeException {
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
	public AerospikeClient(ClientPolicy policy, String hostname, int port)
		throws AerospikeException {
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
	public AerospikeClient(ClientPolicy policy, Host... hosts)
		throws AerospikeException {

		// Disable log subscribe requirement to avoid a breaking change in a minor release.
		// TODO: Reintroduce requirement in the next major client release.
		/*
		if (! Log.isSet()) {
			throw new AerospikeException("Log.setCallback() or Log.setCallbackStandard() must be called." + System.lineSeparator() +
				"See https://developer.aerospike.com/client/java/usage/logging for details.");
		}
		*/

		if (policy == null) {
			policy = new ClientPolicy();
		}
		this.readPolicyDefault = policy.readPolicyDefault;
		this.writePolicyDefault = policy.writePolicyDefault;
		this.scanPolicyDefault = policy.scanPolicyDefault;
		this.queryPolicyDefault = policy.queryPolicyDefault;
		this.batchPolicyDefault = policy.batchPolicyDefault;
		this.batchParentPolicyWriteDefault = policy.batchParentPolicyWriteDefault;
		this.batchWritePolicyDefault = policy.batchWritePolicyDefault;
		this.batchDeletePolicyDefault = policy.batchDeletePolicyDefault;
		this.batchUDFPolicyDefault = policy.batchUDFPolicyDefault;
		this.infoPolicyDefault = policy.infoPolicyDefault;
		this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);

		cluster = new Cluster(policy, hosts);
	}

	//-------------------------------------------------------
	// Protected Initialization
	//-------------------------------------------------------

	/**
	 * ClientPolicy only constructor. Do not use directly.
	 */
	protected AerospikeClient(ClientPolicy policy) {
		if (policy != null) {
			this.readPolicyDefault = policy.readPolicyDefault;
			this.writePolicyDefault = policy.writePolicyDefault;
			this.scanPolicyDefault = policy.scanPolicyDefault;
			this.queryPolicyDefault = policy.queryPolicyDefault;
			this.batchPolicyDefault = policy.batchPolicyDefault;
			this.batchParentPolicyWriteDefault = policy.batchParentPolicyWriteDefault;
			this.batchWritePolicyDefault = policy.batchWritePolicyDefault;
			this.batchDeletePolicyDefault = policy.batchDeletePolicyDefault;
			this.batchUDFPolicyDefault = policy.batchUDFPolicyDefault;
			this.infoPolicyDefault = policy.infoPolicyDefault;
			this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);
		}
		else {
			this.readPolicyDefault = new Policy();
			this.writePolicyDefault = new WritePolicy();
			this.scanPolicyDefault = new ScanPolicy();
			this.queryPolicyDefault = new QueryPolicy();
			this.batchPolicyDefault = new BatchPolicy();
			this.batchParentPolicyWriteDefault = BatchPolicy.WriteDefault();
			this.batchWritePolicyDefault = new BatchWritePolicy();
			this.batchDeletePolicyDefault = new BatchDeletePolicy();
			this.batchUDFPolicyDefault = new BatchUDFPolicy();
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

	public final BatchPolicy getBatchParentPolicyWriteDefault() {
		return batchParentPolicyWriteDefault;
	}

	public final BatchWritePolicy getBatchWritePolicyDefault() {
		return batchWritePolicyDefault;
	}

	public final BatchDeletePolicy getBatchDeletePolicyDefault() {
		return batchDeletePolicyDefault;
	}

	public final BatchUDFPolicy getBatchUDFPolicyDefault() {
		return batchUDFPolicyDefault;
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
	public final Node getNode(String nodeName)
		throws AerospikeException.InvalidNode {
		return cluster.getNode(nodeName);
	}

	/**
	 * Return operating cluster statistics.
	 */
	public final ClusterStats getClusterStats() {
		return cluster.getStats();
	}

	/**
	 * Asynchronously return operating cluster statistics.
	 */
	public final void getClusterStats(ClusterStatsListener listener) {
		cluster.getStats(listener);
	}

	/**
	 * Return operating cluster.
	 */
	public final Cluster getCluster() {
		return cluster;
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
	public final void put(WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	public final void append(WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	public final void prepend(WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	 * Add integer/double bin values to existing record bin values.
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if add fails
	 */
	public final void add(WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.ADD);
		command.execute();
	}

	/**
	 * Asynchronously add integer/double bin values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	public final boolean delete(WritePolicy policy, Key key)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncDelete command = new AsyncDelete(cluster, listener, policy, key);
		eventLoop.execute(cluster, command);
	}

	/**
	 * Delete records for specified keys. If a key is not found, the corresponding result
	 * {@link BatchRecord#resultCode} will be {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param deletePolicy	delete configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException.BatchRecordArray	which contains results for keys that did complete
	 */
	public final BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			return new BatchResults(new BatchRecord[0], true);
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (deletePolicy == null) {
			deletePolicy = batchDeletePolicyDefault;
		}

		BatchAttr attr = new BatchAttr();
		attr.setDelete(deletePolicy);

		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], attr.hasWrite);
		}

		try {
			BatchStatus status = new BatchStatus(true);
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				commands[count++] = new Batch.OperateArrayCommand(cluster, batchNode, batchPolicy, keys, null, records, attr, status);
			}

			BatchExecutor.execute(cluster, batchPolicy, commands, status);
			return new BatchResults(records, status.getStatus());
		}
		catch (Throwable e) {
			// Batch terminated on fatal error.
			throw new AerospikeException.BatchRecordArray(records, e);
		}
	}

	/**
	 * Asynchronously delete records for specified keys.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param deletePolicy	delete configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void delete(
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy batchPolicy,
		BatchDeletePolicy deletePolicy,
		Key[] keys
	) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(new BatchRecord[0], true);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (deletePolicy == null) {
			deletePolicy = batchDeletePolicyDefault;
		}

		BatchAttr attr = new BatchAttr();
		attr.setDelete(deletePolicy);

		new AsyncBatch.OperateRecordArrayExecutor(eventLoop, cluster, batchPolicy, listener, keys, null, attr);
	}

	/**
	 * Asynchronously delete records for specified keys.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param deletePolicy	delete configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void delete(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy batchPolicy,
		BatchDeletePolicy deletePolicy,
		Key[] keys
	) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (deletePolicy == null) {
			deletePolicy = batchDeletePolicyDefault;
		}

		BatchAttr attr = new BatchAttr();
		attr.setDelete(deletePolicy);

		new AsyncBatch.OperateRecordSequenceExecutor(eventLoop, cluster, batchPolicy, listener, keys, null, attr);
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
	public final void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate)
		throws AerospikeException {
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
			sb.append("truncate-namespace:namespace=");
			sb.append(ns);
		}

		if (beforeLastUpdate != null) {
			sb.append(";lut=");
			// Convert to nanoseconds since unix epoch (1970-01-01)
			sb.append(beforeLastUpdate.getTimeInMillis() * 1000000L);
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
	public final void touch(WritePolicy policy, Key key)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	public final boolean exists(Policy policy, Key key)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = readPolicyDefault;
		}
		AsyncExists command = new AsyncExists(cluster, listener, policy, key);
		eventLoop.execute(cluster, command);
	}

	/**
	 * Check if multiple record keys exist in one batch call.
	 * The returned boolean array is in positional order with the original key array order.
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @return			array key/existence status pairs
	 * @throws AerospikeException.BatchExists	which contains results for keys that did complete
	 */
	public final boolean[] exists(BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			return new boolean[0];
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		boolean[] existsArray = new boolean[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				commands[count++] = new Batch.ExistsArrayCommand(cluster, batchNode, policy, keys, existsArray, status);
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return existsArray;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchExists(existsArray, e);
		}
	}

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned boolean array is in positional order with the original key array order.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new boolean[0]);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
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
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
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
	public final Record get(Policy policy, Key key)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	public final Record get(Policy policy, Key key, String... binNames)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	public final Record getHeader(Policy policy, Key key)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param records	list of unique record identifiers and the bins to retrieve.
	 *					The returned records are located in the same list.
	 * @return			true if all batch key requests succeeded
	 * @throws AerospikeException	if read fails
	 */
	public final boolean get(BatchPolicy policy, List<BatchRead> records)
		throws AerospikeException {
		if (records.size() == 0) {
			return true;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		BatchStatus status = new BatchStatus(true);
		List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, records, status);
		BatchCommand[] commands = new BatchCommand[batchNodes.size()];
		int count = 0;

		for (BatchNode batchNode : batchNodes) {
			commands[count++] = new Batch.ReadListCommand(cluster, batchNode, policy, records, status);
		}
		BatchExecutor.execute(cluster, policy, commands, status);
		return status.getStatus();
	}

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and the bins to retrieve.
	 *						The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records)
		throws AerospikeException {
		if (records.size() == 0) {
			listener.onSuccess(records);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
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
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and the bins to retrieve.
	 *						The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records)
		throws AerospikeException {
		if (records.size() == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
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
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @return			array of records
	 * @throws AerospikeException.BatchRecords	which contains results for keys that did complete
	 */
	public final Record[] get(BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			return new Record[0];
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		Record[] records = new Record[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				commands[count++] = new Batch.GetArrayCommand(cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_GET_ALL, false, status);
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return records;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecords(records, e);
		}
	}

	/**
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetArrayExecutor(eventLoop, cluster, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
	}

	/**
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetSequenceExecutor(eventLoop, cluster, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
	}

	/**
	 * Read multiple record headers and bins for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @param binNames	array of bins to retrieve
	 * @return			array of records
	 * @throws AerospikeException.BatchRecords	which contains results for keys that did complete
	 */
	public final Record[] get(BatchPolicy policy, Key[] keys, String... binNames)
		throws AerospikeException {
		if (keys.length == 0) {
			return new Record[0];
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		Record[] records = new Record[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				commands[count++] = new Batch.GetArrayCommand(cluster, batchNode, policy, keys, binNames, null, records, Command.INFO1_READ, false, status);
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return records;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecords(records, e);
		}
	}

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param binNames		array of bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetArrayExecutor(eventLoop, cluster, policy, listener, keys, binNames, null, Command.INFO1_READ, false);
	}

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param binNames		array of bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetSequenceExecutor(eventLoop, cluster, policy, listener, keys, binNames, null, Command.INFO1_READ, false);
	}

	/**
	 * Read multiple records for specified keys using read operations in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @param ops		array of read operations on record
	 * @return			array of records
	 * @throws AerospikeException.BatchRecords	which contains results for keys that did complete
	 */
	public final Record[] get(BatchPolicy policy, Key[] keys, Operation... ops)
		throws AerospikeException {
		if (keys.length == 0) {
			return new Record[0];
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		Record[] records = new Record[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				commands[count++] = new Batch.GetArrayCommand(cluster, batchNode, policy, keys, null, ops, records, Command.INFO1_READ, true, status);
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return records;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecords(records, e);
		}
	}

	/**
	 * Asynchronously read multiple records for specified keys using read operations in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			array of read operations on record
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, Operation... ops)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetArrayExecutor(eventLoop, cluster, policy, listener, keys, null, ops, Command.INFO1_READ, true);
	}

	/**
	 * Asynchronously read multiple records for specified keys using read operations in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			array of read operations on record
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, Operation... ops)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetSequenceExecutor(eventLoop, cluster, policy, listener, keys, null, ops, Command.INFO1_READ, true);
	}

	/**
	 * Read multiple record header data for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @return			array of records
	 * @throws AerospikeException.BatchRecords	which contains results for keys that did complete
	 */
	public final Record[] getHeader(BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			return new Record[0];
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		Record[] records = new Record[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				commands[count++] = new Batch.GetArrayCommand(cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_NOBINDATA, false, status);
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return records;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecords(records, e);
		}
	}

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetArrayExecutor(eventLoop, cluster, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA, false);
	}

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		new AsyncBatch.GetSequenceExecutor(eventLoop, cluster, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA, false);
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
	public final Record operate(WritePolicy policy, Key key, Operation... operations)
		throws AerospikeException {
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		OperateArgs args = new OperateArgs(cluster, policy, writePolicyDefault, operatePolicyReadDefault, key, operations);
		AsyncOperate command = new AsyncOperate(listener, key, args);
		eventLoop.execute(cluster, command);
	}

	//-------------------------------------------------------
	// Batch Read/Write Operations
	//-------------------------------------------------------

	/**
	 * Read/Write multiple records for specified batch keys in one batch call.
	 * This method allows different namespaces/bins for each key in the batch.
	 * The returned records are located in the same list.
	 * <p>
	 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
	 * {@link BatchUDF}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param records	list of unique record identifiers and read/write operations
	 * @return			true if all batch sub-commands succeeded
	 * @throws AerospikeException	if command fails
	 */
	public final boolean operate(BatchPolicy policy, List<BatchRecord> records)
		throws AerospikeException {
		if (records.size() == 0) {
			return true;
		}

		if (policy == null) {
			policy = batchParentPolicyWriteDefault;
		}

		BatchStatus status = new BatchStatus(true);
		List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, records, status);
		BatchCommand[] commands = new BatchCommand[batchNodes.size()];
		int count = 0;

		for (BatchNode batchNode : batchNodes) {
			commands[count++] = new Batch.OperateListCommand(cluster, batchNode, policy, records, status);
		}
		BatchExecutor.execute(cluster, policy, commands, status);
		return status.getStatus();
	}

	/**
	 * Asynchronously read/write multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * <p>
	 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
	 * {@link BatchUDF}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and read/write operations
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void operate(
		EventLoop eventLoop,
		BatchOperateListListener listener,
		BatchPolicy policy,
		List<BatchRecord> records
	) throws AerospikeException {
		if (records.size() == 0) {
			listener.onSuccess(records, false);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchParentPolicyWriteDefault;
		}
		new AsyncBatch.OperateListExecutor(eventLoop, cluster, policy, listener, records);
	}

	/**
	 * Asynchronously read/write multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * Each record result is returned in separate onRecord() calls.
	 * <p>
	 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
	 * {@link BatchUDF}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and read/write operations
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void operate(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy policy,
		List<BatchRecord> records
	) throws AerospikeException {
		if (records.size() == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = batchParentPolicyWriteDefault;
		}
		new AsyncBatch.OperateSequenceExecutor(eventLoop, cluster, policy, listener, records);
	}

	/**
	 * Perform read/write operations on multiple keys. If a key is not found, the corresponding result
	 * {@link BatchRecord#resultCode} will be {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param writePolicy	write configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops
	 * read/write operations to perform. {@link Operation#get()} is not allowed because it returns a
	 * variable number of bins and makes it difficult (sometimes impossible) to lineup operations
	 * with results. Instead, use {@link Operation#get(String)} for each bin name.
	 * @throws AerospikeException.BatchRecordArray	which contains results for keys that did complete
	 */
	public final BatchResults operate(
		BatchPolicy batchPolicy,
		BatchWritePolicy writePolicy,
		Key[] keys,
		Operation... ops
	) throws AerospikeException {
		if (keys.length == 0) {
			return new BatchResults(new BatchRecord[0], true);
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (writePolicy == null) {
			writePolicy = batchWritePolicyDefault;
		}

		BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], attr.hasWrite);
		}

		try {
			BatchStatus status = new BatchStatus(true);
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				commands[count++] = new Batch.OperateArrayCommand(cluster, batchNode, batchPolicy, keys, ops, records, attr, status);
			}

			BatchExecutor.execute(cluster, batchPolicy, commands, status);
			return new BatchResults(records, status.getStatus());
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecordArray(records, e);
		}
	}

	/**
	 * Asynchronously perform read/write operations on multiple keys.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param writePolicy	write configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops
	 * read/write operations to perform. {@link Operation#get()} is not allowed because it returns a
	 * variable number of bins and makes it difficult (sometimes impossible) to lineup operations
	 * with results. Instead, use {@link Operation#get(String)} for each bin name.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void operate(
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy batchPolicy,
		BatchWritePolicy writePolicy,
		Key[] keys,
		Operation... ops
	) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(new BatchRecord[0], true);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (writePolicy == null) {
			writePolicy = batchWritePolicyDefault;
		}

		BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
		new AsyncBatch.OperateRecordArrayExecutor(eventLoop, cluster, batchPolicy, listener, keys, ops, attr);
	}

	/**
	 * Asynchronously perform read/write operations on multiple keys.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param writePolicy	write configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops
	 * read/write operations to perform. {@link Operation#get()} is not allowed because it returns a
	 * variable number of bins and makes it difficult (sometimes impossible) to lineup operations
	 * with results. Instead, use {@link Operation#get(String)} for each bin name.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void operate(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy batchPolicy,
		BatchWritePolicy writePolicy,
		Key[] keys,
		Operation... ops
	) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (writePolicy == null) {
			writePolicy = batchWritePolicyDefault;
		}

		BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
		new AsyncBatch.OperateRecordSequenceExecutor(eventLoop, cluster, batchPolicy, listener, keys, ops, attr);
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
		PartitionTracker tracker = new PartitionTracker(policy, nodes);
		ScanExecutor.scanPartitions(cluster, policy, namespace, setName, binNames, callback, tracker);
	}

	/**
	 * Asynchronously read all records in specified namespace and set.  If the policy's
	 * <code>concurrentNodes</code> is specified, each server node will be read in
	 * parallel.  Otherwise, server nodes are read in series.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace, String setName, String... binNames)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = scanPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();
		PartitionTracker tracker = new PartitionTracker(policy, nodes);
		new AsyncScanPartitionExecutor(eventLoop, cluster, policy, listener, namespace, setName, binNames, tracker);
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

		PartitionTracker tracker = new PartitionTracker(policy, node);
		ScanExecutor.scanPartitions(cluster, policy, namespace, setName, binNames, callback, tracker);
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
		PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
		ScanExecutor.scanPartitions(cluster, policy, namespace, setName, binNames, callback, tracker);
	}

	/**
	 * Asynchronously read records in specified namespace, set and partition filter.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
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
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = scanPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();
		PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
		new AsyncScanPartitionExecutor(eventLoop, cluster, policy, listener, namespace, setName, binNames, tracker);
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
	 * @param resourcePath		  class path where Lua resource is located
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
	 *   "	return rec[name]\n" +
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
	public final void removeUdf(InfoPolicy policy, String serverPath)
		throws AerospikeException {
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
	 * {@code udf file = <server udf dir>/<package name>.lua}
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
	 * {@code udf file = <server udf dir>/<package name>.lua}
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
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
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = writePolicyDefault;
		}
		AsyncExecute command = new AsyncExecute(cluster, listener, policy, key, packageName, functionName, functionArgs);
		eventLoop.execute(cluster, command);
	}

	/**
	 * Execute user defined function on server for each key and return results.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * {@code udf file = <server udf dir>/<package name>.lua}
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param udfPolicy		udf configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param packageName	server package name where user defined function resides
	 * @param functionName	user defined function
	 * @param functionArgs	arguments passed in to user defined function
	 * @throws AerospikeException.BatchRecordArray	which contains results for keys that did complete
	 */
	public final BatchResults execute(
		BatchPolicy batchPolicy,
		BatchUDFPolicy udfPolicy,
		Key[] keys,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException {
		if (keys.length == 0) {
			return new BatchResults(new BatchRecord[0], true);
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (udfPolicy == null) {
			udfPolicy = batchUDFPolicyDefault;
		}

		byte[] argBytes = Packer.pack(functionArgs);

		BatchAttr attr = new BatchAttr();
		attr.setUDF(udfPolicy);

		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], attr.hasWrite);
		}

		try {
			BatchStatus status = new BatchStatus(true);
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				commands[count++] = new Batch.UDFCommand(cluster, batchNode, batchPolicy, keys, packageName, functionName, argBytes, records, attr, status);
			}

			BatchExecutor.execute(cluster, batchPolicy, commands, status);
			return new BatchResults(records, status.getStatus());
		}
		catch (Throwable e) {
			// Batch terminated on fatal error.
			throw new AerospikeException.BatchRecordArray(records, e);
		}
	}

	/**
	 * Asynchronously execute user defined function on server for each key and return results.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The package name is used to locate the udf file location:
	 * <p>
	 * {@code udf file = <server udf dir>/<package name>.lua}
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param udfPolicy		udf configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param packageName	server package name where user defined function resides
	 * @param functionName	user defined function
	 * @param functionArgs	arguments passed in to user defined function
	 * @throws AerospikeException	if command fails
	 */
	public final void execute(
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy batchPolicy,
		BatchUDFPolicy udfPolicy,
		Key[] keys,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess(new BatchRecord[0], true);
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (udfPolicy == null) {
			udfPolicy = batchUDFPolicyDefault;
		}

		byte[] argBytes = Packer.pack(functionArgs);

		BatchAttr attr = new BatchAttr();
		attr.setUDF(udfPolicy);

		new AsyncBatch.UDFArrayExecutor(eventLoop, cluster, batchPolicy, listener, keys, packageName, functionName, argBytes, attr);
	}

	/**
	 * Asynchronously execute user defined function on server for each key and return results.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * Each record result is returned in separate onRecord() calls.
	 * <p>
	 * The package name is used to locate the udf file location:
	 * <p>
	 * {@code udf file = <server udf dir>/<package name>.lua}
	 * <p>
	 * Requires server version 6.0+
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param udfPolicy		udf configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param packageName	server package name where user defined function resides
	 * @param functionName	user defined function
	 * @param functionArgs	arguments passed in to user defined function
	 * @throws AerospikeException	if command fails
	 */
	public final void execute(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy batchPolicy,
		BatchUDFPolicy udfPolicy,
		Key[] keys,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (udfPolicy == null) {
			udfPolicy = batchUDFPolicyDefault;
		}

		byte[] argBytes = Packer.pack(functionArgs);

		BatchAttr attr = new BatchAttr();
		attr.setUDF(udfPolicy);

		new AsyncBatch.UDFSequenceExecutor(eventLoop, cluster, batchPolicy, listener, keys, packageName, functionName, argBytes, attr);
	}

	//----------------------------------------------------------
	// Query/Execute
	//----------------------------------------------------------

	/**
	 * Apply user defined function on records that match the background query statement filter.
	 * Records are not returned to the client.
	 * This asynchronous server call will return before the command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * ExecuteTask instance.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param statement				background query definition
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

		long taskId = statement.prepareTaskId();
		Node[] nodes = cluster.validateNodes();
		Executor executor = new Executor(cluster, nodes.length);

		for (Node node : nodes) {
			ServerCommand command = new ServerCommand(cluster, node, policy, statement, taskId);
			executor.addCommand(command);
		}
		executor.execute(nodes.length);
		return new ExecuteTask(cluster, policy, statement, taskId);
	}

	/**
	 * Apply operations on records that match the background query statement filter.
	 * Records are not returned to the client.
	 * This asynchronous server call will return before the command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * ExecuteTask instance.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param statement				background query definition
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

		if (operations.length > 0) {
			statement.setOperations(operations);
		}

		long taskId = statement.prepareTaskId();
		Node[] nodes = cluster.validateNodes();
		Executor executor = new Executor(cluster, nodes.length);

		for (Node node : nodes) {
			ServerCommand command = new ServerCommand(cluster, node, policy, statement, taskId);
			executor.addCommand(command);
		}
		executor.execute(nodes.length);
		return new ExecuteTask(cluster, policy, statement, taskId);
	}

	//--------------------------------------------------------
	// Query functions
	//--------------------------------------------------------

	/**
	 * Execute query on all server nodes and return record iterator. The query executor puts
	 * records on a queue in separate threads. The calling thread concurrently pops records off
	 * the queue through the record iterator.
	 * <p>
	 * This method is not recommended for paginated queries when the user does not iterate through
	 * all records in the RecordSet. In this case, there is a lag between when the client marks the
	 * last record retrieved from the server and when the record is retrieved from the RecordSet.
	 * For this case, use {@link #query(QueryPolicy, Statement, QueryListener)} which uses a listener
	 * callback (without a buffer) instead of a RecordSet.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet query(QueryPolicy policy, Statement statement)
		throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionQuery || statement.getFilter() == null) {
			PartitionTracker tracker = new PartitionTracker(policy, statement, nodes);
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @throws AerospikeException	if event loop registration fails
	 */
	public final void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionQuery || statement.getFilter() == null) {
			PartitionTracker tracker = new PartitionTracker(policy, statement, nodes);
			new AsyncQueryPartitionExecutor(eventLoop, listener, cluster, policy, statement, tracker);
		}
		else {
			new AsyncQueryExecutor(eventLoop, listener, cluster, policy, statement, nodes);
		}
	}

	/**
	 * Execute query on all server nodes and return records via the listener. This method will
	 * block until the query is complete. Listener callbacks are made within the scope of this call.
	 * <p>
	 * If {@link com.aerospike.client.policy.QueryPolicy#maxConcurrentNodes} is not 1, the supplied
	 * listener must handle shared data in a thread-safe manner, because the listener will be called
	 * by multiple query threads (one thread per node) in parallel.
	 * <p>
	 * Requires server version 6.0+ if using a secondary index query.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param listener				where to send results
	 * @throws AerospikeException	if query fails
	 */
	public final void query(
		QueryPolicy policy,
		Statement statement,
		QueryListener listener
	) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionQuery || statement.getFilter() == null) {
			PartitionTracker tracker = new PartitionTracker(policy, statement, nodes);
			QueryListenerExecutor.execute(cluster, policy, statement, listener, tracker);
		}
		else {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Query by partition is not supported");
		}
	}

	/**
	 * Execute query for specified partitions and return records via the listener. This method will
	 * block until the query is complete. Listener callbacks are made within the scope of this call.
	 * <p>
	 * If {@link com.aerospike.client.policy.QueryPolicy#maxConcurrentNodes} is not 1, the supplied
	 * listener must handle shared data in a thread-safe manner, because the listener will be called
	 * by multiple query threads (one thread per node) in parallel.
	 * <p>
	 * The completion status of all partitions is stored in the partitionFilter when the query terminates.
	 * This partitionFilter can then be used to resume an incomplete query at a later time.
	 * This is the preferred method for query terminate/resume functionality.
	 * <p>
	 * Requires server version 6.0+ if using a secondary index query.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		data partition filter. Set to
	 * 								{@link com.aerospike.client.query.PartitionFilter#all()} for all partitions.
	 * @param listener				where to send results
	 * @throws AerospikeException	if query fails
	 */
	public final void query(
		QueryPolicy policy,
		Statement statement,
		PartitionFilter partitionFilter,
		QueryListener listener
	) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionQuery || statement.getFilter() == null) {
			PartitionTracker tracker = new PartitionTracker(policy, statement, nodes, partitionFilter);
			QueryListenerExecutor.execute(cluster, policy, statement, listener, tracker);
		}
		else {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Query by partition is not supported");
		}
	}

	/**
	 * Execute query on a single server node and return record iterator.  The query executor puts
	 * records on a queue in a separate thread.  The calling thread concurrently pops records off
	 * the queue through the record iterator.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param node					server node to execute query
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet queryNode(QueryPolicy policy, Statement statement, Node node)
		throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		if (cluster.hasPartitionQuery || statement.getFilter() == null) {
			PartitionTracker tracker = new PartitionTracker(policy, statement, node);
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
	 * <p>
	 * Requires server version 6.0+ if using a secondary index query.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		filter on a subset of data partitions
	 * @throws AerospikeException	if query fails
	 */
	public final RecordSet queryPartitions(
		QueryPolicy policy,
		Statement statement,
		PartitionFilter partitionFilter
	) throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionQuery || statement.getFilter() == null) {
			PartitionTracker tracker = new PartitionTracker(policy, statement, nodes, partitionFilter);
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
	 * <p>
	 * Requires server version 6.0+ if using a secondary index query.
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		filter on a subset of data partitions
	 * @throws AerospikeException	if query fails
	 */
	public final void queryPartitions(
		EventLoop eventLoop,
		RecordSequenceListener listener,
		QueryPolicy policy,
		Statement statement,
		PartitionFilter partitionFilter
	) throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();

		if (cluster.hasPartitionQuery || statement.getFilter() == null) {
			PartitionTracker tracker = new PartitionTracker(policy, statement, nodes, partitionFilter);
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
	 * {@code udf file = <udf dir>/<package name>.lua}
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
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
	 * @param statement				query definition
	 * @throws AerospikeException	if query fails
	 */
	public final ResultSet queryAggregate(QueryPolicy policy, Statement statement)
		throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		Node[] nodes = cluster.validateNodes();
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
	 * @param statement				query definition
	 * @param node					server node to execute query
	 * @throws AerospikeException	if query fails
	 */
	public final ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node)
		throws AerospikeException {
		if (policy == null) {
			policy = queryPolicyDefault;
		}
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
	 * @param ctx					optional context to index on elements within a CDT
	 * @throws AerospikeException	if index create fails
	 */
	public final IndexTask createIndex(
		Policy policy,
		String namespace,
		String setName,
		String indexName,
		String binName,
		IndexType indexType,
		IndexCollectionType indexCollectionType,
		CTX... ctx
	) throws AerospikeException {
		if (policy == null) {
			policy = writePolicyDefault;
		}

		String command = buildCreateIndexInfoCommand(namespace, setName, indexName, binName, indexType, indexCollectionType, ctx);

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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @param ctx					optional context to index on elements within a CDT
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
		IndexCollectionType indexCollectionType,
		CTX... ctx
	) throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = writePolicyDefault;
		}

		String command = buildCreateIndexInfoCommand(namespace, setName, indexName, binName, indexType, indexCollectionType, ctx);
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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
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
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

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
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
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
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = infoPolicyDefault;
		}

		if (node == null) {
			node = cluster.getRandomNode();
		}

		AsyncInfoCommand command = new AsyncInfoCommand(listener, policy, node, commands);
		eventLoop.execute(cluster, command);
	}

	//-----------------------------------------------------------------
	// XDR - Cross datacenter replication
	//-----------------------------------------------------------------

	/**
	 * Set XDR filter for given datacenter name and namespace. The expression filter indicates
	 * which records XDR should ship to the datacenter. If the expression filter is null, the
	 * XDR filter will be removed.
	 *
	 * @param policy				info configuration parameters, pass in null for defaults
	 * @param datacenter			XDR datacenter name
	 * @param namespace				namespace - equivalent to database name
	 * @param filter				expression filter
	 * @throws AerospikeException	if command fails
	 */
	public final void setXDRFilter(
		InfoPolicy policy,
		String datacenter,
		String namespace,
		Expression filter
	) throws AerospikeException {
		if (policy == null) {
			policy = infoPolicyDefault;
		}

		// Send XDR command to one node. That node will distribute the XDR command to other nodes.
		String filterString = (filter != null)? filter.getBase64() : "null";
		String command = "xdr-set-filter:dc=" + datacenter + ";namespace=" + namespace + ";exp=" + filterString;
		Node node = cluster.getRandomNode();
		String response = Info.request(policy, node, command);

		if (response.equalsIgnoreCase("ok")) {
			return;
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "xdr-set-filter failed: " + response);
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
	 * @param roles					variable arguments array of role names.  Predefined roles are listed in {@link com.aerospike.client.admin.Role}
	 * @throws AerospikeException	if command fails
	 */
	public final void createUser(AdminPolicy policy, String user, String password, List<String> roles)
		throws AerospikeException {
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
	public final void dropUser(AdminPolicy policy, String user)
		throws AerospikeException {
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
	public final void changePassword(AdminPolicy policy, String user, String password)
		throws AerospikeException {
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
	 * @param roles					role names.  Predefined roles are listed in {@link com.aerospike.client.admin.Role}
	 * @throws AerospikeException	if command fails
	 */
	public final void grantRoles(AdminPolicy policy, String user, List<String> roles)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.grantRoles(cluster, policy, user, roles);
	}

	/**
	 * Remove roles from user's list of roles.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param roles					role names.  Predefined roles are listed in {@link com.aerospike.client.admin.Role}
	 * @throws AerospikeException	if command fails
	 */
	public final void revokeRoles(AdminPolicy policy, String user, List<String> roles)
		throws AerospikeException {
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
	public final void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges)
		throws AerospikeException {
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
	public final void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.createRole(cluster, policy, roleName, privileges, whitelist, 0, 0);
	}

	/**
	 * Create user defined role with optional privileges, whitelist and read/write quotas.
	 * Quotas require server security configuration "enable-quotas" to be set to true.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			optional list of privileges assigned to role.
	 * @param whitelist				optional list of allowable IP addresses assigned to role.
	 * 								IP addresses can contain wildcards (ie. 10.1.2.0/24).
	 * @param readQuota				optional maximum reads per second limit, pass in zero for no limit.
	 * @param writeQuota			optional maximum writes per second limit, pass in zero for no limit.
	 * @throws AerospikeException	if command fails
	 */
	public final void createRole(
		AdminPolicy policy,
		String roleName,
		List<Privilege> privileges,
		List<String> whitelist,
		int readQuota,
		int writeQuota
	) throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.createRole(cluster, policy, roleName, privileges, whitelist, readQuota, writeQuota);
	}

	/**
	 * Drop user defined role.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @throws AerospikeException	if command fails
	 */
	public final void dropRole(AdminPolicy policy, String roleName)
		throws AerospikeException {
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
	public final void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)
		throws AerospikeException {
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
	public final void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)
		throws AerospikeException {
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
	public final void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.setWhitelist(cluster, policy, roleName, whitelist);
	}

	/**
	 * Set maximum reads/writes per second limits for a role.  If a quota is zero, the limit is removed.
	 * Quotas require server security configuration "enable-quotas" to be set to true.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param readQuota				maximum reads per second limit, pass in zero for no limit.
	 * @param writeQuota			maximum writes per second limit, pass in zero for no limit.
	 * @throws AerospikeException	if command fails
	 */
	public final void setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.setQuotas(cluster, policy, roleName, readQuota, writeQuota);
	}

	/**
	 * Retrieve roles for a given user.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name filter
	 * @throws AerospikeException	if command fails
	 */
	public final User queryUser(AdminPolicy policy, String user)
		throws AerospikeException {
		AdminCommand.UserCommand command = new AdminCommand.UserCommand(1);
		return command.queryUser(cluster, policy, user);
	}

	/**
	 * Retrieve all users and their roles.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @throws AerospikeException	if command fails
	 */
	public final List<User> queryUsers(AdminPolicy policy)
		throws AerospikeException {
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
	public final Role queryRole(AdminPolicy policy, String roleName)
		throws AerospikeException {
		AdminCommand.RoleCommand command = new AdminCommand.RoleCommand(1);
		return command.queryRole(cluster, policy, roleName);
	}

	/**
	 * Retrieve all roles.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @throws AerospikeException	if command fails
	 */
	public final List<Role> queryRoles(AdminPolicy policy)
		throws AerospikeException {
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
		IndexCollectionType indexCollectionType,
		CTX[] ctx
	) {
		StringBuilder sb = new StringBuilder(1024);
		sb.append("sindex-create:ns=");
		sb.append(namespace);

		if (setName != null && setName.length() > 0) {
			sb.append(";set=");
			sb.append(setName);
		}

		sb.append(";indexname=");
		sb.append(indexName);

		if (ctx != null && ctx.length > 0) {
			byte[] bytes = Pack.pack(ctx);
			String base64 = Crypto.encodeBase64(bytes);

			sb.append(";context=");
			sb.append(base64);
		}

		if (indexCollectionType != IndexCollectionType.DEFAULT) {
			sb.append(";indextype=");
			sb.append(indexCollectionType);
		}

		sb.append(";indexdata=");
		sb.append(binName);
		sb.append(",");
		sb.append(indexType);
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
		Connection conn = node.getConnection(policy.connectTimeout, policy.socketTimeout);
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
