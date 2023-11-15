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
package com.aerospike.client.proxy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.BatchUDF;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.NettyEventLoop;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.OperateArgs;
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
import com.aerospike.client.metrics.MetricsPolicy;
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
import com.aerospike.client.proxy.BatchProxy.BatchListListenerSync;
import com.aerospike.client.proxy.auth.AuthTokenManager;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcChannelProvider;
import com.aerospike.client.proxy.grpc.GrpcClientPolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.Util;

import io.netty.channel.Channel;

/**
 * Aerospike proxy client based implementation of {@link IAerospikeClient}. The proxy client
 * communicates with a proxy server via GRPC and HTTP/2. The proxy server relays the database
 * commands to the Aerospike server. The proxy client does not have knowledge of Aerospike
 * server nodes. Only the proxy server can communicate directly with Aerospike server nodes.
 *
 * GRPC is an async framework, so an Aerospike sync command schedules the corresponding
 * async command and then waits for the async command to complete before returning the data
 * to the user.
 *
 * The async methods` eventLoop argument is ignored in the proxy client. Instead, the
 * commands are pipelined into blocks which are then executed via one of multiple GRPC channels.
 * Since the eventLoop thread is not chosen, results can be returned from different threads.
 * If data is shared between multiple async command listeners, that data must be accessed in
 * a thread-safe manner.
 */
public class AerospikeClientProxy implements IAerospikeClient, Closeable {
	//-------------------------------------------------------
	// Static variables.
	//-------------------------------------------------------

	/**
	 * Proxy client version
	 */
	public static String Version = getVersion();

	/**
	 * Lower limit of proxy server connection.
	 */
	private static final int MIN_CONNECTIONS = 1;

	// Thread factory used in synchronous batch, scan and query commands.
	public final ThreadFactory threadFactory;

	/**
	 * Upper limit of proxy server connection.
	 */
	private static final int MAX_CONNECTIONS = 8;

	private static final String NotSupported = "Method not supported in proxy client: ";

	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------

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
	 * Default user defined function policy used in batch UDF execute commands.
	 */
	public final BatchUDFPolicy batchUDFPolicyDefault;

	/**
	 * Default info policy that is used when info command policy is null.
	 */
	public final InfoPolicy infoPolicyDefault;

	private final WritePolicy operatePolicyReadDefault;
	private final AuthTokenManager authTokenManager;
	private final GrpcCallExecutor executor;

	//-------------------------------------------------------
	// Constructors
	//-------------------------------------------------------

	/**
	 * Initialize proxy client with suitable hosts to seed the cluster map.
	 * The client policy is used to set defaults and size internal data structures.
	 * <p>
	 * In most cases, only one host is necessary to seed the cluster. The remaining hosts
	 * are added as future seeds in case of a complete network failure.
	 *
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hosts					array of potential hosts to seed the cluster
	 * @throws AerospikeException	if all host connections fail
	 */
	public AerospikeClientProxy(ClientPolicy policy, Host... hosts) {
		if (policy == null) {
			policy = new ClientPolicy();
			policy.minConnsPerNode = 1;
			policy.maxConnsPerNode = 8;
			policy.asyncMaxConnsPerNode = 8;
			policy.timeout = 5000;
		}

		this.threadFactory = Thread.ofVirtual().name("Aerospike-", 0L).factory();
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

		GrpcChannelProvider channelProvider = new GrpcChannelProvider();

		if (policy.user != null || policy.password != null) {
			authTokenManager = new AuthTokenManager(policy, channelProvider);
		}
		else {
			authTokenManager = null;
		}

		try {
			// The gRPC client policy transformed from the client policy.
			GrpcClientPolicy grpcClientPolicy = toGrpcClientPolicy(policy);
			executor = new GrpcCallExecutor(grpcClientPolicy, authTokenManager, hosts);
			channelProvider.setCallExecutor(executor);

			// Warmup after the call executor in the channel provider has
			// been set. The channel provider is used to fetch auth tokens
			// required for the warm up calls.
			executor.warmupChannels();
		}
		catch (Throwable e) {
			if(authTokenManager != null) {
				authTokenManager.close();
			}
			throw e;
		}
	}

	/**
	 * Return client version string.
	 */
	private static String getVersion() {
		final Properties properties = new Properties();
		String version = null;

		try {
			properties.load(AerospikeClientProxy.class.getClassLoader().getResourceAsStream("project.properties"));
			version = properties.getProperty("version");
		}
		catch (Exception e) {
			Log.warn("Failed to retrieve client version: " + Util.getErrorMessage(e));
		}
		return version;
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
	// Client Management
	//-------------------------------------------------------

	/**
	 * Close GRPC executor and associated resources. The client instance should not
	 * be used after this call.
	 */
	@Override
	public void close() {
		try {
			executor.close();
		}
		catch (Throwable e) {
			Log.warn("Failed to close grpcCallExecutor: " + Util.getErrorMessage(e));
		}

		try {
			if (authTokenManager != null) {
				authTokenManager.close();
			}
		}
		catch (Throwable e) {
			Log.warn("Failed to close authTokenManager: " + Util.getErrorMessage(e));
		}
	}

	/**
	 * This method will always return true in the proxy client.
	 */
	@Override
	public boolean isConnected() {
		return executor != null;
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public Node[] getNodes() {
		throw new AerospikeException(NotSupported + "getNodes");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public List<String> getNodeNames() {
		throw new AerospikeException(NotSupported + "getNodeNames");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public Node getNode(String nodeName) {
		throw new AerospikeException(NotSupported + "getNode");
	}

	/**
	 * Not supported in proxy client.
	 */
	public final void enableMetrics(MetricsPolicy policy) {
		throw new AerospikeException(NotSupported + "enableMetrics");
	}

	/**
	 * Not supported in proxy client.
	 */
	public final void disableMetrics() {
		throw new AerospikeException(NotSupported + "disableMetrics");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public ClusterStats getClusterStats() {
		throw new AerospikeException(NotSupported + "getClusterStats");
	}

	/**
	 * Not supported in proxy client.
	 */
	public final void getClusterStats(ClusterStatsListener listener) {
		throw new AerospikeException(NotSupported + "getClusterStats");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public Cluster getCluster() {
		throw new AerospikeException(NotSupported + "getCluster");
	}

	//-------------------------------------------------------
	// Write Record Operations
	//-------------------------------------------------------

	/**
	 * Write record bin(s).
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if write fails
	 */
	@Override
	public void put(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		put(null, listener, policy, key, bins);
		getFuture(future);
	}

	/**
	 * Asynchronously write record bin(s).
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.WRITE);
		command.execute();
	}

	//-------------------------------------------------------
	// String Operations
	//-------------------------------------------------------

	/**
	 * Append bin string values to existing record bin values.
	 * This call only works for string values.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if append fails
	 */
	@Override
	public void append(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		append(null, listener, policy, key, bins);
		getFuture(future);
	}

	/**
	 * Asynchronously append bin string values to existing record bin values.
	 * This call only works for string values.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.APPEND);
		command.execute();
	}

	/**
	 * Prepend bin string values to existing record bin values.
	 * This call works only for string values.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if prepend fails
	 */
	@Override
	public void prepend(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		prepend(null, listener, policy, key, bins);
		getFuture(future);
	}

	/**
	 * Asynchronously prepend bin string values to existing record bin values.
	 * This call only works for string values.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.PREPEND);
		command.execute();
	}

	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------

	/**
	 * Add integer/double bin values to existing record bin values.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if add fails
	 */
	@Override
	public void add(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		add(null, listener, policy, key, bins);
		getFuture(future);
	}

	/**
	 * Asynchronously add integer/double bin values to existing record bin values.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.ADD);
		command.execute();
	}

	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------

	/**
	 * Delete record for specified key.
	 *
	 * @param policy				delete configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						whether record existed on server before deletion
	 * @throws AerospikeException	if delete fails
	 */
	@Override
	public boolean delete(WritePolicy policy, Key key) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		DeleteListener listener = prepareDeleteListener(future);
		delete(null, listener, policy, key);
		return getFuture(future);
	}

	/**
	 * Asynchronously delete record for specified key.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		DeleteCommandProxy command = new DeleteCommandProxy(executor, listener, policy, key);
		command.execute();
	}

	/**
	 * Delete records for specified keys. If a key is not found, the corresponding result
	 * {@link BatchRecord#resultCode} will be {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 *
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param deletePolicy	delete configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException.BatchRecordArray	which contains results for keys that did complete
	 */
	@Override
	public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys) {
		CompletableFuture<BatchResults> future = new CompletableFuture<>();
		BatchRecordArrayListener listener = prepareBatchRecordArrayListener(future);
		delete(null, listener, batchPolicy, deletePolicy, keys);
		return getFuture(future);
	}

	/**
	 * Asynchronously delete records for specified keys.
	 * <p>
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param deletePolicy	delete configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void delete(
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy batchPolicy,
		BatchDeletePolicy deletePolicy,
		Key[] keys
	) {
		if (keys.length == 0) {
			listener.onSuccess(new BatchRecord[0], true);
			return;
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (deletePolicy == null) {
			deletePolicy = batchDeletePolicyDefault;
		}

		BatchAttr attr = new BatchAttr();
		attr.setDelete(deletePolicy);

		CommandProxy command = new BatchProxy.OperateRecordArrayCommand(executor,
			batchPolicy, keys, null, listener, attr);

		command.execute();
	}

	/**
	 * Asynchronously delete records for specified keys.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param deletePolicy	delete configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void delete(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy batchPolicy,
		BatchDeletePolicy deletePolicy,
		Key[] keys
	) {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (deletePolicy == null) {
			deletePolicy = batchDeletePolicyDefault;
		}

		BatchAttr attr = new BatchAttr();
		attr.setDelete(deletePolicy);

		CommandProxy command = new BatchProxy.OperateRecordSequenceCommand(executor,
			batchPolicy, keys, null, listener, attr);

		command.execute();
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) {
		throw new AerospikeException(NotSupported + "truncate");
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
	@Override
	public void touch(WritePolicy policy, Key key) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		touch(null, listener, policy, key);
		getFuture(future);
	}

	/**
	 * Asynchronously reset record's time to expiration using the policy's expiration.
	 * Fail if the record does not exist.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		TouchCommandProxy command = new TouchCommandProxy(executor, listener, policy, key);
		command.execute();
	}

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------

	/**
	 * Determine if a record key exists.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						whether record exists or not
	 * @throws AerospikeException	if command fails
	 */
	@Override
	public boolean exists(Policy policy, Key key) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		ExistsListener listener = prepareExistsListener(future);
		exists(null, listener, policy, key);
		return getFuture(future);
	}

	/**
	 * Asynchronously determine if a record key exists.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ExistsCommandProxy command = new ExistsCommandProxy(executor, listener, policy, key);
		command.execute();
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
	@Override
	public boolean[] exists(BatchPolicy policy, Key[] keys) {
		CompletableFuture<boolean[]> future = new CompletableFuture<>();
		ExistsArrayListener listener = prepareExistsArrayListener(future);
		exists(null, listener, policy, keys);
		return getFuture(future);
	}

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * <p>
	 * The returned boolean array is in positional order with the original key array order.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys) {
		if (keys.length == 0) {
			listener.onSuccess(keys, new boolean[0]);
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.ExistsArrayCommand(executor, policy, listener, keys);
		command.execute();
	}

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * <p>
	 * Each key's result is returned in separate onExists() calls.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys) {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.ExistsSequenceCommand(executor, policy, listener, keys);
		command.execute();
	}

	//-------------------------------------------------------
	// Read Record Operations
	//-------------------------------------------------------

	/**
	 * Read entire record for specified key.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	@Override
	public Record get(Policy policy, Key key) {
		return get(policy, key, (String[])null);
	}

	/**
	 * Asynchronously read entire record for specified key.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) {
		get(eventLoop, listener, policy, key, (String[])null);
	}

	/**
	 * Read record header and bins for specified key.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	@Override
	public Record get(Policy policy, Key key, String... binNames) {
		CompletableFuture<Record> future = new CompletableFuture<>();
		RecordListener listener = prepareRecordListener(future);
		get(null, listener, policy, key, binNames);
		return getFuture(future);
	}

	/**
	 * Asynchronously read record header and bins for specified key.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames) {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ReadCommandProxy command = new ReadCommandProxy(executor, listener, policy, key, binNames);
		command.execute();
	}

	/**
	 * Read record generation and expiration only for specified key. Bins are not read.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	@Override
	public Record getHeader(Policy policy, Key key) {
		CompletableFuture<Record> future = new CompletableFuture<>();
		RecordListener listener = prepareRecordListener(future);
		getHeader(null, listener, policy, key);
		return getFuture(future);
	}

	/**
	 * Asynchronously read record generation and expiration only for specified key.  Bins are not read.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ReadHeaderCommandProxy command = new ReadHeaderCommandProxy(executor, listener, policy, key);
		command.execute();
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
	@Override
	public boolean get(BatchPolicy policy, List<BatchRead> records) {
		if (records.size() == 0) {
			return true;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CompletableFuture<Boolean> future = new CompletableFuture<>();
		BatchListListenerSync listener = prepareBatchListListenerSync(future);

		CommandProxy command = new BatchProxy.ReadListCommandSync(executor, policy, listener, records);
		command.execute();

		return getFuture(future);
	}

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and the bins to retrieve.
	 *						The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records) {
		if (records.size() == 0) {
			listener.onSuccess(records);
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}
		CommandProxy command = new BatchProxy.ReadListCommand(executor, policy, listener, records);
		command.execute();
	}

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * Each record result is returned in separate onRecord() calls.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and the bins to retrieve.
	 *						The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) {
		if (records.size() == 0) {
			listener.onSuccess();
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.ReadSequenceCommand(executor, policy, listener, records);
		command.execute();
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
	@Override
	public Record[] get(BatchPolicy policy, Key[] keys) {
		CompletableFuture<Record[]> future = new CompletableFuture<>();
		RecordArrayListener listener = prepareRecordArrayListener(future);
		get(null, listener, policy, keys);
		return getFuture(future);
	}

	/**
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.GetArrayCommand(executor, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
		command.execute();
	}

	/**
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.GetSequenceCommand(executor, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
		command.execute();
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
	@Override
	public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) {
		CompletableFuture<Record[]> future = new CompletableFuture<>();
		RecordArrayListener listener = prepareRecordArrayListener(future);
		get(null, listener, policy, keys, binNames);
		return getFuture(future);
	}

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param binNames		array of bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames) {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.GetArrayCommand(executor, policy, listener, keys, binNames, null, Command.INFO1_READ, false);
		command.execute();
	}

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param binNames		array of bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames) {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.GetSequenceCommand(executor, policy, listener, keys, binNames, null, Command.INFO1_READ, false);
		command.execute();
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
	@Override
	public Record[] get(BatchPolicy policy, Key[] keys, Operation... ops) {
		CompletableFuture<Record[]> future = new CompletableFuture<>();
		RecordArrayListener listener = prepareRecordArrayListener(future);
		get(null, listener, policy, keys, ops);
		return getFuture(future);
	}

	/**
	 * Asynchronously read multiple records for specified keys using read operations in one batch call.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			array of read operations on record
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, Operation... ops) {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.GetArrayCommand(executor, policy, listener, keys, null, ops, Command.INFO1_READ, true);
		command.execute();
	}

	/**
	 * Asynchronously read multiple records for specified keys using read operations in one batch call.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			array of read operations on record
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, Operation... ops) {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.GetSequenceCommand(executor, policy, listener, keys, null, ops, Command.INFO1_READ, true);
		command.execute();
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
	@Override
	public Record[] getHeader(BatchPolicy policy, Key[] keys) {
		CompletableFuture<Record[]> future = new CompletableFuture<>();
		RecordArrayListener listener = prepareRecordArrayListener(future);
		getHeader(null, listener, policy, keys);
		return getFuture(future);
	}

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) {
		if (keys.length == 0) {
			listener.onSuccess(keys, new Record[0]);
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.GetArrayCommand(executor, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA, false);
		command.execute();
	}

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (policy == null) {
			policy = batchPolicyDefault;
		}

		CommandProxy command = new BatchProxy.GetSequenceCommand(executor, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA, false);
		command.execute();
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
	@Override
	public Record operate(WritePolicy policy, Key key, Operation... operations) {
		CompletableFuture<Record> future = new CompletableFuture<>();
		RecordListener listener = prepareRecordListener(future);
		operate(null, listener, policy, key, operations);
		return getFuture(future);
	}

	/**
	 * Asynchronously perform multiple read/write operations on a single key in one batch call.
	 * <p>
	 * An example would be to add an integer value to an existing record and then
	 * read the result, all in one database call.
	 * <p>
	 * The server executes operations in the same order as the operations array.
	 * Both scalar bin operations (Operation) and CDT bin operations (ListOperation,
	 * MapOperation) can be performed in same call.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations) {
		OperateArgs args = new OperateArgs(policy, writePolicyDefault, operatePolicyReadDefault, operations);
		OperateCommandProxy command = new OperateCommandProxy(executor, listener, args.writePolicy, key, args);
		command.execute();
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
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param records	list of unique record identifiers and read/write operations
	 * @return			true if all batch sub-commands succeeded
	 * @throws AerospikeException	if command fails
	 */
	@Override
	public boolean operate(BatchPolicy policy, List<BatchRecord> records) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		BatchOperateListListener listener = prepareBatchOperateListListener(future);
		operate(null, listener, policy, records);
		return getFuture(future);
	}

	/**
	 * Asynchronously read/write multiple records for specified batch keys in one batch call.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * <p>
	 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
	 * {@link BatchUDF}.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and read/write operations
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void operate(
		EventLoop eventLoop,
		BatchOperateListListener listener,
		BatchPolicy policy,
		List<BatchRecord> records
	) {
		if (records.size() == 0) {
			listener.onSuccess(records, true);
			return;
		}

		if (policy == null) {
			policy = batchParentPolicyWriteDefault;
		}

		CommandProxy command = new BatchProxy.OperateListCommand(executor, policy, listener, records);
		command.execute();
	}

	/**
	 * Asynchronously read/write multiple records for specified batch keys in one batch call.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * Each record result is returned in separate onRecord() calls.
	 * <p>
	 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
	 * {@link BatchUDF}.
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and read/write operations
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void operate(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy policy,
		List<BatchRecord> records
	) {
		if (records.size() == 0) {
			listener.onSuccess();
			return;
		}

		if (policy == null) {
			policy = batchParentPolicyWriteDefault;
		}

		CommandProxy command = new BatchProxy.OperateSequenceCommand(executor, policy, listener, records);
		command.execute();
	}

	/**
	 * Perform read/write operations on multiple keys. If a key is not found, the corresponding result
	 * {@link BatchRecord#resultCode} will be {@link ResultCode#KEY_NOT_FOUND_ERROR}.
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
	@Override
	public BatchResults operate(
		BatchPolicy batchPolicy,
		BatchWritePolicy writePolicy,
		Key[] keys,
		Operation... ops
	) {
		CompletableFuture<BatchResults> future = new CompletableFuture<>();
		BatchRecordArrayListener listener = prepareBatchRecordArrayListener(future);
		operate(null, listener, batchPolicy, writePolicy, keys, ops);
		return getFuture(future);
	}

	/**
	 * Asynchronously perform read/write operations on multiple keys.
	 * <p>
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 *
	 * @param eventLoop		ignored, pass in null
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
	@Override
	public void operate(
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy batchPolicy,
		BatchWritePolicy writePolicy,
		Key[] keys,
		Operation... ops
	) {
		if (keys.length == 0) {
			listener.onSuccess(new BatchRecord[0], true);
			return;
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (writePolicy == null) {
			writePolicy = batchWritePolicyDefault;
		}

		BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);

		CommandProxy command = new BatchProxy.OperateRecordArrayCommand(executor,
			batchPolicy, keys, ops, listener, attr);

		command.execute();
	}

	/**
	 * Asynchronously perform read/write operations on multiple keys.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 *
	 * @param eventLoop		ignored, pass in null
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
	@Override
	public void operate(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy batchPolicy,
		BatchWritePolicy writePolicy,
		Key[] keys,
		Operation... ops
	) {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
		}

		if (batchPolicy == null) {
			batchPolicy = batchParentPolicyWriteDefault;
		}

		if (writePolicy == null) {
			writePolicy = batchWritePolicyDefault;
		}

		BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);

		CommandProxy command = new BatchProxy.OperateRecordSequenceCommand(executor,
			batchPolicy, keys, ops, listener, attr);

		command.execute();
	}

	//-------------------------------------------------------
	// Scan Operations
	//-------------------------------------------------------

	/**
	 * Read all records in specified namespace and set.
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
	@Override
	public void scanAll(
		ScanPolicy policy,
		String namespace,
		String setName,
		ScanCallback callback,
		String... binNames
	) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		RecordSequenceListener listener = new RecordSequenceListenerToCallback(callback, future);
		scanPartitions(null, listener, policy, null, namespace, setName, binNames);
		getFuture(future);
	}

	/**
	 * Asynchronously read all records in specified namespace and set.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void scanAll(
		EventLoop eventLoop,
		RecordSequenceListener listener,
		ScanPolicy policy,
		String namespace,
		String setName,
		String... binNames
	) {
		scanPartitions(eventLoop, listener, policy, null, namespace, setName, binNames);
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void scanNode(
		ScanPolicy policy,
		String nodeName,
		String namespace,
		String setName,
		ScanCallback callback,
		String... binNames
	) {
		throw new AerospikeException(NotSupported + "scanNode");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void scanNode(
		ScanPolicy policy,
		Node node,
		String namespace,
		String setName,
		ScanCallback callback,
		String... binNames
	) {
		throw new AerospikeException(NotSupported + "scanNode");
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
	@Override
	public void scanPartitions(
		ScanPolicy policy,
		PartitionFilter partitionFilter,
		String namespace,
		String setName,
		ScanCallback callback,
		String... binNames
	) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		RecordSequenceListener listener = new RecordSequenceListenerToCallback(callback, future);
		scanPartitions(null, listener, policy, partitionFilter, namespace, setName, binNames);
		getFuture(future);
	}

	/**
	 * Asynchronously read records in specified namespace, set and partition filter.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param partitionFilter		filter on a subset of data partitions
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void scanPartitions(
		EventLoop eventLoop,
		RecordSequenceListener listener,
		ScanPolicy policy,
		PartitionFilter partitionFilter,
		String namespace,
		String setName,
		String... binNames
	) {
		if (policy == null) {
			policy = scanPolicyDefault;
		}

		PartitionTracker tracker = null;

		if (partitionFilter != null) {
			tracker = new PartitionTracker(policy, 1, partitionFilter);
		}

		ScanCommandProxy command = new ScanCommandProxy(executor, policy, listener, namespace,
			setName, binNames, partitionFilter, tracker);
		command.execute();
	}

	//---------------------------------------------------------------
	// User defined functions
	//---------------------------------------------------------------

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) {
		throw new AerospikeException(NotSupported + "register");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public RegisterTask register(
		Policy policy,
		ClassLoader resourceLoader,
		String resourcePath,
		String serverPath,
		Language language
	) {
		throw new AerospikeException(NotSupported + "register");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) {
		throw new AerospikeException(NotSupported + "registerUdfString");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void removeUdf(InfoPolicy policy, String serverPath) {
		throw new AerospikeException(NotSupported + "removeUdf");
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
	@Override
	public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... functionArgs) {
		CompletableFuture<Object> future = new CompletableFuture<>();
		ExecuteListener listener = prepareExecuteListener(future);
		execute(null, listener, policy, key, packageName, functionName, functionArgs);
		return getFuture(future);
	}

	/**
	 * Asynchronously execute user defined function on server.
	 * <p>
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * {@code udf file = <server udf dir>/<package name>.lua}
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void execute(
		EventLoop eventLoop,
		ExecuteListener listener,
		WritePolicy policy,
		Key key,
		String packageName,
		String functionName,
		Value... functionArgs
	) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		ExecuteCommandProxy command = new ExecuteCommandProxy(executor, listener, policy, key,
			packageName, functionName, functionArgs);
		command.execute();
	}

	/**
	 * Execute user defined function on server for each key and return results.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * {@code udf file = <server udf dir>/<package name>.lua}
	 *
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param udfPolicy		udf configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param packageName	server package name where user defined function resides
	 * @param functionName	user defined function
	 * @param functionArgs	arguments passed in to user defined function
	 * @throws AerospikeException.BatchRecordArray	which contains results for keys that did complete
	 */
	@Override
	public BatchResults execute(
		BatchPolicy batchPolicy,
		BatchUDFPolicy udfPolicy,
		Key[] keys,
		String packageName,
		String functionName,
		Value... functionArgs
	) {
		CompletableFuture<BatchResults> future = new CompletableFuture<>();
		BatchRecordArrayListener listener = prepareBatchRecordArrayListener(future);
		execute(null, listener, batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
		return getFuture(future);
	}

	/**
	 * Asynchronously execute user defined function on server for each key and return results.
	 * <p>
	 * The package name is used to locate the udf file location:
	 * <p>
	 * {@code udf file = <server udf dir>/<package name>.lua}
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param udfPolicy		udf configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param packageName	server package name where user defined function resides
	 * @param functionName	user defined function
	 * @param functionArgs	arguments passed in to user defined function
	 * @throws AerospikeException	if command fails
	 */
	@Override
	public void execute(
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy batchPolicy,
		BatchUDFPolicy udfPolicy,
		Key[] keys,
		String packageName,
		String functionName,
		Value... functionArgs
	) {
		if (keys.length == 0) {
			listener.onSuccess(new BatchRecord[0], true);
			return;
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

		CommandProxy command = new BatchProxy.UDFArrayCommand(executor, batchPolicy,
			listener, keys, packageName, functionName, argBytes, attr);

		command.execute();
	}

	/**
	 * Asynchronously execute user defined function on server for each key and return results.
	 * Each record result is returned in separate onRecord() calls.
	 * <p>
	 * The package name is used to locate the udf file location:
	 * <p>
	 * {@code udf file = <server udf dir>/<package name>.lua}
	 *
	 * @param eventLoop		ignored, pass in null
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param udfPolicy		udf configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param packageName	server package name where user defined function resides
	 * @param functionName	user defined function
	 * @param functionArgs	arguments passed in to user defined function
	 * @throws AerospikeException	if command fails
	 */
	@Override
	public void execute(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy batchPolicy,
		BatchUDFPolicy udfPolicy,
		Key[] keys,
		String packageName,
		String functionName,
		Value... functionArgs
	) {
		if (keys.length == 0) {
			listener.onSuccess();
			return;
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

		CommandProxy command = new BatchProxy.UDFSequenceCommand(executor, batchPolicy,
			listener, keys, packageName, functionName, argBytes, attr);

		command.execute();
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
	@Override
	public ExecuteTask execute(
		WritePolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) {
		statement.setAggregateFunction(packageName, functionName, functionArgs);
		return executeBackgroundTask(policy, statement);
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
	@Override
	public ExecuteTask execute(WritePolicy policy, Statement statement, Operation... operations) {
		if (operations.length > 0) {
			statement.setOperations(operations);
		}
		return executeBackgroundTask(policy, statement);
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
	@Override
	public RecordSet query(QueryPolicy policy, Statement statement) {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		// @Ashish taskId will be zero by default here.
		RecordSequenceRecordSet recordSet = new RecordSequenceRecordSet(statement.getTaskId(), policy.recordQueueSize);
		query(null, recordSet, policy, statement);
		return recordSet;
	}

	/**
	 * Asynchronously execute query on all server nodes.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @throws AerospikeException	if event loop registration fails
	 */
	@Override
	public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement) {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		long taskId = statement.prepareTaskId();
		QueryCommandProxy command = new QueryCommandProxy(executor, listener,
			policy, statement, taskId, null, null);
		command.execute();
	}

	/**
	 * Execute query on all server nodes and return records via the listener. This method will
	 * block until the query is complete. Listener callbacks are made within the scope of this call.
	 * <p>
	 * If {@link com.aerospike.client.policy.QueryPolicy#maxConcurrentNodes} is not 1, the supplied
	 * listener must handle shared data in a thread-safe manner, because the listener will be called
	 * by multiple query threads (one thread per node) in parallel.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param listener				where to send results
	 * @throws AerospikeException	if query fails
	 */
	@Override
	public void query(QueryPolicy policy, Statement statement, QueryListener listener) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		RecordSequenceToQueryListener adaptor = new RecordSequenceToQueryListener(listener, future);
		query(null, adaptor, policy, statement);
		getFuture(future);
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
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		data partition filter. Set to
	 * 								{@link com.aerospike.client.query.PartitionFilter#all()} for all partitions.
	 * @param listener				where to send results
	 * @throws AerospikeException	if query fails
	 */
	@Override
	public void query(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter, QueryListener listener) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		RecordSequenceToQueryListener adaptor = new RecordSequenceToQueryListener(listener, future);
		queryPartitions(null, adaptor, policy, statement, partitionFilter);
		getFuture(future);
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) {
		throw new AerospikeException(NotSupported + "queryNode");
	}

	/**
	 * Execute query for specified partitions and return record iterator.  The query executor puts
	 * records on a queue in separate threads.  The calling thread concurrently pops records off
	 * the queue through the record iterator.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		filter on a subset of data partitions
	 * @throws AerospikeException	if query fails
	 */
	@Override
	public RecordSet queryPartitions(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter) {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		// @Ashish taskId will be zero by default here.
		RecordSequenceRecordSet recordSet = new RecordSequenceRecordSet(statement.getTaskId(), policy.recordQueueSize);
		queryPartitions(null, recordSet, policy, statement, partitionFilter);
		return recordSet;
	}

	/**
	 * Asynchronously execute query for specified partitions.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 *
	 * @param eventLoop				ignored, pass in null
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		filter on a subset of data partitions
	 * @throws AerospikeException	if query fails
	 */
	@Override
	public void queryPartitions(
		EventLoop eventLoop,
		RecordSequenceListener listener,
		QueryPolicy policy,
		Statement statement,
		PartitionFilter partitionFilter
	) {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		long taskId = statement.prepareTaskId();
		PartitionTracker tracker = new PartitionTracker(policy, statement, 1, partitionFilter);
		QueryCommandProxy command = new QueryCommandProxy(executor, listener, policy,
			statement, taskId, partitionFilter, tracker);
		command.execute();
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
	@Override
	public ResultSet queryAggregate(
		QueryPolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) {
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
	@Override
	public ResultSet queryAggregate(QueryPolicy policy, Statement statement) {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		long taskId = statement.prepareTaskId();
		QueryAggregateCommandProxy commandProxy = new QueryAggregateCommandProxy(
			executor, threadFactory, policy, statement, taskId);
		commandProxy.execute();
		return commandProxy.getResultSet();
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) {
		throw new AerospikeException(NotSupported + "queryAggregateNode");
	}

	//--------------------------------------------------------
	// Secondary Index functions
	//--------------------------------------------------------

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public IndexTask createIndex(
		Policy policy,
		String namespace,
		String setName,
		String indexName,
		String binName,
		IndexType indexType
	) {
		throw new AerospikeException(NotSupported + "createIndex");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public IndexTask createIndex(
		Policy policy,
		String namespace,
		String setName,
		String indexName,
		String binName,
		IndexType indexType,
		IndexCollectionType indexCollectionType,
		CTX... ctx
	) {
		throw new AerospikeException(NotSupported + "createIndex");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void createIndex(
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
	) {
		throw new AerospikeException(NotSupported + "createIndex");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName) {
		throw new AerospikeException(NotSupported + "dropIndex");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void dropIndex(
		EventLoop eventLoop,
		IndexListener listener,
		Policy policy,
		String namespace,
		String setName,
		String indexName
	) {
		throw new AerospikeException(NotSupported + "dropIndex");
	}

	//-----------------------------------------------------------------
	// Async Info functions (sync info functions located in Info class)
	//-----------------------------------------------------------------

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void info(EventLoop eventLoop, InfoListener listener, InfoPolicy policy, Node node, String... commands) {
		throw new AerospikeException(NotSupported + "info");
	}

	//-----------------------------------------------------------------
	// XDR - Cross datacenter replication
	//-----------------------------------------------------------------

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void setXDRFilter(InfoPolicy policy, String datacenter, String namespace, Expression filter) {
		throw new AerospikeException(NotSupported + "setXDRFilter");
	}

	//-------------------------------------------------------
	// User administration
	//-------------------------------------------------------

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void createUser(AdminPolicy policy, String user, String password, List<String> roles) {
		throw new AerospikeException(NotSupported + "createUser");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void dropUser(AdminPolicy policy, String user) {
		throw new AerospikeException(NotSupported + "dropUser");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void changePassword(AdminPolicy policy, String user, String password) {
		throw new AerospikeException(NotSupported + "changePassword");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void grantRoles(AdminPolicy policy, String user, List<String> roles) {
		throw new AerospikeException(NotSupported + "grantRoles");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void revokeRoles(AdminPolicy policy, String user, List<String> roles) {
		throw new AerospikeException(NotSupported + "revokeRoles");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) {
		throw new AerospikeException(NotSupported + "createRole");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist) {
		throw new AerospikeException(NotSupported + "createRole");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void createRole(
		AdminPolicy policy,
		String roleName,
		List<Privilege> privileges,
		List<String> whitelist,
		int readQuota,
		int writeQuota
	) {
		throw new AerospikeException(NotSupported + "createRole");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void dropRole(AdminPolicy policy, String roleName) {
		throw new AerospikeException(NotSupported + "dropRole");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) {
		throw new AerospikeException(NotSupported + "grantPrivileges");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) {
		throw new AerospikeException(NotSupported + "revokePrivileges");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist) {
		throw new AerospikeException(NotSupported + "setWhitelist");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public void setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota) {
		throw new AerospikeException(NotSupported + "setQuotas");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public User queryUser(AdminPolicy policy, String user) {
		throw new AerospikeException(NotSupported + "queryUser");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public List<User> queryUsers(AdminPolicy policy) {
		throw new AerospikeException(NotSupported + "queryUsers");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public Role queryRole(AdminPolicy policy, String roleName) {
		throw new AerospikeException(NotSupported + "queryRole");
	}

	/**
	 * Not supported in proxy client.
	 */
	@Override
	public List<Role> queryRoles(AdminPolicy policy) {
		throw new AerospikeException(NotSupported + "queryRoles");
	}

	//-------------------------------------------------------
	// Internal Methods
	//-------------------------------------------------------

	private static GrpcClientPolicy toGrpcClientPolicy(ClientPolicy policy) {
		List<io.netty.channel.EventLoop> eventLoops = null;
		Class<? extends Channel> channelType = null;

		if (policy.eventLoops != null) {
			if (! (policy.eventLoops instanceof NettyEventLoops)) {
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Netty event loops are required in proxy client");
			}

			NettyEventLoops nettyLoops = (NettyEventLoops)policy.eventLoops;
			NettyEventLoop[] array = nettyLoops.getArray();
			eventLoops = new ArrayList<>(array.length);

			for (NettyEventLoop loop : array) {
				eventLoops.add(loop.get());
			}

			channelType = nettyLoops.getSocketChannelClass();
		}

		int maxConnections = Math.min(MAX_CONNECTIONS, Math.max(MIN_CONNECTIONS,
			Math.max(policy.asyncMaxConnsPerNode, policy.maxConnsPerNode)));

		return GrpcClientPolicy.newBuilder(eventLoops, channelType)
			.maxChannels(maxConnections)
			.connectTimeoutMillis(policy.timeout)
			.closeTimeout(policy.closeTimeout)
			.tlsPolicy(policy.tlsPolicy)
			.build();
	}

	private ExecuteTask executeBackgroundTask(WritePolicy policy, Statement statement) {
		if (policy == null) {
			policy = writePolicyDefault;
		}

		CompletableFuture<Void> future = new CompletableFuture<>();
		long taskId = statement.prepareTaskId();

		BackgroundExecuteCommandProxy command = new BackgroundExecuteCommandProxy(executor, policy,
			statement, taskId, future);
		command.execute();

		// Check whether the background task started.
		getFuture(future);

		return new ExecuteTaskProxy(executor, taskId, statement.isScan());
	}

	private static WriteListener prepareWriteListener(final CompletableFuture<Void> future) {
		return new WriteListener() {
			@Override
			public void onSuccess(Key key) {
				future.complete(null);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static DeleteListener prepareDeleteListener(final CompletableFuture<Boolean> future) {
		return new DeleteListener() {
			@Override
			public void onSuccess(Key key, boolean existed) {
				future.complete(existed);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static RecordListener prepareRecordListener(final CompletableFuture<Record> future) {
		return new RecordListener() {
			@Override
			public void onSuccess(Key key, Record record) {
				future.complete(record);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static ExistsListener prepareExistsListener(final CompletableFuture<Boolean> future) {
		return new ExistsListener() {
			@Override
			public void onSuccess(Key key, boolean exists) {
				future.complete(exists);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static ExecuteListener prepareExecuteListener(final CompletableFuture<Object> future) {
		return new ExecuteListener() {
			@Override
			public void onSuccess(Key key, Object obj) {
				future.complete(obj);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static ExistsArrayListener prepareExistsArrayListener(final CompletableFuture<boolean[]> future) {
		return new ExistsArrayListener() {
			@Override
			public void onSuccess(Key[] keys, boolean[] exists) {
				future.complete(exists);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static RecordArrayListener prepareRecordArrayListener(final CompletableFuture<Record[]> future) {
		return new RecordArrayListener() {
			@Override
			public void onSuccess(Key[] keys, Record[] records) {
				future.complete(records);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static BatchListListenerSync prepareBatchListListenerSync(final CompletableFuture<Boolean> future) {
		return new BatchListListenerSync() {
			@Override
			public void onSuccess(List<BatchRead> records, boolean status) {
				future.complete(status);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static BatchOperateListListener prepareBatchOperateListListener(final CompletableFuture<Boolean> future) {
		return new BatchOperateListListener() {
			@Override
			public void onSuccess(List<BatchRecord> records, boolean status) {
				future.complete(status);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		};
	}

	private static BatchRecordArrayListener prepareBatchRecordArrayListener(final CompletableFuture<BatchResults> future) {
		return new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				future.complete(new BatchResults(records, status));
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				future.completeExceptionally(new AerospikeException.BatchRecordArray(records, ae));
			}
		};
	}

	static <T> T getFuture(final CompletableFuture<T> future) {
		try {
			return future.get();
		}
		catch (ExecutionException e) {
			if (e.getCause() instanceof AerospikeException) {
				throw (AerospikeException)e.getCause();
			}
			throw new AerospikeException(e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AerospikeException(e);
		}
	}
}
