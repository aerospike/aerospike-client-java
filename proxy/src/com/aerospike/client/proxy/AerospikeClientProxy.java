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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
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
import com.aerospike.client.cluster.ThreadDaemonFactory;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
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
 * Aerospike proxy client based implementation of {@link AerospikeClient}.
 * The proxy client communicates with a proxy server via GRPC and HTTP/2.
 * The proxy server relays the database commands to the Aerospike server.
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

	/**
	 * Is threadPool shared between other client instances or classes.  If threadPool is
	 * not shared (default), threadPool will be shutdown when the client instance is closed.
	 * <p>
	 * If threadPool is shared, threadPool will not be shutdown when the client instance is
	 * closed. This shared threadPool should be shutdown manually before the program
	 * terminates.  Shutdown is recommended, but not absolutely required if threadPool is
	 * constructed to use daemon threads.
	 * <p>
	 * Default: false
	 */
	private final boolean sharedThreadPool;

	/**
	 * Underlying thread pool used in synchronous batch, scan, and query commands. These commands
	 * are often sent to multiple server nodes in parallel threads.  A thread pool improves
	 * performance because threads do not have to be created/destroyed for each command.
	 * The default, null, indicates that the following daemon thread pool will be used:
	 * <pre>
	 * threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
	 *     public final Thread newThread(Runnable runnable) {
	 * 			Thread thread = new Thread(runnable);
	 * 			thread.setDaemon(true);
	 * 			return thread;
	 *        }
	 *    });
	 * </pre>
	 * Daemon threads automatically terminate when the program terminates.
	 * <p>
	 * Default: null (use Executors.newCachedThreadPool)
	 */
	private final ExecutorService threadPool;

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

	public AerospikeClientProxy(ClientPolicy policy, Host... hosts) {
		if (policy == null) {
			policy = new ClientPolicy();
			policy.minConnsPerNode = 1;
			policy.maxConnsPerNode = 8;
			policy.asyncMaxConnsPerNode = 8;
			policy.timeout = 5000;
		}

		if (policy.threadPool == null) {
			threadPool = Executors.newCachedThreadPool(new ThreadDaemonFactory());
		}
		else {
			threadPool = policy.threadPool;
		}
		sharedThreadPool = policy.sharedThreadPool;

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
		}
		catch (Throwable e) {
			if(authTokenManager != null) {
				authTokenManager.close();
			}
			throw e;
		}
	}

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

		if (! sharedThreadPool) {
			// Shutdown synchronous thread pool.
			threadPool.shutdown();
		}
	}

	@Override
	public boolean isConnected() {
		return executor != null;
	}

	@Override
	public Node[] getNodes() {
		throw new AerospikeException(NotSupported + "getNodes");
	}

	@Override
	public List<String> getNodeNames() {
		throw new AerospikeException(NotSupported + "getNodeNames");
	}

	@Override
	public Node getNode(String nodeName) {
		throw new AerospikeException(NotSupported + "getNode");
	}

	@Override
	public ClusterStats getClusterStats() {
		throw new AerospikeException(NotSupported + "getClusterStats");
	}

	@Override
	public Cluster getCluster() {
		throw new AerospikeException(NotSupported + "getCluster");
	}

	//-------------------------------------------------------
	// Write Record Operations
	//-------------------------------------------------------

	@Override
	public void put(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		put(null, listener, policy, key, bins);
		getFuture(future);
	}

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

	@Override
	public void append(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		append(null, listener, policy, key, bins);
		getFuture(future);
	}

	@Override
	public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.APPEND);
		command.execute();
	}

	@Override
	public void prepend(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		prepend(null, listener, policy, key, bins);
		getFuture(future);
	}

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

	@Override
	public void add(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		add(null, listener, policy, key, bins);
		getFuture(future);
	}

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

	@Override
	public boolean delete(WritePolicy policy, Key key) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		DeleteListener listener = prepareDeleteListener(future);
		delete(null, listener, policy, key);
		return getFuture(future);
	}

	@Override
	public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		DeleteCommandProxy command = new DeleteCommandProxy(executor, listener, policy, key);
		command.execute();
	}

	@Override
	public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys) {
		CompletableFuture<BatchResults> future = new CompletableFuture<>();
		BatchRecordArrayListener listener = prepareBatchRecordArrayListener(future);
		delete(null, listener, batchPolicy, deletePolicy, keys);
		return getFuture(future);
	}

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

	@Override
	public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) {
		throw new AerospikeException(NotSupported + "truncate");
	}

	//-------------------------------------------------------
	// Touch Operations
	//-------------------------------------------------------

	@Override
	public void touch(WritePolicy policy, Key key) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		WriteListener listener = prepareWriteListener(future);
		touch(null, listener, policy, key);
		getFuture(future);
	}

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

	@Override
	public boolean exists(Policy policy, Key key) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		ExistsListener listener = prepareExistsListener(future);
		exists(null, listener, policy, key);
		return getFuture(future);
	}

	@Override
	public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ExistsCommandProxy command = new ExistsCommandProxy(executor, listener, policy, key);
		command.execute();
	}

	@Override
	public boolean[] exists(BatchPolicy policy, Key[] keys) {
		CompletableFuture<boolean[]> future = new CompletableFuture<>();
		ExistsArrayListener listener = prepareExistsArrayListener(future);
		exists(null, listener, policy, keys);
		return getFuture(future);
	}

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

	@Override
	public Record get(Policy policy, Key key) {
		return get(policy, key, (String[])null);
	}

	@Override
	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) {
		get(eventLoop, listener, policy, key, (String[])null);
	}

	@Override
	public Record get(Policy policy, Key key, String... binNames) {
		CompletableFuture<Record> future = new CompletableFuture<>();
		RecordListener listener = prepareRecordListener(future);
		get(null, listener, policy, key, binNames);
		return getFuture(future);
	}

	@Override
	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames) {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ReadCommandProxy command = new ReadCommandProxy(executor, listener, policy, key, binNames);
		command.execute();
	}

	@Override
	public Record getHeader(Policy policy, Key key) {
		CompletableFuture<Record> future = new CompletableFuture<>();
		RecordListener listener = prepareRecordListener(future);
		getHeader(null, listener, policy, key);
		return getFuture(future);
	}

	@Override
	public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) {
		if (policy == null) {
			policy = readPolicyDefault;
		}
		ReadHeaderCommandProxy command = new ReadHeaderCommandProxy(executor, listener, policy, key);
		command.execute();
	}

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

	@Override
	public Record[] get(BatchPolicy policy, Key[] keys) {
		CompletableFuture<Record[]> future = new CompletableFuture<>();
		RecordArrayListener listener = prepareRecordArrayListener(future);
		get(null, listener, policy, keys);
		return getFuture(future);
	}

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

	@Override
	public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) {
		CompletableFuture<Record[]> future = new CompletableFuture<>();
		RecordArrayListener listener = prepareRecordArrayListener(future);
		get(null, listener, policy, keys, binNames);
		return getFuture(future);
	}

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

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	@Override
	public Record[] get(BatchPolicy policy, Key[] keys, Operation... ops) {
		CompletableFuture<Record[]> future = new CompletableFuture<>();
		RecordArrayListener listener = prepareRecordArrayListener(future);
		get(null, listener, policy, keys, ops);
		return getFuture(future);
	}

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

	@Override
	public Record[] getHeader(BatchPolicy policy, Key[] keys) {
		CompletableFuture<Record[]> future = new CompletableFuture<>();
		RecordArrayListener listener = prepareRecordArrayListener(future);
		getHeader(null, listener, policy, keys);
		return getFuture(future);
	}

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

	@Override
	public Record operate(WritePolicy policy, Key key, Operation... operations) {
		CompletableFuture<Record> future = new CompletableFuture<>();
		RecordListener listener = prepareRecordListener(future);
		operate(null, listener, policy, key, operations);
		return getFuture(future);
	}

	@Override
	public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations) {
		OperateArgs args = new OperateArgs(policy, writePolicyDefault, operatePolicyReadDefault, key, operations);
		OperateCommandProxy command = new OperateCommandProxy(executor, listener, args.writePolicy, key, args);
		command.execute();
	}

	//-------------------------------------------------------
	// Batch Read/Write Operations
	//-------------------------------------------------------

	@Override
	public boolean operate(BatchPolicy policy, List<BatchRecord> records) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		BatchOperateListListener listener = prepareBatchOperateListListener(future);
		operate(null, listener, policy, records);
		return getFuture(future);
	}

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

	@Override
	public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) {
		throw new AerospikeException(NotSupported + "register");
	}

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

	@Override
	public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) {
		throw new AerospikeException(NotSupported + "registerUdfString");
	}

	@Override
	public void removeUdf(InfoPolicy policy, String serverPath) {
		throw new AerospikeException(NotSupported + "removeUdf");
	}

	@Override
	public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) {
		CompletableFuture<Object> future = new CompletableFuture<>();
		ExecuteListener listener = prepareExecuteListener(future);
		execute(null, listener, policy, key, packageName, functionName, args);
		return getFuture(future);
	}

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

	private ExecuteTask executeBackgroundTask(WritePolicy policy, Statement statement) {
		if (policy == null) {
			policy = writePolicyDefault;
		}

		CompletableFuture<Void> future = new CompletableFuture<>();
		BackgroundExecuteCommandProxy command = new BackgroundExecuteCommandProxy(executor, policy,
			statement, future);
		command.execute();

		// Check whether the background task started.
		getFuture(future);

		// The background executor ensures the statement has a taskId either
		// from user input or separately prepared.
		return new ExecuteTaskProxy(executor, statement.getTaskId(), statement.isScan());
	}

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

	@Override
	public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement) {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		QueryCommandProxy command = new QueryCommandProxy(executor, listener, policy, statement, null, null);
		command.execute();
	}

	@Override
	public void query(QueryPolicy policy, Statement statement, QueryListener listener) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		RecordSequenceToQueryListener adaptor = new RecordSequenceToQueryListener(listener, future);
		query(null, adaptor, policy, statement);
		getFuture(future);
	}

	@Override
	public void query(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter, QueryListener listener) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		RecordSequenceToQueryListener adaptor = new RecordSequenceToQueryListener(listener, future);
		queryPartitions(null, adaptor, policy, statement, partitionFilter);
		getFuture(future);
	}

	@Override
	public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) {
		throw new AerospikeException(NotSupported + "queryNode");
	}

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

		PartitionTracker tracker = new PartitionTracker(policy, statement, 1, partitionFilter);
		QueryCommandProxy command = new QueryCommandProxy(executor, listener, policy,
			statement, partitionFilter, tracker);
		command.execute();
	}

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

	@Override
	public ResultSet queryAggregate(QueryPolicy policy, Statement statement) {
		if (policy == null) {
			policy = queryPolicyDefault;
		}

		QueryAggregateCommandProxy commandProxy = new QueryAggregateCommandProxy(
			executor, threadPool, policy, statement);
		commandProxy.execute();
		return commandProxy.getResultSet();
	}

	@Override
	public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) {
		throw new AerospikeException(NotSupported + "queryAggregateNode");
	}

	//--------------------------------------------------------
	// Secondary Index functions
	//--------------------------------------------------------

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

	@Override
	public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName) {
		throw new AerospikeException(NotSupported + "dropIndex");
	}

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

	@Override
	public void info(EventLoop eventLoop, InfoListener listener, InfoPolicy policy, Node node, String... commands) {
		throw new AerospikeException(NotSupported + "info");
	}

	//-----------------------------------------------------------------
	// XDR - Cross datacenter replication
	//-----------------------------------------------------------------

	@Override
	public void setXDRFilter(InfoPolicy policy, String datacenter, String namespace, Expression filter) {
		throw new AerospikeException(NotSupported + "setXDRFilter");
	}

	//-------------------------------------------------------
	// User administration
	//-------------------------------------------------------

	@Override
	public void createUser(AdminPolicy policy, String user, String password, List<String> roles) {
		throw new AerospikeException(NotSupported + "createUser");
	}

	@Override
	public void dropUser(AdminPolicy policy, String user) {
		throw new AerospikeException(NotSupported + "dropUser");
	}

	@Override
	public void changePassword(AdminPolicy policy, String user, String password) {
		throw new AerospikeException(NotSupported + "changePassword");
	}

	@Override
	public void grantRoles(AdminPolicy policy, String user, List<String> roles) {
		throw new AerospikeException(NotSupported + "grantRoles");
	}

	@Override
	public void revokeRoles(AdminPolicy policy, String user, List<String> roles) {
		throw new AerospikeException(NotSupported + "revokeRoles");
	}

	@Override
	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) {
		throw new AerospikeException(NotSupported + "createRole");
	}

	@Override
	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist) {
		throw new AerospikeException(NotSupported + "createRole");
	}

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

	@Override
	public void dropRole(AdminPolicy policy, String roleName) {
		throw new AerospikeException(NotSupported + "dropRole");
	}

	@Override
	public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) {
		throw new AerospikeException(NotSupported + "grantPrivileges");
	}

	@Override
	public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) {
		throw new AerospikeException(NotSupported + "revokePrivileges");
	}

	@Override
	public void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist) {
		throw new AerospikeException(NotSupported + "setWhitelist");
	}

	@Override
	public void setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota) {
		throw new AerospikeException(NotSupported + "setQuotas");
	}

	@Override
	public User queryUser(AdminPolicy policy, String user) {
		throw new AerospikeException(NotSupported + "queryUser");
	}

	@Override
	public List<User> queryUsers(AdminPolicy policy) {
		throw new AerospikeException(NotSupported + "queryUsers");
	}

	@Override
	public Role queryRole(AdminPolicy policy, String roleName) {
		throw new AerospikeException(NotSupported + "queryRole");
	}

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
