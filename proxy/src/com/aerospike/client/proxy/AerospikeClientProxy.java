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
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
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
import com.aerospike.client.proxy.auth.AuthTokenManager;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcChannelProvider;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.Util;

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

	private static String NotSupported = "Method not supported in proxy client: ";

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
	 * Default user defined function policy used in batch UDF excecute commands.
	 */
	public final BatchUDFPolicy batchUDFPolicyDefault;

	/**
	 * Default info policy that is used when info command policy is null.
	 */
	public final InfoPolicy infoPolicyDefault;

	private final WritePolicy operatePolicyReadDefault;
	private final AuthTokenManager authTokenManager;
	private final GrpcCallExecutor grpcCallExecutor;

	//-------------------------------------------------------
	// Constructors
	//-------------------------------------------------------

	public AerospikeClientProxy(ClientPolicy policy, Host... hosts) {
		if (Version == null) {
			throw new AerospikeException("Failed to retrieve client version from resource");
		}

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

		GrpcChannelProvider channelProvider = new GrpcChannelProvider();
		authTokenManager = new AuthTokenManager(policy, channelProvider);

		try {
			grpcCallExecutor = new GrpcCallExecutor(policy, authTokenManager, hosts);
			channelProvider.setCallExecutor(grpcCallExecutor);
		}
		catch (Throwable e) {
			authTokenManager.close();
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
			grpcCallExecutor.close();
		}
		catch (Throwable e) {
			Log.warn("Failed to close grpcCallExecutor: " + Util.getErrorMessage(e));
		}

		try {
			authTokenManager.close();
		}
		catch (Throwable e) {
			Log.warn("Failed to close authTokenManager: " + Util.getErrorMessage(e));
		}
	}

	@Override
	public boolean isConnected() {
		return grpcCallExecutor != null;
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
		CompletableFuture<Void> future = new CompletableFuture<Void>();
		WriteListener listener = prepareWriteListener(future);
		put(null, listener, policy, key, bins);
		getFuture(future);
	}

	@Override
	public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(grpcCallExecutor, listener, policy, key, bins, Operation.Type.WRITE);
		command.execute();
	}

	//-------------------------------------------------------
	// String Operations
	//-------------------------------------------------------

	@Override
	public void append(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<Void>();
		WriteListener listener = prepareWriteListener(future);
		append(null, listener, policy, key, bins);
		getFuture(future);
	}

	@Override
	public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(grpcCallExecutor, listener, policy, key, bins, Operation.Type.APPEND);
		command.execute();
	}

	@Override
	public void prepend(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<Void>();
		WriteListener listener = prepareWriteListener(future);
		prepend(null, listener, policy, key, bins);
		getFuture(future);
	}

	@Override
	public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(grpcCallExecutor, listener, policy, key, bins, Operation.Type.PREPEND);
		command.execute();
	}

	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------

	@Override
	public void add(WritePolicy policy, Key key, Bin... bins) {
		CompletableFuture<Void> future = new CompletableFuture<Void>();
		WriteListener listener = prepareWriteListener(future);
		add(null, listener, policy, key, bins);
		getFuture(future);
	}

	@Override
	public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		WriteCommandProxy command = new WriteCommandProxy(grpcCallExecutor, listener, policy, key, bins, Operation.Type.ADD);
		command.execute();
	}

	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------

	@Override
	public boolean delete(WritePolicy policy, Key key) {
		CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();
		DeleteListener listener = prepareDeleteListener(future);
		delete(null, listener, policy, key);
		return getFuture(future);
	}

	@Override
	public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) {
		if (policy == null) {
			policy = writePolicyDefault;
		}
		DeleteCommandProxy command = new DeleteCommandProxy(grpcCallExecutor, listener, policy, key);
		command.execute();
	}

	@Override
	public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys) {
		throw new AerospikeException(NotSupported + "batch delete");
	}

	@Override
	public void delete(
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy batchPolicy,
		BatchDeletePolicy deletePolicy,
		Key[] keys
	) {
		throw new AerospikeException(NotSupported + "batch delete");
	}

	@Override
	public void delete(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy batchPolicy,
		BatchDeletePolicy deletePolicy,
		Key[] keys
	) {
		throw new AerospikeException(NotSupported + "batch delete");
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
	}

	@Override
	public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) {
	}

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------

	@Override
	public boolean exists(Policy policy, Key key) {
		return false;
	}

	@Override
	public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) {
	}

	@Override
	public boolean[] exists(BatchPolicy policy, Key[] keys) {
		return new boolean[0];
	}

	@Override
	public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys) {
	}

	@Override
	public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys) {
	}

	//-------------------------------------------------------
	// Read Record Operations
	//-------------------------------------------------------

	@Override
	public Record get(Policy policy, Key key) {
		return get(policy, key, (String[]) null);
	}

	@Override
	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) {
		get(eventLoop, listener, policy, key, (String[]) null);
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
		ReadCommandProxy command = new ReadCommandProxy(grpcCallExecutor, listener, policy, key, binNames);
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
		ReadHeaderCommandProxy command = new ReadHeaderCommandProxy(grpcCallExecutor, listener, policy, key);
		command.execute();
	}

	@Override
	public boolean get(BatchPolicy policy, List<BatchRead> records) {
		return false;
	}

	@Override
	public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records) {
	}

	@Override
	public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) {
	}

	@Override
	public Record[] get(BatchPolicy policy, Key[] keys) {
		return new Record[0];
	}

	@Override
	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) {
	}

	@Override
	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) {
	}

	@Override
	public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) {
		return new Record[0];
	}

	@Override
	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames) {
	}

	@Override
	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames) {
	}

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	@Override
	public Record[] get(BatchPolicy policy, Key[] keys, Operation... ops) {
		return new Record[0];
	}

	@Override
	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, Operation... ops) {
	}

	@Override
	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, Operation... ops) {
	}

	@Override
	public Record[] getHeader(BatchPolicy policy, Key[] keys) {
		return new Record[0];
	}

	@Override
	public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) {
	}

	@Override
	public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) {
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
		OperateCommandProxy command = new OperateCommandProxy(grpcCallExecutor, listener, policy, key, args);
		command.execute();
	}

	//-------------------------------------------------------
	// Batch Read/Write Operations
	//-------------------------------------------------------

	@Override
	public boolean operate(BatchPolicy policy, List<BatchRecord> records) {
		return false;
	}

	@Override
	public void operate(
		EventLoop eventLoop,
		BatchOperateListListener listener,
		BatchPolicy policy,
		List<BatchRecord> records
	) {
	}

	@Override
	public void operate(
		EventLoop eventLoop,
		BatchRecordSequenceListener listener,
		BatchPolicy policy,
		List<BatchRecord> records
	) {
	}

	@Override
	public BatchResults operate(
		BatchPolicy batchPolicy,
		BatchWritePolicy writePolicy,
		Key[] keys,
		Operation... ops
	) {
		return null;
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
	}

	//---------------------------------------------------------------
	// User defined functions
	//---------------------------------------------------------------

	@Override
	public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) {
		return null;
	}

	@Override
	public RegisterTask register(
		Policy policy,
		ClassLoader resourceLoader,
		String resourcePath,
		String serverPath,
		Language language
	) {
		return null;
	}

	@Override
	public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) {
		return null;
	}

	@Override
	public void removeUdf(InfoPolicy policy, String serverPath) {
	}

	@Override
	public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) {
		return null;
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
		return null;
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
	}

	//----------------------------------------------------------
	// Query/Execute
	//----------------------------------------------------------

	@Override
	public ExecuteTask execute(
		WritePolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) {
		return null;
	}

	@Override
	public ExecuteTask execute(WritePolicy policy, Statement statement, Operation... operations) {
		return null;
	}

	//--------------------------------------------------------
	// Query functions
	//--------------------------------------------------------

	@Override
	public RecordSet query(QueryPolicy policy, Statement statement) {
		return null;
	}

	@Override
	public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement) {
	}

	@Override
	public void query(QueryPolicy policy, Statement statement, QueryListener listener) {
	}

	@Override
	public void query(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter, QueryListener listener) {
	}

	@Override
	public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) {
		return null;
	}

	@Override
	public RecordSet queryPartitions(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter) {
		return null;
	}

	@Override
	public void queryPartitions(
		EventLoop eventLoop,
		RecordSequenceListener listener,
		QueryPolicy policy,
		Statement statement,
		PartitionFilter partitionFilter
	) {
	}

	@Override
	public ResultSet queryAggregate(
		QueryPolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) {
		return null;
	}

	@Override
	public ResultSet queryAggregate(QueryPolicy policy, Statement statement) {
		return null;
	}

	@Override
	public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) {
		return null;
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
		return null;
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
		return null;
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
	}

	@Override
	public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName) {
		return null;
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
	}

	//-----------------------------------------------------------------
	// Async Info functions (sync info functions located in Info class)
	//-----------------------------------------------------------------

	@Override
	public void info(EventLoop eventLoop, InfoListener listener, InfoPolicy policy, Node node, String... commands) {
	}

	//-----------------------------------------------------------------
	// XDR - Cross datacenter replication
	//-----------------------------------------------------------------

	@Override
	public void setXDRFilter(InfoPolicy policy, String datacenter,
							 String namespace, Expression filter) {

	}

	//-------------------------------------------------------
	// User administration
	//-------------------------------------------------------

	@Override
	public void createUser(AdminPolicy policy, String user, String password, List<String> roles) {
	}

	@Override
	public void dropUser(AdminPolicy policy, String user) {
	}

	@Override
	public void changePassword(AdminPolicy policy, String user, String password) {
	}

	@Override
	public void grantRoles(AdminPolicy policy, String user, List<String> roles) {
	}

	@Override
	public void revokeRoles(AdminPolicy policy, String user, List<String> roles) {
	}

	@Override
	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) {
	}

	@Override
	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist) {
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
	}

	@Override
	public void dropRole(AdminPolicy policy, String roleName) {
	}

	@Override
	public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) {
	}

	@Override
	public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) {
	}

	@Override
	public void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist) {
	}

	@Override
	public void setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota) {

	}

	@Override
	public User queryUser(AdminPolicy policy, String user) {
		return null;
	}

	@Override
	public List<User> queryUsers(AdminPolicy policy) {
		return null;
	}

	@Override
	public Role queryRole(AdminPolicy policy, String roleName) {
		return null;
	}

	@Override
	public List<Role> queryRoles(AdminPolicy policy) {
		return null;
	}

	//-------------------------------------------------------
	// Internal Methods
	//-------------------------------------------------------

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

	private static <T> T getFuture(final CompletableFuture<T> future) {
		try {
			return future.get();
		}
		catch (ExecutionException e) {
			throw new AerospikeException(e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AerospikeException(e);
		}
	}
}
