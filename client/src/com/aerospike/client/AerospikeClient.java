/*
 * Copyright 2012-2025 Aerospike, Inc.
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
import java.util.Optional;

import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.AsyncBatch;
import com.aerospike.client.async.AsyncBatchExecutor;
import com.aerospike.client.async.AsyncBatchSingle;
import com.aerospike.client.async.AsyncCommand;
import com.aerospike.client.async.AsyncDelete;
import com.aerospike.client.async.AsyncExecute;
import com.aerospike.client.async.AsyncExists;
import com.aerospike.client.async.AsyncIndexTask;
import com.aerospike.client.async.AsyncInfoCommand;
import com.aerospike.client.async.AsyncOperateRead;
import com.aerospike.client.async.AsyncOperateWrite;
import com.aerospike.client.async.AsyncQueryExecutor;
import com.aerospike.client.async.AsyncQueryPartitionExecutor;
import com.aerospike.client.async.AsyncRead;
import com.aerospike.client.async.AsyncReadHeader;
import com.aerospike.client.async.AsyncScanPartitionExecutor;
import com.aerospike.client.async.AsyncTouch;
import com.aerospike.client.async.AsyncTxnMonitor;
import com.aerospike.client.async.AsyncTxnRoll;
import com.aerospike.client.async.AsyncWrite;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Batch;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.BatchExecutor;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.command.BatchSingle;
import com.aerospike.client.command.BatchStatus;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.DeleteCommand;
import com.aerospike.client.command.ExecuteCommand;
import com.aerospike.client.command.Executor;
import com.aerospike.client.command.ExistsCommand;
import com.aerospike.client.command.IBatchCommand;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.command.OperateCommandRead;
import com.aerospike.client.command.OperateCommandWrite;
import com.aerospike.client.command.ReadCommand;
import com.aerospike.client.command.ReadHeaderCommand;
import com.aerospike.client.command.RegisterCommand;
import com.aerospike.client.command.ScanExecutor;
import com.aerospike.client.command.TouchCommand;
import com.aerospike.client.command.TxnMonitor;
import com.aerospike.client.command.TxnRoll;
import com.aerospike.client.command.WriteCommand;
import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.YamlConfigProvider;
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
import com.aerospike.client.listener.AbortListener;
import com.aerospike.client.listener.CommitListener;
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
import com.aerospike.client.policy.TxnRollPolicy;
import com.aerospike.client.policy.TxnVerifyPolicy;
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
import com.aerospike.client.util.Version;

/**
 * Main client to access an Aerospike cluster and perform database operations (get, put, query, batch, etc.).
 * <p>
 * Thread-safe; use one instance per cluster and share it across threads. Implements {@link IAerospikeClient}. Use {@link ClientPolicy} and {@link Host} (or seed strings) to construct.
 *
 * <p><b>Example:</b>
 * <p>Create a client, put a record, and get it back.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *   Key key = new Key("test", "set1", "id1");
 *   client.put(null, key, new Bin("name", "Alice"));
 *   Record rec = client.get(null, key);
 * } finally {
 *   client.close();
 * }
 * }</pre>
 *
 * @see IAerospikeClient
 * @see ClientPolicy
 * @see Host
 * @see Key
 * @see Record
 */
public class AerospikeClient implements IAerospikeClient, Closeable {
	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------

	private String version;

	private ConfigurationProvider configProvider;

	protected Cluster cluster;

	/**
	 * Default read policy that is used when read command policy is null.
	 */
	public final Policy readPolicyDefault;
	private Policy mergedReadPolicyDefault;

	/**
	 * Default write policy that is used when write command policy is null.
	 */
	public final WritePolicy writePolicyDefault;
	private WritePolicy mergedWritePolicyDefault;

	/**
	 * Default scan policy that is used when scan command policy is null.
	 */
	public final ScanPolicy scanPolicyDefault;
	private ScanPolicy mergedScanPolicyDefault;

	/**
	 * Default query policy that is used when query command policy is null.
	 */
	public final QueryPolicy queryPolicyDefault;
	private QueryPolicy mergedQueryPolicyDefault;

	/**
	 * Default parent policy used in batch read commands. Parent policy fields
	 * include socketTimeout, totalTimeout, maxRetries, etc...
	 */
	public final BatchPolicy batchPolicyDefault;
	private BatchPolicy mergedBatchPolicyDefault;

	/**
	 * Default parent policy used in batch write commands. Parent policy fields
	 * include socketTimeout, totalTimeout, maxRetries, etc...
	 */
	public final BatchPolicy batchParentPolicyWriteDefault;
	private BatchPolicy mergedBatchParentPolicyWriteDefault;

	/**
	 * Default write policy used in batch operate commands.
	 * Write policy fields include generation, expiration, durableDelete, etc...
	 */
	public final BatchWritePolicy batchWritePolicyDefault;
	private BatchWritePolicy mergedBatchWritePolicyDefault;

	/**
	 * Default delete policy used in batch delete commands.
	 */
	public final BatchDeletePolicy batchDeletePolicyDefault;
	private BatchDeletePolicy mergedBatchDeletePolicyDefault;

	/**
	 * Default user defined function policy used in batch UDF execute commands.
	 */
	public final BatchUDFPolicy batchUDFPolicyDefault;
	private BatchUDFPolicy mergedBatchUDFPolicyDefault;

	/**
	 * Default info policy that is used when info command policy is null.
	 */
	public final InfoPolicy infoPolicyDefault;

	/**
	 * Default transaction policy when verifying record versions in a batch on a commit.
	 */
	public final TxnVerifyPolicy txnVerifyPolicyDefault;
	private TxnVerifyPolicy mergedTxnVerifyPolicyDefault;

	/**
	 * Default transaction policy when rolling the transaction records forward (commit)
	 * or back (abort) in a batch.
	 */
	public final TxnRollPolicy txnRollPolicyDefault;
	private TxnRollPolicy mergedTxnRollPolicyDefault;

	private final WritePolicy operatePolicyReadDefault;
	private WritePolicy mergedOperatePolicyReadDefault;

	private ClientPolicy mergedClientPolicy;

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
			throw new AerospikeException(
				"Log.setCallback() or Log.setCallbackStandard() must be called." + System.lineSeparator() +
				"See https://aerospike.com/docs/develop/client/java/logging/ for details.");
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
		this.txnVerifyPolicyDefault = policy.txnVerifyPolicyDefault;
		this.txnRollPolicyDefault = policy.txnRollPolicyDefault;
		this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);

		String configPath = YamlConfigProvider.getConfigPath();

		if (configPath != null) {
			this.configProvider = YamlConfigProvider.getConfigProvider(configPath);
		}
		else {
			this.configProvider = null;
		}

		mergedClientPolicy = policy;
		mergedReadPolicyDefault = readPolicyDefault;
		mergedWritePolicyDefault = writePolicyDefault;
		mergedScanPolicyDefault = scanPolicyDefault;
		mergedQueryPolicyDefault = queryPolicyDefault;
		mergedBatchPolicyDefault = batchPolicyDefault;
		mergedBatchParentPolicyWriteDefault = batchParentPolicyWriteDefault;
		mergedBatchWritePolicyDefault = batchWritePolicyDefault;
		mergedBatchDeletePolicyDefault = batchDeletePolicyDefault;
		mergedBatchUDFPolicyDefault = batchUDFPolicyDefault;
		mergedTxnVerifyPolicyDefault = txnVerifyPolicyDefault;
		mergedTxnRollPolicyDefault = txnRollPolicyDefault;
		mergedOperatePolicyReadDefault = operatePolicyReadDefault;

		if (configProvider != null) {
			mergePoliciesWithConfig();
		}

		version = Optional.ofNullable(getClass().getPackage())
				.map(Package::getImplementationVersion)
				.orElse("n/a");

		cluster = new Cluster(this, mergedClientPolicy, configPath, hosts);
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
			this.txnVerifyPolicyDefault = policy.txnVerifyPolicyDefault;
			this.txnRollPolicyDefault = policy.txnRollPolicyDefault;
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
			this.txnVerifyPolicyDefault = new TxnVerifyPolicy();
			this.txnRollPolicyDefault = new TxnRollPolicy();
			this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);
		}
	}

	/**
	 * Return the client's ConfigurationProvider.
	 */
	public ConfigurationProvider getConfigProvider() {
		return configProvider;
	}

	/**
	 * Set client's ConfigurationProvider. For internal use only.
	 */
	public void setConfigProvider(ConfigurationProvider provider) {
		this.configProvider = provider;
	}

	/**
	 * Return the mergedClientPolicy.
	 */
	public ClientPolicy getClientPolicy() {
		return mergedClientPolicy;
	}

	//-------------------------------------------------------
	// Default Policies
	//-------------------------------------------------------

	/**
	 * Return read policy default. Use when the policy will not be modified.
	 */
	public final Policy getReadPolicyDefault() {
		return readPolicyDefault;
	}

	/**
	 * Copy read policy default. Use when the policy will be modified for use in a specific command.
	 */
	public final Policy copyReadPolicyDefault() {
		return new Policy(readPolicyDefault);
	}

	/**
	 * Return write policy default. Use when the policy will not be modified.
	 */
	public final WritePolicy getWritePolicyDefault() {
		return writePolicyDefault;
	}

	/**
	 * Copy write policy default. Use when the policy will be modified for use in a specific command.
	 */
	public final WritePolicy copyWritePolicyDefault() {
		return new WritePolicy(writePolicyDefault);
	}

	/**
	 * Return scan policy default. Use when the policy will not be modified.
	 */
	public final ScanPolicy getScanPolicyDefault() {
		return scanPolicyDefault;
	}

	/**
	 * Copy scan policy default. Use when the policy will be modified for use in a specific command.
	 */
	public final ScanPolicy copyScanPolicyDefault() {
		return new ScanPolicy(scanPolicyDefault);
	}

	/**
	 * Return query policy default. Use when the policy will not be modified.
	 */
	public final QueryPolicy getQueryPolicyDefault() {
		return queryPolicyDefault;
	}

	/**
	 * Copy query policy default. Use when the policy will be modified for use in a specific command.
	 */
	public final QueryPolicy copyQueryPolicyDefault() {
		return new QueryPolicy(queryPolicyDefault);
	}

	/**
	 * Return batch header read policy default. Use when the policy will not be modified.
	 */
	public final BatchPolicy getBatchPolicyDefault() {
		return batchPolicyDefault;
	}

	/**
	 * Copy batch header read policy default. Use when the policy will be modified for use in a
	 * specific command.
	 */
	public final BatchPolicy copyBatchPolicyDefault() {
		return new BatchPolicy(batchPolicyDefault);
	}

	/**
	 * Return batch header write policy default. Use when the policy will not be modified.
	 */
	public final BatchPolicy getBatchParentPolicyWriteDefault() {
		return batchParentPolicyWriteDefault;
	}

	/**
	 * Copy batch header write policy default. Use when the policy will be modified for use in a
	 * specific command.
	 */
	public final BatchPolicy copyBatchParentPolicyWriteDefault() {
		return new BatchPolicy(batchParentPolicyWriteDefault);
	}

	/**
	 * Return batch detail write policy default. Use when the policy will not be modified.
	 */
	public final BatchWritePolicy getBatchWritePolicyDefault() {
		return batchWritePolicyDefault;
	}

	/**
	 * Copy batch detail write policy default. Use when the policy will be modified for use in a
	 * specific command.
	 */
	public final BatchWritePolicy copyBatchWritePolicyDefault() {
		return new BatchWritePolicy(batchWritePolicyDefault);
	}

	/**
	 * Return batch detail delete policy default. Use when the policy will not be modified.
	 */
	public final BatchDeletePolicy getBatchDeletePolicyDefault() {
		return batchDeletePolicyDefault;
	}

	/**
	 * Copy batch detail delete policy default. Use when the policy will be modified for use in a
	 * specific command.
	 */
	public final BatchDeletePolicy copyBatchDeletePolicyDefault() {
		return new BatchDeletePolicy(batchDeletePolicyDefault);
	}

	/**
	 * Return batch detail UDF policy default. Use when the policy will not be modified.
	 */
	public final BatchUDFPolicy getBatchUDFPolicyDefault() {
		return batchUDFPolicyDefault;
	}

	/**
	 * Copy batch detail UDF policy default. Use when the policy will be modified for use in a
	 * specific command.
	 */
	public final BatchUDFPolicy copyBatchUDFPolicyDefault() {
		return new BatchUDFPolicy(batchUDFPolicyDefault);
	}

	/**
	 * Return info command policy default. Use when the policy will not be modified.
	 */
	public final InfoPolicy getInfoPolicyDefault() {
		return infoPolicyDefault;
	}

	/**
	 * Copy info command policy default. Use when the policy will be modified for use in a
	 * specific command.
	 */
	public final InfoPolicy copyInfoPolicyDefault() {
		return new InfoPolicy(infoPolicyDefault);
	}

	/**
	 * Return transaction record version verify policy default. Use when the policy will not be modified.
	 */
	public final TxnVerifyPolicy getTxnVerifyPolicyDefault() {
		return txnVerifyPolicyDefault;
	}

	/**
	 * Copy transaction record version verify policy default. Use when the policy will be modified for use
	 * in a specific command.
	 */
	public final TxnVerifyPolicy copyTxnVerifyPolicyDefault() {
		return new TxnVerifyPolicy(txnVerifyPolicyDefault);
	}

	/**
	 * Return transaction roll forward/back policy default. Use when the policy will not be modified.
	 */
	public final TxnRollPolicy getTxnRollPolicyDefault() {
		return txnRollPolicyDefault;
	}

	/**
	 * Copy transaction roll forward/back policy default. Use when the policy will be modified for use
	 * in a specific command.
	 */
	public final TxnRollPolicy copyTxnRollPolicyDefault() {
		return new TxnRollPolicy(txnRollPolicyDefault);
	}

	/**
	 * Merge the default policies and the current clientPolicy with any applicable config properties.  This should
	 * be done at init and every time the config is updated
	 */
	public void mergePoliciesWithConfig() {
		mergedClientPolicy = new ClientPolicy(mergedClientPolicy, configProvider, true);
		mergedReadPolicyDefault = new Policy(mergedReadPolicyDefault, configProvider, true);
		mergedWritePolicyDefault = new WritePolicy(mergedWritePolicyDefault, configProvider, true, "");
		mergedScanPolicyDefault = new ScanPolicy(mergedScanPolicyDefault, configProvider, true);
		mergedQueryPolicyDefault = new QueryPolicy(mergedQueryPolicyDefault, configProvider, true);
		mergedBatchPolicyDefault = new BatchPolicy(mergedBatchPolicyDefault, configProvider, true, "");
		mergedBatchParentPolicyWriteDefault = new BatchPolicy(mergedBatchParentPolicyWriteDefault, configProvider, true, "(Parent)");
		mergedBatchWritePolicyDefault = new BatchWritePolicy(mergedBatchWritePolicyDefault, configProvider, true);
		mergedBatchDeletePolicyDefault = new BatchDeletePolicy(mergedBatchDeletePolicyDefault, configProvider, true);
		mergedBatchUDFPolicyDefault = new BatchUDFPolicy(mergedBatchUDFPolicyDefault, configProvider, true);
		mergedTxnVerifyPolicyDefault = new TxnVerifyPolicy(mergedTxnVerifyPolicyDefault, configProvider, true);
		mergedTxnRollPolicyDefault = new TxnRollPolicy(mergedTxnRollPolicyDefault, configProvider, true);
		mergedOperatePolicyReadDefault = new WritePolicy(mergedOperatePolicyReadDefault, configProvider, true, "(Operate)");
	}

	/**
	 * Restore default values to the merged default policies. A "re-merging" with any existing config will also occur.
	 */
	public void restorePolicyDefaults() {
		mergedClientPolicy = new ClientPolicy(new ClientPolicy(), configProvider, true);
		mergedReadPolicyDefault = new Policy(readPolicyDefault, configProvider, true);
		mergedWritePolicyDefault = new WritePolicy(writePolicyDefault, configProvider, true, "");
		mergedScanPolicyDefault = new ScanPolicy(scanPolicyDefault, configProvider, true);
		mergedQueryPolicyDefault = new QueryPolicy(queryPolicyDefault, configProvider, true);
		mergedBatchPolicyDefault = new BatchPolicy(batchPolicyDefault, configProvider, true, "");
		mergedBatchParentPolicyWriteDefault = new BatchPolicy(batchParentPolicyWriteDefault, configProvider, true, "(Parent)");
		mergedBatchWritePolicyDefault = new BatchWritePolicy(batchWritePolicyDefault, configProvider, true);
		mergedBatchDeletePolicyDefault = new BatchDeletePolicy(batchDeletePolicyDefault, configProvider, true);
		mergedBatchUDFPolicyDefault = new BatchUDFPolicy(batchUDFPolicyDefault, configProvider, true);
		mergedTxnVerifyPolicyDefault = new TxnVerifyPolicy(txnVerifyPolicyDefault, configProvider, true);
		mergedTxnRollPolicyDefault = new TxnRollPolicy(txnRollPolicyDefault, configProvider, true);
		mergedOperatePolicyReadDefault = new WritePolicy(operatePolicyReadDefault, configProvider, true, "(Operate)");
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
	 * Enable extended periodic cluster and node latency metrics.
	 */
	public final void enableMetrics(MetricsPolicy policy) {
		cluster.enableMetrics(policy);
	}

	/**
	 * Disable extended periodic cluster and node latency metrics.
	 */
	public final void disableMetrics() {
		cluster.disableMetrics();
	}

	/**
	 * Return operating cluster statistics snapshot.
	 */
	public final ClusterStats getClusterStats() {
		return cluster.getStats();
	}

	/**
	 * Asynchronously return operating cluster statistics snapshot.
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

	/**
	 * Return the client version
	 */
	public String getVersion() {
		return version;
	}

	//-------------------------------------------------------
	// Transaction
	//-------------------------------------------------------

	/**
	 * Attempt to commit the given transaction. First, the expected record versions are
	 * sent to the server nodes for verification. If all nodes return success, the transaction is
	 * committed. Otherwise, the transaction is aborted.
	 * <p>
	 * Requires server version 8.0+
	 * <p>
	 * <p>Commit a transaction after put; check returned status.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key key = new Key("ns", "set", "txnkey");
	 * Txn txn = new Txn();
	 * WritePolicy wp = client.copyWritePolicyDefault();
	 * wp.txn = txn;
	 * client.put(wp, key, new Bin("bin1", "val1"));
	 * CommitStatus status = client.commit(txn);
	 * client.close();
	 * }</pre>
	 *
	 * @param txn	transaction
	 * @return		status of the commit on success
	 * @throws AerospikeException.Commit	when verify or commit fails
	 * @see #commit(EventLoop, CommitListener, Txn)
	 * @see #abort(Txn)
	 * @see Txn
	 */
	public final CommitStatus commit(Txn txn)
		throws AerospikeException.Commit {

		TxnRoll tr = new TxnRoll(cluster, txn);

		switch (txn.getState()) {
			default:
			case OPEN:
				tr.verify(mergedTxnVerifyPolicyDefault, mergedTxnRollPolicyDefault);
				return tr.commit(mergedTxnRollPolicyDefault);

			case VERIFIED:
				return tr.commit(mergedTxnRollPolicyDefault);

			case COMMITTED:
				return CommitStatus.ALREADY_COMMITTED;

			case ABORTED:
				throw new AerospikeException(ResultCode.TXN_ALREADY_ABORTED, "Transaction already aborted");
		}
	}

	/**
	 * Asynchronously attempt to commit the given transaction. First, the expected
	 * record versions are sent to the server nodes for verification. If all nodes return success,
	 * the transaction is committed. Otherwise, the transaction is aborted.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Requires server version 8.0+
	 * <p>
	 * <p>Async commit: put with txn policy then commit via event loop and listener.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Txn txn = new Txn();
	 * WritePolicy wp = client.copyWritePolicyDefault();
	 * wp.txn = txn;
	 * client.put(loop, null, wp, key, new Bin("bin1", "val1"));
	 * client.commit(loop, new CommitListener() { ... }, txn);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param txn			transaction
	 * @throws AerospikeException	when event loop registration fails
	 * @see #commit(Txn)
	 * @see CommitListener
	 */
	public final void commit(EventLoop eventLoop, CommitListener listener, Txn txn)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		AsyncTxnRoll atr = new AsyncTxnRoll(
			cluster, eventLoop, mergedTxnVerifyPolicyDefault, mergedTxnRollPolicyDefault, txn
			);

		switch (txn.getState()) {
			default:
			case OPEN:
				atr.verify(listener);
				break;

			case VERIFIED:
				atr.commit(listener);
				break;

			case COMMITTED:
				listener.onSuccess(CommitStatus.ALREADY_COMMITTED);
				break;

			case ABORTED:
				throw new AerospikeException(ResultCode.TXN_ALREADY_ABORTED, "Transaction already aborted");
		}
	}

	/**
	 * Abort and rollback the given transaction.
	 * <p>
	 * Requires server version 8.0+
	 * <p>
	 * <p>Abort a transaction and check returned status.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Txn txn = new Txn();
	 * WritePolicy wp = client.copyWritePolicyDefault();
	 * wp.txn = txn;
	 * client.put(wp, key, new Bin("bin1", "val1"));
	 * AbortStatus status = client.abort(txn);
	 * client.close();
	 * }</pre>
	 *
	 * @param txn	transaction to abort
	 * @return		status of the abort
	 * @throws AerospikeException	when transaction was already committed
	 * @see #commit(Txn)
	 * @see #abort(EventLoop, AbortListener, Txn)
	 * @see Txn
	 */
	public final AbortStatus abort(Txn txn) {
		TxnRoll tr = new TxnRoll(cluster, txn);

		switch (txn.getState()) {
			default:
			case OPEN:
			case VERIFIED:
				return tr.abort(mergedTxnRollPolicyDefault);

			case COMMITTED:
				throw new AerospikeException(ResultCode.TXN_ALREADY_COMMITTED, "Transaction already committed");

			case ABORTED:
				return AbortStatus.ALREADY_ABORTED;
		}
	}

	/**
	 * Asynchronously abort and rollback the given transaction.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Requires server version 8.0+
	 * <p>
	 * <p>Async abort: put with txn then abort via event loop and AbortListener.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Txn txn = new Txn();
	 * WritePolicy wp = client.copyWritePolicyDefault();
	 * wp.txn = txn;
	 * client.put(loop, null, wp, key, new Bin("bin1", "val1"));
	 * client.abort(loop, new AbortListener() { ... }, txn);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param txn			transaction to abort
	 * @throws AerospikeException	when event loop registration fails
	 * @see #abort(Txn)
	 * @see AbortListener
	 */
	public final void abort(EventLoop eventLoop, AbortListener listener, Txn txn)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		AsyncTxnRoll atr = new AsyncTxnRoll(cluster, eventLoop, null, mergedTxnRollPolicyDefault, txn);

		switch (txn.getState()) {
			default:
			case OPEN:
			case VERIFIED:
				atr.abort(listener);
				break;

			case COMMITTED:
				throw new AerospikeException(ResultCode.TXN_ALREADY_COMMITTED, "Transaction already committed");

			case ABORTED:
				listener.onSuccess(AbortStatus.ALREADY_ABORTED);
				break;
		}
	}

	//-------------------------------------------------------
	// Write Record Operations
	//-------------------------------------------------------

	/**
	 * Write record bins; policy controls timeout, expiration, and create/replace behavior.
	 * <p>
	 * <p>Put bins for a key then get the record.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key key = new Key("ns", "set", "mykey");
	 * client.put(null, key, new Bin("bin1", "value1"), new Bin("bin2", 42));
	 * Record rec = client.get(null, key);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy  write configuration, or null for defaults
	 * @param key     record key
	 * @param bins    bin name/value pairs
	 * @throws AerospikeException when write fails
	 * @see #get(Policy, Key)
	 * @see #put(EventLoop, WriteListener, WritePolicy, Key, Bin...)
	 */
	public final void put(WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKey(cluster, policy, key);
		}

		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.WRITE);
		command.execute();
	}

	/**
	 * Asynchronously write record bins.
	 * <p>
	 * <p>Async put via event loop and WriteListener.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.put(loop, writeListener, null, key, new Bin("bin1", "v"));
	 * }</pre>
	 *
	 * @param eventLoop  event loop; null to use round-robin
	 * @param listener   callback, or null for fire-and-forget
	 * @param policy     write configuration, or null for defaults
	 * @param key        record key
	 * @param bins       bin name/value pairs
	 * @throws AerospikeException when event loop registration fails
	 * @see #put(WritePolicy, Key, Bin...)
	 * @see WriteListener
	 */
	public final void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		AsyncWrite command = new AsyncWrite(cluster, listener, policy, key, bins, Operation.Type.WRITE);
		AsyncTxnMonitor.execute(eventLoop, cluster, policy, command);
	}

	//-------------------------------------------------------
	// String Operations
	//-------------------------------------------------------

	/**
	 * Append string values to existing bin values (strings only).
	 * <p>
	 * <p>Append a string to an existing bin.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.append(null, key, new Bin("name", "suffix"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy  write configuration, or null for defaults
	 * @param key     record key
	 * @param bins    bin name/value pairs
	 * @throws AerospikeException when append fails
	 * @see #put(WritePolicy, Key, Bin...)
	 * @see #append(EventLoop, WriteListener, WritePolicy, Key, Bin...)
	 */
	public final void append(WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKey(cluster, policy, key);
		}

		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.APPEND);
		command.execute();
	}

	/**
	 * Asynchronously append string values to existing bin values.
	 * <p>
	 * <p>Async append via event loop and WriteListener.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.append(loop, listener, null, key, new Bin("name", "suffix"));
	 * }</pre>
	 *
	 * @param eventLoop  event loop; null to use round-robin
	 * @param listener   callback, or null for fire-and-forget
	 * @param policy     write configuration, or null for defaults
	 * @param key        record key
	 * @param bins       bin name/value pairs
	 * @throws AerospikeException when event loop registration fails
	 * @see #append(WritePolicy, Key, Bin...)
	 */
	public final void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		AsyncWrite command = new AsyncWrite(cluster, listener, policy, key, bins, Operation.Type.APPEND);
		AsyncTxnMonitor.execute(eventLoop, cluster, policy, command);
	}

	/**
	 * Prepend string values to existing bin values (strings only).
	 * <p>
	 * <p>Prepend a string to an existing bin.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.prepend(null, key, new Bin("name", "prefix"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy  write configuration, or null for defaults
	 * @param key     record key
	 * @param bins    bin name/value pairs
	 * @throws AerospikeException when prepend fails
	 * @see #append(WritePolicy, Key, Bin...)
	 * @see #prepend(EventLoop, WriteListener, WritePolicy, Key, Bin...)
	 */
	public final void prepend(WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKey(cluster, policy, key);
		}

		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.PREPEND);
		command.execute();
	}

	/**
	 * Asynchronously prepend string values to existing bin values.
	 * <p>
	 * <p>Async prepend via event loop and WriteListener.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key key = new Key("ns", "set", "prependkey");
	 * client.prepend(loop, listener, null, key, new Bin("prependbin", "Hello "));
	 * }</pre>
	 *
	 * @param eventLoop  event loop; null to use round-robin
	 * @param listener   callback, or null for fire-and-forget
	 * @param policy     write configuration, or null for defaults
	 * @param key        record key
	 * @param bins       bin name/value pairs
	 * @throws AerospikeException when event loop registration fails
	 * @see #prepend(WritePolicy, Key, Bin...)
	 */
	public final void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		AsyncWrite command = new AsyncWrite(cluster, listener, policy, key, bins, Operation.Type.PREPEND);
		AsyncTxnMonitor.execute(eventLoop, cluster, policy, command);
	}

	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------

	/**
	 * Add integer/double bin values to existing bins; creates record/bin if absent.
	 * <p>
	 * <p>Add an integer to a bin (creates bin if absent).</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.add(null, key, new Bin("counter", 1));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy  write configuration, or null for defaults
	 * @param key     record key
	 * @param bins    bin name/value pairs (integer or double)
	 * @throws AerospikeException when add fails
	 * @see #operate(WritePolicy, Key, Operation...)
	 * @see #add(EventLoop, WriteListener, WritePolicy, Key, Bin...)
	 */
	public final void add(WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKey(cluster, policy, key);
		}

		WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.ADD);
		command.execute();
	}

	/**
	 * Asynchronously add integer/double bin values to existing values.
	 * <p>
	 * <p>Async add via event loop and WriteListener.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key key = new Key("ns", "set", "addkey");
	 * client.add(loop, listener, null, key, new Bin("addbin", 10));
	 * }</pre>
	 *
	 * @param eventLoop  event loop; null to use round-robin
	 * @param listener   callback, or null for fire-and-forget
	 * @param policy     write configuration, or null for defaults
	 * @param key        record key
	 * @param bins       bin name/value pairs (integer or double)
	 * @throws AerospikeException when event loop registration fails
	 * @see #add(WritePolicy, Key, Bin...)
	 */
	public final void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		AsyncWrite command = new AsyncWrite(cluster, listener, policy, key, bins, Operation.Type.ADD);
		AsyncTxnMonitor.execute(eventLoop, cluster, policy, command);
	}

	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------

	/**
	 * Delete the record for the given key.
	 * <p>
	 * <p>Delete a record and check if it existed.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * boolean existed = client.delete(null, key);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy  write configuration, or null for defaults
	 * @param key     record key
	 * @return        true if record existed before deletion
	 * @throws AerospikeException when delete fails
	 * @see #delete(BatchPolicy, BatchDeletePolicy, Key[])
	 * @see #delete(EventLoop, DeleteListener, WritePolicy, Key)
	 */
	public final boolean delete(WritePolicy policy, Key key)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKey(cluster, policy, key);
		}

		DeleteCommand command = new DeleteCommand(cluster, policy, key);
		command.execute();
		return command.existed();
	}

	/**
	 * Asynchronously delete the record for the given key.
	 * <p>
	 * <p>Async delete via event loop and DeleteListener.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key key = new Key("ns", "set", "delkey");
	 * client.delete(loop, new DeleteListener() { ... }, null, key);
	 * }</pre>
	 *
	 * @param eventLoop  event loop; null to use round-robin
	 * @param listener   callback, or null for fire-and-forget
	 * @param policy     write configuration, or null for defaults
	 * @param key        record key
	 * @throws AerospikeException when event loop registration fails
	 * @see #delete(WritePolicy, Key)
	 * @see DeleteListener
	 */
	public final void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		AsyncDelete command = new AsyncDelete(cluster, listener, policy, key);
		AsyncTxnMonitor.execute(eventLoop, cluster, policy, command);
	}

	/**
	 * Delete records for the given keys in a single batch; missing keys get {@link ResultCode#KEY_NOT_FOUND_ERROR}.
	 * <p>
	 * Requires server 6.0+.
	 * <p>
	 * <p>Batch delete keys and get results per key.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * BatchResults results = client.delete(null, null, keys);
	 * client.close();
	 * }</pre>
	 *
	 * @param batchPolicy  batch configuration, or null for defaults
	 * @param deletePolicy delete configuration, or null for defaults
	 * @param keys         keys to delete
	 * @return             results per key
	 * @throws AerospikeException.BatchRecordArray when some keys fail (contains partial results)
	 * @see #delete(WritePolicy, Key)
	 * @see #operate(BatchPolicy, List)
	 */
	public final BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			return new BatchResults(new BatchRecord[0], true);
		}

		if (batchPolicy == null) {
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}

		if (deletePolicy == null) {
			deletePolicy = mergedBatchDeletePolicyDefault;
		} else if (configProvider != null) {
			deletePolicy = new BatchDeletePolicy(deletePolicy, configProvider);
		}

		if (batchPolicy.txn != null) {
			TxnMonitor.addKeys(cluster, batchPolicy, keys);
		}

		BatchAttr attr = new BatchAttr();
		attr.setDelete(deletePolicy);

		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], attr.hasWrite);
		}

		try {
			BatchStatus status = new BatchStatus(true);
			List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];
			int count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.Delete(
						cluster, batchPolicy, attr, records[i], status, bn.node);
				}
				else {
					commands[count++] = new Batch.OperateArrayCommand(
						cluster, bn, batchPolicy, keys, null, records, attr, status);
				}
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
	 * Asynchronously delete records for the given keys; results delivered to listener as a single array.
	 * <p>
	 * Missing keys set {@link BatchRecord#resultCode} to {@link ResultCode#KEY_NOT_FOUND_ERROR}. Requires server 6.0+.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.delete(loop, new BatchRecordArrayListener() { ... }, null, null, keys);
	 * }</pre>
	 *
	 * @param eventLoop    event loop; null to use round-robin
	 * @param listener     callback for batch results
	 * @param batchPolicy  batch configuration, or null for defaults
	 * @param deletePolicy delete configuration, or null for defaults
	 * @param keys         keys to delete
	 * @throws AerospikeException when event loop registration fails
	 * @see #delete(BatchPolicy, BatchDeletePolicy, Key[])
	 * @see #delete(EventLoop, BatchRecordSequenceListener, BatchPolicy, BatchDeletePolicy, Key[])
	 * @see BatchRecordArrayListener
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
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}

		if (deletePolicy == null) {
			deletePolicy = mergedBatchDeletePolicyDefault;
		} else if (configProvider != null) {
			deletePolicy = new BatchDeletePolicy(deletePolicy, configProvider);
		}

		BatchAttr attr = new BatchAttr();
		attr.setDelete(deletePolicy);

		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], attr.hasWrite);
		}

		AsyncBatchExecutor.BatchRecordArray executor = new AsyncBatchExecutor.BatchRecordArray(
			eventLoop, cluster, listener, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.Delete(
					executor, cluster, batchPolicy, attr, records[i], bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.OperateRecordArrayCommand(
					executor, bn, batchPolicy, keys, null, records, attr);
			}
		}
		AsyncTxnMonitor.executeBatch(batchPolicy, executor, commands, keys);
	}

	/**
	 * Asynchronously delete records for the given keys; each result delivered via onRecord.
	 * <p>
	 * Missing keys set {@link BatchRecord#resultCode} to {@link ResultCode#KEY_NOT_FOUND_ERROR}. Requires server 6.0+.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.delete(loop, new BatchRecordSequenceListener() { ... }, null, null, keys);
	 * }</pre>
	 *
	 * @param eventLoop    event loop; null to use round-robin
	 * @param listener     callback for per-record results
	 * @param batchPolicy  batch configuration, or null for defaults
	 * @param deletePolicy delete configuration, or null for defaults
	 * @param keys         keys to delete
	 * @throws AerospikeException when event loop registration fails
	 * @see #delete(BatchPolicy, BatchDeletePolicy, Key[])
	 * @see #delete(EventLoop, BatchRecordArrayListener, BatchPolicy, BatchDeletePolicy, Key[])
	 * @see BatchRecordSequenceListener
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
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}

		if (deletePolicy == null) {
			deletePolicy = mergedBatchDeletePolicyDefault;
		} else if (configProvider != null) {
			deletePolicy = new BatchDeletePolicy(deletePolicy, configProvider);
		}

		BatchAttr attr = new BatchAttr();
		attr.setDelete(deletePolicy);

		boolean[] sent = new boolean[keys.length];
		AsyncBatchExecutor.BatchRecordSequence executor = new AsyncBatchExecutor.BatchRecordSequence(
			eventLoop, cluster, listener, sent);
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, null, attr.hasWrite, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.DeleteSequenceSent(
					executor, cluster, batchPolicy, keys[i], attr, bn.node, listener, i);
			}
			else {
				commands[count++] = new AsyncBatch.OperateRecordSequenceCommand(
					executor, bn, batchPolicy, keys, null, sent, listener, attr);
			}
		}
		AsyncTxnMonitor.executeBatch(batchPolicy, executor, commands, keys);
	}

	/**
	 * Remove records in namespace/set efficiently; many orders of magnitude faster than deleting one at a time.
	 * <p>
	 * Server call may return before truncation completes; new writes use last-update times after the cutoff.
	 * <p>Truncate a set in a namespace.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.truncate(null, "ns", "set1", null);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy             info configuration, or null for defaults
	 * @param ns                 namespace
	 * @param set                set name, or null for all sets in namespace
	 * @param beforeLastUpdate    delete records before this time, or null for all
	 * @throws AerospikeException when truncate fails
	 * @see <a href="https://www.aerospike.com/docs/reference/info#truncate">truncate</a>
	 * @see #delete(WritePolicy, Key)
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
	 * Reset record time-to-expiration using the policy; fails when the record does not exist.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.touch(null, key);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy             write configuration, or null for defaults
	 * @param key                record key
	 * @throws AerospikeException when touch fails or record does not exist
	 * @see #touched(WritePolicy, Key)
	 * @see #touch(EventLoop, WriteListener, WritePolicy, Key)
	 */
	public final void touch(WritePolicy policy, Key key)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKey(cluster, policy, key);
		}

		TouchCommand command = new TouchCommand(cluster, policy, key, true);
		command.execute();
	}

	/**
	 * Asynchronously reset record time-to-expiration using the policy.
	 * <p>
	 * @param eventLoop  event loop; null to use round-robin
	 * @param listener   callback, or null for fire-and-forget
	 * @param policy     write configuration, or null for defaults
	 * @param key        record key
	 * @throws AerospikeException when event loop registration fails
	 * @see #touch(WritePolicy, Key)
	 * @see WriteListener
	 */
	public final void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		AsyncTouch command = new AsyncTouch(cluster, listener, policy, key);
		AsyncTxnMonitor.execute(eventLoop, cluster, policy, command);
	}

	/**
	 * Reset record time-to-expiration; returns true if record existed and was touched, false if not found.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * boolean ok = client.touched(null, key);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy             write configuration, or null for defaults
	 * @param key                record key
	 * @return                   true if touched, false if record did not exist
	 * @throws AerospikeException when command fails
	 * @see #touch(WritePolicy, Key)
	 * @see #touched(EventLoop, ExistsListener, WritePolicy, Key)
	 */
	public final boolean touched(WritePolicy policy, Key key)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKey(cluster, policy, key);
		}

		TouchCommand command = new TouchCommand(cluster, policy, key, false);
		command.execute();
		return command.getTouched();
	}

	/**
	 * Asynchronously reset record time-to-expiration; listener receives false when record does not exist.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key key = new Key("ns", "set", "touchkey");
	 * client.touched(loop, new ExistsListener() { ... }, null, key);
	 * }</pre>
	 *
	 * @param eventLoop  event loop; null to use round-robin
	 * @param listener   callback
	 * @param policy     write configuration, or null for defaults
	 * @param key        record key
	 * @throws AerospikeException when event loop registration fails
	 * @see #touched(WritePolicy, Key)
	 * @see ExistsListener
	 */
	public final void touched(EventLoop eventLoop, ExistsListener listener, WritePolicy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		AsyncTouch command = new AsyncTouch(cluster, listener, policy, key);
		AsyncTxnMonitor.execute(eventLoop, cluster, policy, command);
	}

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------

	/**
	 * Check whether a record key exists.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * boolean found = client.exists(null, key);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy             read configuration, or null for defaults
	 * @param key                record key
	 * @return                   true if record exists
	 * @throws AerospikeException when command fails
	 * @see #get(Policy, Key)
	 * @see #exists(BatchPolicy, Key[])
	 * @see #exists(EventLoop, ExistsListener, Policy, Key)
	 */
	public final boolean exists(Policy policy, Key key)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedReadPolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(key.namespace);
		}

		ExistsCommand command = new ExistsCommand(cluster, policy, key);
		command.execute();
		return command.exists();
	}

	/**
	 * Asynchronously check whether a record key exists.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key key = new Key("ns", "set", "existskey");
	 * client.exists(loop, new ExistsListener() { ... }, null, key);
	 * }</pre>
	 *
	 * @param eventLoop  event loop; null to use round-robin
	 * @param listener   callback
	 * @param policy     read configuration, or null for defaults
	 * @param key        record key
	 * @throws AerospikeException when event loop registration fails
	 * @see #exists(Policy, Key)
	 * @see ExistsListener
	 */
	public final void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedReadPolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(key.namespace);
		}

		AsyncExists command = new AsyncExists(cluster, listener, policy, key);
		eventLoop.execute(cluster, command);
	}

	/**
	 * Check if multiple record keys exist in one batch call; result array matches key order.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * boolean[] found = client.exists(null, keys);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy  batch configuration, or null for defaults
	 * @param keys    keys to check
	 * @return        existence per key, same order as keys
	 * @throws AerospikeException.BatchExists when some keys fail (contains partial results)
	 * @see #exists(Policy, Key)
	 * @see #exists(EventLoop, ExistsArrayListener, BatchPolicy, Key[])
	 */
	public final boolean[] exists(BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			return new boolean[0];
		}

		if (policy == null) {
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		boolean[] existsArray = new boolean[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];
			int count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.Exists(
						cluster, policy, keys[i], existsArray, i, status, bn.node);
				}
				else {
					commands[count++] = new Batch.ExistsArrayCommand(
						cluster, bn, policy, keys, existsArray, status);
				}
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return existsArray;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchExists(existsArray, e);
		}
	}

	/**
	 * Asynchronously check if multiple record keys exist in one batch call; results in one callback as array.
	 * <p>
	 * Result array order matches key order.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.exists(loop, new ExistsArrayListener() { ... }, null, keys);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			unique record identifiers
	 * @throws AerospikeException	when event loop registration fails
	 * @see #exists(BatchPolicy, Key[])
	 * @see #exists(EventLoop, ExistsSequenceListener, BatchPolicy, Key[])
	 * @see ExistsArrayListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		boolean[] existsArray = new boolean[keys.length];
		AsyncBatchExecutor.ExistsArray executor = new AsyncBatchExecutor.ExistsArray(
			eventLoop, cluster, listener, keys, existsArray);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.Exists(
					executor, cluster, policy, keys[i], bn.node, existsArray, i);
			}
			else {
				commands[count++] = new AsyncBatch.ExistsArrayCommand(
					executor, bn, policy, keys, existsArray);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Asynchronously check if multiple record keys exist in one batch call; each result via onExists().
	 * <p>
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.exists(loop, new ExistsSequenceListener() { ... }, null, keys);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			unique record identifiers
	 * @throws AerospikeException	when event loop registration fails
	 * @see #exists(BatchPolicy, Key[])
	 * @see #exists(EventLoop, ExistsArrayListener, BatchPolicy, Key[])
	 * @see ExistsSequenceListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		AsyncBatchExecutor.ExistsSequence executor = new AsyncBatchExecutor.ExistsSequence(
			eventLoop, cluster, listener);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.ExistsSequence(
					executor, cluster, policy, keys[i], bn.node, listener);
			}
			else {
				commands[count++] = new AsyncBatch.ExistsSequenceCommand(
					executor, bn, policy, keys, listener);
			}
		}
		executor.execute(commands);
	}

	//-------------------------------------------------------
	// Read Record Operations
	//-------------------------------------------------------

	/**
	 * Read entire record for specified key.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key key = new Key("ns", "set", "mykey");
	 * Record rec = client.get(null, key);
	 * if (rec != null) { String s = rec.getString("bin1"); }
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	when read fails
	 * @see #get(Policy, Key, String...)
	 * @see #get(EventLoop, RecordListener, Policy, Key)
	 */
	public final Record get(Policy policy, Key key)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedReadPolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(key.namespace);
		}

		ReadCommand command = new ReadCommand(cluster, policy, key);
		command.execute();
		return command.getRecord();
	}

	/**
	 * Asynchronously read entire record for specified key.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key key = new Key("ns", "set", "mykey");
	 * client.get(loop, new RecordListener() { ... }, null, key);
	 * }</pre>
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(Policy, Key)
	 * @see RecordListener
	 */
	public final void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedReadPolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(key.namespace);
		}

		AsyncRead command = new AsyncRead(cluster, listener, policy, key, null);
		eventLoop.execute(cluster, command);
	}

	/**
	 * Read record header and bins for specified key and bin names.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key key = new Key("ns", "set", "mykey");
	 * Record rec = client.get(null, key, "bin1", "bin2");
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	when read fails
	 * @see #get(Policy, Key)
	 * @see #get(EventLoop, RecordListener, Policy, Key, String...)
	 */
	public final Record get(Policy policy, Key key, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedReadPolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(key.namespace);
		}

		ReadCommand command = new ReadCommand(cluster, policy, key, binNames);
		command.execute();
		return command.getRecord();
	}

	/**
	 * Asynchronously read record header and bins for specified key and bin names.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.get(loop, new RecordListener() { ... }, null, key, "bin1", "bin2");
	 * }</pre>
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(Policy, Key, String...)
	 * @see RecordListener
	 */
	public final void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedReadPolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(key.namespace);
		}

		AsyncRead command = new AsyncRead(cluster, listener, policy, key, binNames);
		eventLoop.execute(cluster, command);
	}

	/**
	 * Read record generation and expiration only; bins are not read.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key key = new Key("ns", "set", "mykey");
	 * Record rec = client.getHeader(null, key);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	when read fails
	 * @see #get(Policy, Key)
	 * @see #getHeader(EventLoop, RecordListener, Policy, Key)
	 */
	public final Record getHeader(Policy policy, Key key)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedReadPolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(key.namespace);
		}

		ReadHeaderCommand command = new ReadHeaderCommand(cluster, policy, key);
		command.execute();
		return command.getRecord();
	}

	/**
	 * Asynchronously read record generation and expiration only; bins are not read.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.getHeader(loop, new RecordListener() { ... }, null, key);
	 * }</pre>
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	when event loop registration fails
	 * @see #getHeader(Policy, Key)
	 * @see RecordListener
	 */
	public final void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedReadPolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(key.namespace);
		}

		AsyncReadHeader command = new AsyncReadHeader(cluster, listener, policy, key);
		eventLoop.execute(cluster, command);
	}

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	/**
	 * Read multiple records for specified batch keys in one batch call; different bins per key allowed.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * List batchReads = java.util.Arrays.asList(
	 *   new BatchRead(new Key("ns", "set", "k1"), "bin1"),
	 *   new BatchRead(new Key("ns", "set", "k2"))
	 * );
	 * client.get(null, batchReads);
	 * for (BatchRead br : batchReads) { Record r = br.record; }
	 * client.close();
	 * }</pre>
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param records	list of unique record identifiers and the bins to retrieve.
	 *					The returned records are located in the same list.
	 * @return			true if all batch key requests succeeded
	 * @throws AerospikeException	when read fails
	 * @see #get(BatchPolicy, Key[])
	 * @see #get(EventLoop, BatchListListener, BatchPolicy, List)
	 */
	public final boolean get(BatchPolicy policy, List<BatchRead> records)
		throws AerospikeException {
		if (records.size() == 0) {
			return true;
		}

		if (policy == null) {
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(records);
		}

		BatchStatus status = new BatchStatus(true);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, records, status);
		IBatchCommand[] commands = new IBatchCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new BatchSingle.ReadRecord(cluster, policy, records.get(i), status, bn.node);
			}
			else {
				commands[count++] = new Batch.ReadListCommand(cluster, bn, policy, records, status);
			}
		}
		BatchExecutor.execute(cluster, policy, commands, status);
		return status.getStatus();
	}

	/**
	 * Asynchronously read multiple records for specified batch keys; results in the same list.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * List batchReads = java.util.Arrays.asList(new BatchRead(new Key("ns", "set", "k1")), new BatchRead(new Key("ns", "set", "k2")));
	 * client.get(loop, new BatchListListener() { ... }, null, batchReads);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and the bins to retrieve.
	 *						The returned records are located in the same list.
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(BatchPolicy, List)
	 * @see BatchListListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(records);
		}

		AsyncBatchExecutor.ReadList executor = new AsyncBatchExecutor.ReadList(eventLoop, cluster, listener, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, records, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.Read(executor, cluster, policy, records.get(i), bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.ReadListCommand(executor, bn, policy, records);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Asynchronously read multiple records for specified batch keys; each result via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * List batchReads = java.util.Arrays.asList(new BatchRead(new Key("ns", "set", "k1")), new BatchRead(new Key("ns", "set", "k2")));
	 * client.get(loop, new BatchSequenceListener() { ... }, null, batchReads);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of unique record identifiers and the bins to retrieve.
	 *						The returned records are located in the same list.
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(BatchPolicy, List)
	 * @see BatchSequenceListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(records);
		}

		AsyncBatchExecutor.ReadSequence executor = new AsyncBatchExecutor.ReadSequence(eventLoop, cluster, listener);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, records, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.ReadGetSequence(
					executor, cluster, policy, records.get(i), bn.node, listener);
			}
			else {
				commands[count++] = new AsyncBatch.ReadSequenceCommand(
					executor, bn, policy, listener, records);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Read multiple records for specified keys in one batch call; result order matches key order.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * Record[] records = client.get(null, keys);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @return			array of records (null for missing keys)
	 * @throws AerospikeException.BatchRecords	when some keys fail (contains partial results)
	 * @see #get(BatchPolicy, List)
	 * @see #get(EventLoop, RecordArrayListener, BatchPolicy, Key[])
	 */
	public final Record[] get(BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			return new Record[0];
		}

		if (policy == null) {
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		Record[] records = new Record[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];
			int count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.Read(
						cluster, policy, keys[i], null, records, i, status, bn.node, false);
				}
				else {
					commands[count++] = new Batch.GetArrayCommand(
						cluster, bn, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_GET_ALL,
						false, status);
				}
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return records;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecords(records, e);
		}
	}

	/**
	 * Asynchronously read multiple records for specified keys; results in one callback as array.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.get(loop, new RecordArrayListener() { ... }, null, keys);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(BatchPolicy, Key[])
	 * @see RecordArrayListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		Record[] records = new Record[keys.length];
		AsyncBatchExecutor.GetArray executor = new AsyncBatchExecutor.GetArray(
			eventLoop, cluster, listener, keys, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.Get(
					executor, cluster, policy, keys[i], null, records, bn.node, i, false);
			}
			else {
				commands[count++] = new AsyncBatch.GetArrayCommand(
					executor, bn, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Asynchronously read multiple records for specified keys; each result via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.get(loop, new RecordSequenceListener() { ... }, null, keys);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(BatchPolicy, Key[])
	 * @see RecordSequenceListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		AsyncBatchExecutor.GetSequence executor = new AsyncBatchExecutor.GetSequence(eventLoop, cluster, listener);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.GetSequence(
					executor, cluster, policy, listener, keys[i], null, bn.node, false);
			}
			else {
				commands[count++] = new AsyncBatch.GetSequenceCommand(
					executor, bn, policy, keys, null, null, listener, Command.INFO1_READ | Command.INFO1_GET_ALL,
					false);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Read multiple records for specified keys and bin names; result order matches key order.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * Record[] records = client.get(null, keys, "bin1");
	 * client.close();
	 * }</pre>
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @param binNames	array of bins to retrieve
	 * @return			array of records (null for missing keys)
	 * @throws AerospikeException.BatchRecords	when some keys fail (contains partial results)
	 * @see #get(BatchPolicy, Key[])
	 * @see #get(EventLoop, RecordArrayListener, BatchPolicy, Key[], String...)
	 */
	public final Record[] get(BatchPolicy policy, Key[] keys, String... binNames)
		throws AerospikeException {
		if (keys.length == 0) {
			return new Record[0];
		}

		if (policy == null) {
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		int readAttr = (binNames == null || binNames.length == 0)?
			Command.INFO1_READ | Command.INFO1_GET_ALL : Command.INFO1_READ;

		Record[] records = new Record[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];
			int count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.Read(
						cluster, policy, keys[i], binNames, records, i, status, bn.node, false);
				}
				else {
					commands[count++] = new Batch.GetArrayCommand(
						cluster, bn, policy, keys, binNames, null, records, readAttr, false, status);
				}
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return records;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecords(records, e);
		}
	}

	/**
	 * Asynchronously read multiple records for specified keys and bin names; results in one callback.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.get(loop, new RecordArrayListener() { ... }, null, keys, "bin1");
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param binNames		array of bins to retrieve
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(BatchPolicy, Key[], String...)
	 * @see RecordArrayListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		int readAttr = (binNames == null || binNames.length == 0)?
			Command.INFO1_READ | Command.INFO1_GET_ALL : Command.INFO1_READ;

		Record[] records = new Record[keys.length];
		AsyncBatchExecutor.GetArray executor = new AsyncBatchExecutor.GetArray(
			eventLoop, cluster, listener, keys, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.Get(
					executor, cluster, policy, keys[i], binNames, records, bn.node, i, false);
			}
			else {
				commands[count++] = new AsyncBatch.GetArrayCommand(
					executor, bn, policy, keys, binNames, null, records, readAttr, false);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Asynchronously read multiple records for specified keys and bin names; each result via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.get(loop, new RecordSequenceListener() { ... }, null, keys, "bin1");
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param binNames		array of bins to retrieve
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(BatchPolicy, Key[], String...)
	 * @see RecordSequenceListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		int readAttr = (binNames == null || binNames.length == 0)?
			Command.INFO1_READ | Command.INFO1_GET_ALL : Command.INFO1_READ;

		AsyncBatchExecutor.GetSequence executor = new AsyncBatchExecutor.GetSequence(eventLoop, cluster, listener);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.GetSequence(
					executor, cluster, policy, listener, keys[i], binNames, bn.node, false);
			}
			else {
				commands[count++] = new AsyncBatch.GetSequenceCommand(
					executor, bn, policy, keys, binNames, null, listener, readAttr, false);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Read multiple records for specified keys using read operations; result order matches key order.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * Record[] records = client.get(null, keys, Operation.get("bin1"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @param ops		array of read operations on record
	 * @return			array of records (null for missing keys)
	 * @throws AerospikeException.BatchRecords	when some keys fail (contains partial results)
	 * @see #get(BatchPolicy, Key[])
	 * @see Operation
	 */
	public final Record[] get(BatchPolicy policy, Key[] keys, Operation... ops)
		throws AerospikeException {
		if (keys.length == 0) {
			return new Record[0];
		}

		if (policy == null) {
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		Record[] records = new Record[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];
			int count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.OperateRead(
						cluster, policy, keys[i], ops, records, i, status, bn.node);
				}
				else {
					commands[count++] = new Batch.GetArrayCommand(
						cluster, bn, policy, keys, null, ops, records, Command.INFO1_READ, true, status);
				}
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return records;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecords(records, e);
		}
	}

	/**
	 * Asynchronously read multiple records for specified keys using read operations; results in one callback.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.get(loop, new RecordArrayListener() { ... }, null, keys, Operation.get("bin1"));
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			array of read operations on record
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(BatchPolicy, Key[], Operation...)
	 * @see RecordArrayListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		Record[] records = new Record[keys.length];
		AsyncBatchExecutor.GetArray executor = new AsyncBatchExecutor.GetArray(
			eventLoop, cluster, listener, keys, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.OperateGet(
					executor, cluster, policy, keys[i], ops, records, bn.node, i);
			}
			else {
				commands[count++] = new AsyncBatch.GetArrayCommand(
					executor, bn, policy, keys, null, ops, records, Command.INFO1_READ, true);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Asynchronously read multiple records for specified keys using read operations; each result via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.get(loop, new RecordSequenceListener() { ... }, null, keys, Operation.get("bin1"));
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			array of read operations on record
	 * @throws AerospikeException	when event loop registration fails
	 * @see #get(BatchPolicy, Key[], Operation...)
	 * @see RecordSequenceListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		AsyncBatchExecutor.GetSequence executor = new AsyncBatchExecutor.GetSequence(eventLoop, cluster, listener);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.OperateGetSequence(
					executor, cluster, policy, listener, keys[i], ops, bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.GetSequenceCommand(
					executor, bn, policy, keys, null, ops, listener, Command.INFO1_READ, true);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Read multiple record headers (metadata only) for specified keys; result order matches key order.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * Record[] headers = client.getHeader(null, keys);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param keys		array of unique record identifiers
	 * @return			array of records (metadata only; null for missing keys)
	 * @throws AerospikeException.BatchRecords	when some keys fail (contains partial results)
	 * @see #getHeader(Policy, Key)
	 * @see #get(BatchPolicy, Key[])
	 */
	public final Record[] getHeader(BatchPolicy policy, Key[] keys)
		throws AerospikeException {
		if (keys.length == 0) {
			return new Record[0];
		}

		if (policy == null) {
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		Record[] records = new Record[keys.length];

		try {
			BatchStatus status = new BatchStatus(false);
			List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];
			int count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.ReadHeader(
						cluster, policy, keys[i], records, i, status, bn.node);
				}
				else {
					commands[count++] = new Batch.GetArrayCommand(
						cluster, bn, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_NOBINDATA,
						false, status);
				}
			}
			BatchExecutor.execute(cluster, policy, commands, status);
			return records;
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecords(records, e);
		}
	}

	/**
	 * Asynchronously read multiple record headers (metadata only); results in one callback.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.getHeader(loop, new RecordArrayListener() { ... }, null, keys);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	when event loop registration fails
	 * @see #getHeader(BatchPolicy, Key[])
	 * @see RecordArrayListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		Record[] records = new Record[keys.length];
		AsyncBatchExecutor.GetArray executor = new AsyncBatchExecutor.GetArray(
			eventLoop, cluster, listener, keys, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.ReadHeader(
					executor, cluster, policy, keys[i], records, bn.node, i);
			}
			else {
				commands[count++] = new AsyncBatch.GetArrayCommand(
					executor, bn, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_NOBINDATA,
					false);
			}
		}
		executor.execute(commands);
	}

	/**
	 * Asynchronously read multiple record headers (metadata only); each result via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.getHeader(loop, new RecordSequenceListener() { ... }, null, keys);
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @throws AerospikeException	when event loop registration fails
	 * @see #getHeader(BatchPolicy, Key[])
	 * @see RecordSequenceListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			policy.txn.prepareRead(keys);
		}

		AsyncBatchExecutor.GetSequence executor = new AsyncBatchExecutor.GetSequence(eventLoop, cluster, listener);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, null, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.ReadHeaderSequence(
					executor, cluster, policy, keys[i], bn.node, listener);
			}
			else {
				commands[count++] = new AsyncBatch.GetSequenceCommand(
					executor, bn, policy, keys, null, null, listener, Command.INFO1_READ | Command.INFO1_NOBINDATA,
					false);
			}
		}
		executor.execute(commands);
	}

	//-------------------------------------------------------
	// Generic Database Operations
	//-------------------------------------------------------

	/**
	 * Perform multiple read/write operations on a single key in one call.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key key = new Key("ns", "set", "mykey");
	 * Record rec = client.operate(null, key, Operation.add(new Bin("ctr", 1)), Operation.get("ctr"));
	 * client.close();
	 * }</pre>
	 * <p>
	 * The server executes operations in the same order as the operations array.
	 * Both scalar bin operations (Operation) and CDT bin operations (ListOperation,
	 * MapOperation) can be performed in same call.
	 * Operation results are stored with their associated bin name in the returned record.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @return						record with operation results
	 * @throws AerospikeException	when command fails
	 * @see #operate(EventLoop, RecordListener, WritePolicy, Key, Operation...)
	 * @see Operation
	 */
	public final Record operate(WritePolicy policy, Key key, Operation... operations)
		throws AerospikeException {
		OperateArgs args = new OperateArgs(policy, mergedWritePolicyDefault, mergedOperatePolicyReadDefault, operations);
		policy = args.writePolicy;

		if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (args.hasWrite) {
			if (policy.txn != null) {
				TxnMonitor.addKey(cluster, policy, key);
			}

			OperateCommandWrite command = new OperateCommandWrite(cluster, key, args);
			command.execute();
			return command.getRecord();
		}
		else {
			if (policy.txn != null) {
				policy.txn.prepareRead(key.namespace);
			}

			OperateCommandRead command = new OperateCommandRead(cluster, key, args);
			command.execute();
			return command.getRecord();
		}
	}

	/**
	 * Asynchronously perform multiple read/write operations on a single key in one call.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.operate(loop, recordListener, null, key, Operation.add(new Bin("ctr", 1)), Operation.get("ctr"));
	 * }</pre>
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	when event loop registration fails
	 * @see #operate(WritePolicy, Key, Operation...)
	 * @see RecordListener
	 */
	public final void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		OperateArgs args = new OperateArgs(policy, mergedWritePolicyDefault, mergedOperatePolicyReadDefault, operations);
		policy = args.writePolicy;

		if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (args.hasWrite) {
			AsyncOperateWrite command = new AsyncOperateWrite(cluster, listener, key, args);
			AsyncTxnMonitor.execute(eventLoop, cluster, args.writePolicy, command);
		}
		else {
			if (policy.txn != null) {
				policy.txn.prepareRead(key.namespace);
			}

			AsyncOperateRead command = new AsyncOperateRead(cluster, listener, key, args);
			eventLoop.execute(cluster, command);
		}
	}

	//-------------------------------------------------------
	// Batch Read/Write Operations
	//-------------------------------------------------------

	/**
	 * Read/write multiple records in one batch call; each item can be a different namespace/set/ops.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * java.util.List&lt;BatchRecord&gt; records = java.util.Arrays.asList(
	 *   new BatchRead(new Key("ns", "set", "k1")),
	 *   new BatchWrite(new Key("ns", "set", "k2"), new Bin("x", 1)));
	 * boolean ok = client.operate(null, records);
	 * client.close();
	 * }</pre>
	 * <p>
	 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
	 * {@link BatchUDF}. Requires server version 6.0+.
	 *
	 * @param policy	batch configuration parameters, pass in null for defaults
	 * @param records	list of batch record operations
	 * @return			true if all batch sub-commands succeeded
	 * @throws AerospikeException	when command fails
	 * @see BatchRead
	 * @see BatchWrite
	 */
	public final boolean operate(BatchPolicy policy, List<BatchRecord> records)
		throws AerospikeException {
		if (records.size() == 0) {
			return true;
		}

		if (policy == null) {
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKeys(cluster, policy, records);
		}

		BatchStatus status = new BatchStatus(true);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, records, status);
		IBatchCommand[] commands = new IBatchCommand[bns.size()];

		BatchPolicy origBatchPolicy = new BatchPolicy(policy);
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				BatchRecord record = records.get(i);
				policy = origBatchPolicy;

				switch (record.getType()) {
					case BATCH_READ: {
						BatchRead br = (BatchRead)record;
						commands[count++] = new BatchSingle.ReadRecord(cluster, policy, br, status, bn.node);
						break;
					}

					case BATCH_WRITE: {
						BatchWrite bw = (BatchWrite)record;
						BatchAttr attr = new BatchAttr();
						BatchWritePolicy bwp;
						if (bw.policy == null) {
							bwp = mergedBatchWritePolicyDefault;
						} else if (configProvider != null) {
							bwp = new BatchWritePolicy(bw.policy, configProvider);
							policy.graftBatchWriteConfig(configProvider);
						} else {
							bwp = bw.policy;
						}
						attr.setWrite(bwp);
						attr.adjustWrite(bw.ops);
						attr.setOpSize(bw.ops);
						commands[count++] = new BatchSingle.OperateBatchRecord(
							cluster, policy, bw.ops, attr, record, status, bn.node);
						break;
					}

					case BATCH_UDF: {
						BatchUDF bu = (BatchUDF)record;
						BatchAttr attr = new BatchAttr();
						BatchUDFPolicy bup;
						if (bu.policy == null) {
							bup = this.mergedBatchUDFPolicyDefault;
						} else if (configProvider != null) {
							bup = new BatchUDFPolicy(bu.policy, configProvider);
						} else {
							bup = bu.policy;
						}
						attr.setUDF(bup);
						commands[count++] = new BatchSingle.UDF(
							cluster, policy, bu.packageName, bu.functionName, bu.functionArgs, attr, record, status,
							bn.node);
						break;
					}

					case BATCH_DELETE: {
						BatchDelete bd = (BatchDelete)record;
						BatchAttr attr = new BatchAttr();
						BatchDeletePolicy bdp;
						if (bd.policy == null) {
							bdp = this.mergedBatchDeletePolicyDefault;
						} else if (configProvider != null) {
							bdp = new BatchDeletePolicy(bd.policy, configProvider);
						} else {
							bdp = bd.policy;
						}
						attr.setDelete(bdp);
						commands[count++] = new BatchSingle.Delete(cluster, policy, attr, record, status, bn.node);
						break;
					}

					default: {
						throw new AerospikeException("Invalid batch type: " + record.getType());
					}
				}
			}
			else {
				commands[count++] = new Batch.OperateListCommand(cluster, bn, policy, records, status, configProvider);
			}
		}
		BatchExecutor.execute(cluster, policy, commands, status);
		return status.getStatus();
	}

	/**
	 * Asynchronously read/write multiple records in one batch; results in one callback.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * java.util.List&lt;BatchRecord&gt; records = java.util.Arrays.asList(new BatchRead(new Key("ns", "set", "k1")));
	 * client.operate(loop, new BatchOperateListListener() { ... }, null, records);
	 * }</pre>
	 * <p>
	 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
	 * {@link BatchUDF}. Requires server version 6.0+.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of batch record operations
	 * @throws AerospikeException	when event loop registration fails
	 * @see #operate(BatchPolicy, List)
	 * @see BatchOperateListListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		AsyncBatchExecutor.OperateList executor = new AsyncBatchExecutor.OperateList(
			eventLoop, cluster, listener, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, records, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];

		BatchPolicy origBatchPolicy = new BatchPolicy(policy);
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				BatchRecord record = records.get(i);
				policy = origBatchPolicy;

				switch (record.getType()) {
					case BATCH_READ: {
						BatchRead br = (BatchRead)record;
						commands[count++] = new AsyncBatchSingle.Read(executor, cluster, policy, br, bn.node);
						break;
					}

					case BATCH_WRITE: {
						BatchWrite bw = (BatchWrite)record;
						BatchAttr attr = new BatchAttr();
						BatchWritePolicy bwp;
						if (bw.policy == null) {
							bwp = mergedBatchWritePolicyDefault;
						} else if (configProvider != null) {
							bwp = new BatchWritePolicy(bw.policy, configProvider);
							policy.graftBatchWriteConfig(configProvider);
						} else {
							bwp = bw.policy;
						}
						attr.setWrite(bwp);
						attr.adjustWrite(bw.ops);
						attr.setOpSize(bw.ops);
						commands[count++] = new AsyncBatchSingle.Write(executor, cluster, policy, attr, bw, bn.node);
						break;
					}

					case BATCH_UDF: {
						BatchUDF bu = (BatchUDF)record;
						BatchAttr attr = new BatchAttr();
						BatchUDFPolicy bup;
						if (bu.policy == null) {
							bup = this.mergedBatchUDFPolicyDefault;
						} else if (configProvider != null) {
							bup = new BatchUDFPolicy(bu.policy, configProvider);
						} else {
							bup = bu.policy;
						}
						attr.setUDF(bup);
						commands[count++] = new AsyncBatchSingle.UDF(executor, cluster, policy, attr, bu, bn.node);
						break;
					}

					case BATCH_DELETE: {
						BatchDelete bd = (BatchDelete)record;
						BatchAttr attr = new BatchAttr();
						BatchDeletePolicy bdp;
						if (bd.policy == null) {
							bdp = this.mergedBatchDeletePolicyDefault;
						} else if (configProvider != null) {
							bdp = new BatchDeletePolicy(bd.policy, configProvider);
						} else {
							bdp = bd.policy;
						}
						attr.setDelete(bdp);
						commands[count++] = new AsyncBatchSingle.Delete(executor, cluster, policy, attr, record,
							bn.node);
						break;
					}

					default: {
						throw new AerospikeException("Invalid batch type: " + record.getType());
					}
				}
			}
			else {
				commands[count++] = new AsyncBatch.OperateListCommand(executor, bn, policy, records, configProvider);
			}
		}
		AsyncTxnMonitor.executeBatch(policy, executor, commands, records);
	}

	/**
	 * Asynchronously read/write multiple records in one batch; each result via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * java.util.List&lt;BatchRecord&gt; records = java.util.Arrays.asList(new BatchRead(new Key("ns", "set", "k1")));
	 * client.operate(loop, new BatchRecordSequenceListener() { ... }, null, records);
	 * }</pre>
	 * <p>
	 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
	 * {@link BatchUDF}. Requires server version 6.0+.
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param policy		batch configuration parameters, pass in null for defaults
	 * @param records		list of batch record operations
	 * @throws AerospikeException	when event loop registration fails
	 * @see #operate(BatchPolicy, List)
	 * @see BatchRecordSequenceListener
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
			policy = mergedBatchPolicyDefault;
		} else if (configProvider != null) {
			policy = new BatchPolicy(policy, configProvider);
		}

		AsyncBatchExecutor.OperateSequence executor = new AsyncBatchExecutor.OperateSequence(
			eventLoop, cluster, listener);
		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, records, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];

		BatchPolicy origBatchPolicy = new BatchPolicy(policy);
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				BatchRecord record = records.get(i);
				policy = origBatchPolicy;

				switch (record.getType()) {
					case BATCH_READ: {
						BatchRead br = (BatchRead)record;
						commands[count++] = new AsyncBatchSingle.ReadSequence(
							executor, cluster, policy, br, bn.node, listener, i);
						break;
					}

					case BATCH_WRITE: {
						BatchWrite bw = (BatchWrite)record;
						BatchAttr attr = new BatchAttr();
						BatchWritePolicy bwp;
						if (bw.policy == null) {
							bwp = mergedBatchWritePolicyDefault;
						} else if (configProvider != null) {
							bwp = new BatchWritePolicy(bw.policy, configProvider);
							policy.graftBatchWriteConfig(configProvider);
						} else {
							bwp = bw.policy;
						}
						attr.setWrite(bwp);
						attr.adjustWrite(bw.ops);
						attr.setOpSize(bw.ops);
						commands[count++] = new AsyncBatchSingle.WriteSequence(
							executor, cluster, policy, attr, bw, bn.node, listener, i);
						break;
					}

					case BATCH_UDF: {
						BatchUDF bu = (BatchUDF)record;
						BatchAttr attr = new BatchAttr();
						BatchUDFPolicy bup;
						if (bu.policy == null) {
							bup = this.mergedBatchUDFPolicyDefault;
						} else if (configProvider != null) {
							bup = new BatchUDFPolicy(bu.policy, configProvider);
						} else {
							bup = bu.policy;
						}
						attr.setUDF(bup);
						commands[count++] = new AsyncBatchSingle.UDFSequence(
							executor, cluster, policy, attr, bu, bn.node, listener, i);
						break;
					}

					case BATCH_DELETE: {
						BatchDelete bd = (BatchDelete)record;
						BatchAttr attr = new BatchAttr();
						BatchDeletePolicy bdp;
						if (bd.policy == null) {
							bdp = this.mergedBatchDeletePolicyDefault;
						} else if (configProvider != null) {
							bdp = new BatchDeletePolicy(bd.policy, configProvider);
						} else {
							bdp = bd.policy;
						}
						attr.setDelete(bdp);
						commands[count++] = new AsyncBatchSingle.DeleteSequence(
							executor, cluster, policy, attr, bd, bn.node, listener, i);
						break;
					}

					default: {
						throw new AerospikeException("Invalid batch type: " + record.getType());
					}
				}
			}
			else {
				commands[count++] = new AsyncBatch.OperateSequenceCommand(executor, bn, policy, listener, records,
						configProvider);
			}
		}
		AsyncTxnMonitor.executeBatch(policy, executor, commands, records);
	}

	/**
	 * Perform the same read/write operations on multiple keys; result order matches key order.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * BatchResults results = client.operate(null, null, keys, Operation.get("bin1"));
	 * client.close();
	 * }</pre>
	 * <p>
	 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
	 * {@link ResultCode#KEY_NOT_FOUND_ERROR}. Use {@link Operation#get(String)} per bin; {@link Operation#get()} is not allowed.
	 * Requires server version 6.0+.
	 *
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param writePolicy	write configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			read/write operations to perform
	 * @return				batch results (one per key)
	 * @throws AerospikeException.BatchRecordArray	when some keys fail (contains partial results)
	 * @see #operate(WritePolicy, Key, Operation...)
	 * @see BatchResults
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
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}
		if (writePolicy == null) {
			writePolicy = mergedBatchWritePolicyDefault;
		} else if (configProvider != null) {
			writePolicy = new BatchWritePolicy(writePolicy, configProvider);
		}


		if (batchPolicy.txn != null) {
			TxnMonitor.addKeys(cluster, batchPolicy, keys);
		}

		BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
		if (attr.hasWrite && configProvider != null) {
			batchPolicy.graftBatchWriteConfig(configProvider);
		}

		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], attr.hasWrite);
		}

		try {
			BatchStatus status = new BatchStatus(true);
			List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];
			int count = 0;
			boolean opSizeSet = false;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					if (! opSizeSet) {
						attr.setOpSize(ops);
						opSizeSet = true;
					}

					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.OperateBatchRecord(
						cluster, batchPolicy, ops, attr, records[i], status, bn.node);
				}
				else {
					commands[count++] = new Batch.OperateArrayCommand(
						cluster, bn, batchPolicy, keys, ops, records, attr, status);
				}
			}
			BatchExecutor.execute(cluster, batchPolicy, commands, status);
			return new BatchResults(records, status.getStatus());
		}
		catch (Throwable e) {
			throw new AerospikeException.BatchRecordArray(records, e);
		}
	}

	/**
	 * Asynchronously perform the same read/write operations on multiple keys; results in one callback.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.operate(loop, new BatchRecordArrayListener() { ... }, null, null, keys, Operation.get("bin1"));
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param writePolicy	write configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			read/write operations (use Operation.get(String) per bin)
	 * @throws AerospikeException	when event loop registration fails
	 * @see #operate(BatchPolicy, BatchWritePolicy, Key[], Operation...)
	 * @see BatchRecordArrayListener
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
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}
		if (writePolicy == null) {
			writePolicy = mergedBatchWritePolicyDefault;
		} else if (configProvider != null) {
			writePolicy = new BatchWritePolicy(writePolicy, configProvider);
		}

		BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
		if (attr.hasWrite && configProvider != null) {
			batchPolicy.graftBatchWriteConfig(configProvider);
		}

		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], attr.hasWrite);
		}

		AsyncBatchExecutor.BatchRecordArray executor = new AsyncBatchExecutor.BatchRecordArray(
			eventLoop, cluster, listener, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;
		boolean opSizeSet = false;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				if (!opSizeSet) {
					attr.setOpSize(ops);
					opSizeSet = true;
				}
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.Operate(
					executor, cluster, batchPolicy, attr, records[i], ops, bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.OperateRecordArrayCommand(
					executor, bn, batchPolicy, keys, ops, records, attr);
			}
		}
		AsyncTxnMonitor.executeBatch(batchPolicy, executor, commands, keys);
	}

	/**
	 * Asynchronously perform the same read/write operations on multiple keys; each result via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.operate(loop, new BatchRecordSequenceListener() { ... }, null, null, keys, Operation.get("bin1"));
	 * }</pre>
	 *
	 * @param eventLoop		event loop that will process the command. If NULL, the event
	 * 						loop will be chosen by round-robin.
	 * @param listener		where to send results
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param writePolicy	write configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param ops			read/write operations (use Operation.get(String) per bin)
	 * @throws AerospikeException	when event loop registration fails
	 * @see #operate(BatchPolicy, BatchWritePolicy, Key[], Operation...)
	 * @see BatchRecordSequenceListener
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
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}
		if (writePolicy == null) {
			writePolicy = mergedBatchWritePolicyDefault;
		} else if (configProvider != null) {
			writePolicy = new BatchWritePolicy(writePolicy, configProvider);
		}

		BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
		if (attr.hasWrite && configProvider != null) {
			batchPolicy.graftBatchWriteConfig(configProvider);
		}

		boolean[] sent = new boolean[keys.length];
		AsyncBatchExecutor.BatchRecordSequence executor = new AsyncBatchExecutor.BatchRecordSequence(
			eventLoop, cluster, listener, sent);
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, null, attr.hasWrite, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;
		boolean opSizeSet = false;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				if (!opSizeSet) {
					attr.setOpSize(ops);
					opSizeSet = true;
				}
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.OperateSequence(
					executor, cluster, batchPolicy, keys[i], attr, ops, bn.node, listener, i);
			}
			else {
				commands[count++] = new AsyncBatch.OperateRecordSequenceCommand(
					executor, bn, batchPolicy, keys, ops, sent, listener, attr);
			}
		}
		AsyncTxnMonitor.executeBatch(batchPolicy, executor, commands, keys);
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
	 * @deprecated Use {@link #query(QueryPolicy, Statement, QueryListener)} with a {@link Statement}
	 *             (namespace and set name, no filter) and {@link QueryListener} instead and will be removed eventually.
	 */
	@Deprecated
	public final void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedScanPolicyDefault;
		} else if (configProvider != null) {
			policy = new ScanPolicy(policy, configProvider);
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
	 * @deprecated Use {@link #query(EventLoop, RecordSequenceListener, QueryPolicy, Statement)} with a
	 *             {@link Statement} (namespace and set name, no filter) instead and will be removed eventually.
	 */
	@Deprecated
	public final void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace, String setName, String... binNames)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedScanPolicyDefault;
		} else if (configProvider != null) {
			policy = new ScanPolicy(policy, configProvider);
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
	 * @deprecated Use {@link #query(QueryPolicy, Statement, QueryListener)} or
	 *             {@link #queryPartitions(QueryPolicy, Statement, PartitionFilter)} with a
	 *             {@link Statement} (namespace and set name, no filter) and a partition filter for the node instead.
	 */
	@Deprecated
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
	 * @deprecated Use {@link #query(QueryPolicy, Statement, QueryListener)} or
	 *             {@link #queryPartitions(QueryPolicy, Statement, PartitionFilter)} with a
	 *             {@link Statement} (namespace and set name, no filter) and a partition filter for the node instead and will be removed eventually.
	 */
	@Deprecated
	public final void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedScanPolicyDefault;
		} else if (configProvider != null) {
			policy = new ScanPolicy(policy, configProvider);
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
	 * @deprecated Use {@link #queryPartitions(QueryPolicy, Statement, PartitionFilter)} with a
	 *             {@link Statement} (namespace and setName, no filter) instead and will be removed eventually.
	 */
	@Deprecated
	public final void scanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedScanPolicyDefault;
		} else if (configProvider != null) {
			policy = new ScanPolicy(policy, configProvider);
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
	 * @deprecated Use {@link #queryPartitions(EventLoop, RecordSequenceListener, QueryPolicy, Statement, PartitionFilter)}
	 *             with a {@link Statement} (namespace and setName, no filter) instead and will be removed eventually.
	 */
	@Deprecated
	public final void scanPartitions(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName, String... binNames)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedScanPolicyDefault;
		} else if (configProvider != null) {
			policy = new ScanPolicy(policy, configProvider);
		}

		Node[] nodes = cluster.validateNodes();
		PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
		new AsyncScanPartitionExecutor(eventLoop, cluster, policy, listener, namespace, setName, binNames, tracker);
	}

	//---------------------------------------------------------------
	// User defined functions
	//---------------------------------------------------------------

	/**
	 * Register a UDF package from a file with the server; returns a task to wait for completion.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * RegisterTask task = client.register(null, "myudf.lua", "myudf.lua", Language.LUA);
	 * task.waitTillComplete(1000);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param clientPath			path of client file containing user defined functions, relative to current directory
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory
	 * @param language				language of user defined functions
	 * @return						task to wait for completion
	 * @throws AerospikeException	when register fails
	 * @see RegisterTask
	 * @see #execute(WritePolicy, Key, String, String, Value...)
	 */
	public final RegisterTask register(Policy policy, String clientPath, String serverPath, Language language)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
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
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
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
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
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
	 * Execute a UDF on a single record and return the result.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key key = new Key("ns", "set", "mykey");
	 * Object result = client.execute(null, key, "myudf", "myFunc", Value.get("arg1"));
	 * client.close();
	 * }</pre>
	 * <p>
	 * UDF file location: {@code <server udf dir>/<package name>.lua}
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @return						return value of user defined function
	 * @throws AerospikeException	when command fails
	 * @see #register(Policy, String, String, Language)
	 * @see #execute(EventLoop, ExecuteListener, WritePolicy, Key, String, String, Value...)
	 */
	public final Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... functionArgs)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (policy.txn != null) {
			TxnMonitor.addKey(cluster, policy, key);
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
	 * Asynchronously execute a UDF on a single record.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.execute(loop, executeListener, null, key, "myudf", "myFunc", Value.get("arg1"));
	 * }</pre>
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @throws AerospikeException	when event loop registration fails
	 * @see #execute(WritePolicy, Key, String, String, Value...)
	 * @see ExecuteListener
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
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		AsyncExecute command = new AsyncExecute(cluster, listener, policy, key, packageName, functionName, functionArgs);
		AsyncTxnMonitor.execute(eventLoop, cluster, policy, command);
	}

	/**
	 * Execute a UDF on each of the given keys and return batch results; result order matches key order.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * BatchResults results = client.execute(null, null, keys, "myudf", "myFunc", Value.get("arg1"));
	 * client.close();
	 * }</pre>
	 * <p>
	 * UDF file location: {@code <server udf dir>/<package name>.lua}. Requires server version 6.0+.
	 *
	 * @param batchPolicy	batch configuration parameters, pass in null for defaults
	 * @param udfPolicy		udf configuration parameters, pass in null for defaults
	 * @param keys			array of unique record identifiers
	 * @param packageName	server package name where user defined function resides
	 * @param functionName	user defined function
	 * @param functionArgs	arguments passed in to user defined function
	 * @return				batch results (one per key)
	 * @throws AerospikeException.BatchRecordArray	when some keys fail (contains partial results)
	 * @see #execute(WritePolicy, Key, String, String, Value...)
	 * @see BatchResults
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
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}

		if (udfPolicy == null) {
			udfPolicy = mergedBatchUDFPolicyDefault;
		} else if (configProvider != null) {
			udfPolicy = new BatchUDFPolicy(udfPolicy, configProvider);
		}

		if (batchPolicy.txn != null) {
			TxnMonitor.addKeys(cluster, batchPolicy, keys);
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
			List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];
			int count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.UDF(
						cluster, batchPolicy, packageName, functionName, functionArgs, attr, records[i], status,
						bn.node);
				}
				else {
					commands[count++] = new Batch.UDFCommand(
						cluster, bn, batchPolicy, keys, packageName, functionName, argBytes, records, attr, status);
				}
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
	 * Asynchronously execute a UDF on each of the given keys; results in one callback.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.execute(loop, new BatchRecordArrayListener() { ... }, null, null, keys, "myudf", "myFunc", Value.get("arg1"));
	 * }</pre>
	 * <p>
	 * Requires server version 6.0+.
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
	 * @throws AerospikeException	when event loop registration fails
	 * @see #execute(BatchPolicy, BatchUDFPolicy, Key[], String, String, Value...)
	 * @see BatchRecordArrayListener
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
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}

		if (udfPolicy == null) {
			udfPolicy = mergedBatchUDFPolicyDefault;
		} else if (configProvider != null) {
			udfPolicy = new BatchUDFPolicy(udfPolicy, configProvider);
		}

		byte[] argBytes = Packer.pack(functionArgs);

		BatchAttr attr = new BatchAttr();
		attr.setUDF(udfPolicy);

		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], attr.hasWrite);
		}

		AsyncBatchExecutor.BatchRecordArray executor = new AsyncBatchExecutor.BatchRecordArray(
			eventLoop, cluster, listener, records);
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, attr.hasWrite, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.UDFCommand(
					executor, cluster, batchPolicy, attr, records[i], packageName, functionName, argBytes, bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.UDFArrayCommand(
					executor, bn, batchPolicy, keys, packageName, functionName, argBytes, records, attr);
			}
		}
		AsyncTxnMonitor.executeBatch(batchPolicy, executor, commands, keys);
	}

	/**
	 * Asynchronously execute a UDF on each of the given keys; each result via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Key[] keys = new Key[] { new Key("ns", "set", "k1"), new Key("ns", "set", "k2") };
	 * client.execute(loop, new BatchRecordSequenceListener() { ... }, null, null, keys, "myudf", "myFunc", Value.get("arg1"));
	 * }</pre>
	 * <p>
	 * Requires server version 6.0+.
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
	 * @throws AerospikeException	when event loop registration fails
	 * @see #execute(BatchPolicy, BatchUDFPolicy, Key[], String, String, Value...)
	 * @see BatchRecordSequenceListener
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
			batchPolicy = mergedBatchParentPolicyWriteDefault;
		} else if (configProvider != null) {
			batchPolicy = new BatchPolicy(batchPolicy, configProvider);
		}

		if (udfPolicy == null) {
			udfPolicy = mergedBatchUDFPolicyDefault;
		} else if (configProvider != null) {
			udfPolicy = new BatchUDFPolicy(udfPolicy, configProvider);
		}

		byte[] argBytes = Packer.pack(functionArgs);

		BatchAttr attr = new BatchAttr();
		attr.setUDF(udfPolicy);

		boolean[] sent = new boolean[keys.length];
		AsyncBatchExecutor.BatchRecordSequence executor = new AsyncBatchExecutor.BatchRecordSequence(
			eventLoop, cluster, listener, sent);
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, null, attr.hasWrite, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.UDFSequenceCommand(
					executor, cluster, batchPolicy, keys[i], attr, packageName, functionName, argBytes, bn.node, listener, i);
			}
			else {
				commands[count++] = new AsyncBatch.UDFSequenceCommand(
					executor, bn, batchPolicy, keys, packageName, functionName, argBytes, sent, listener, attr);
			}
		}
		AsyncTxnMonitor.executeBatch(batchPolicy, executor, commands, keys);
	}

	//----------------------------------------------------------
	// Query/Execute
	//----------------------------------------------------------

	/**
	 * Run a UDF in the background on all records matching the statement; returns a task to wait for completion.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * ExecuteTask task = client.execute(null, stmt, "myudf", "myFunc", Value.get("arg1"));
	 * task.waitTillComplete(5000);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param statement				background query definition
	 * @param packageName			server package where user defined function resides
	 * @param functionName			function name
	 * @param functionArgs			arguments to pass to function, if any
	 * @return						task to wait for completion
	 * @throws AerospikeException	when command fails
	 * @see ExecuteTask
	 * @see #execute(WritePolicy, Statement, Operation...)
	 */
	public final ExecuteTask execute(
		WritePolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		statement.setAggregateFunction(packageName, functionName, functionArgs);

		cluster.addCommandCount();

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
	 * Run operations in the background on all records matching the statement; returns a task to wait for completion.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * ExecuteTask task = client.execute(null, stmt, Operation.put(new Bin("flag", 1)));
	 * task.waitTillComplete(5000);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param statement				background query definition
	 * @param operations			operations to perform on selected records
	 * @return						task to wait for completion
	 * @throws AerospikeException	when command fails
	 * @see ExecuteTask
	 * @see #execute(WritePolicy, Statement, String, String, Value...)
	 */
	public final ExecuteTask execute(
		WritePolicy policy,
		Statement statement,
		Operation... operations
	) throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new WritePolicy(policy, configProvider);
		}

		if (operations.length > 0) {
			statement.setOperations(operations);
		}

		cluster.addCommandCount();

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
	 * Execute a query on all nodes and return a record iterator.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * RecordSet rs = client.query(null, stmt);
	 * try { while (rs.next()) { Record r = rs.getRecord(); } } finally { rs.close(); }
	 * client.close();
	 * }</pre>
	 * <p>
	 * For paginated use without consuming the full RecordSet, prefer {@link #query(QueryPolicy, Statement, QueryListener)}.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @return						record iterator
	 * @throws AerospikeException	when query fails
	 * @see #query(EventLoop, RecordSequenceListener, QueryPolicy, Statement)
	 * @see Statement
	 * @see RecordSet
	 */
	public final RecordSet query(QueryPolicy policy, Statement statement)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
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
	 * Asynchronously execute a query on all nodes; each record via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Statement stmt = new Statement("ns", "set");
	 * client.query(loop, new RecordSequenceListener() { ... }, null, stmt);
	 * }</pre>
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @throws AerospikeException	when event loop registration fails
	 * @see #query(QueryPolicy, Statement)
	 * @see RecordSequenceListener
	 */
	public final void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement)
		throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
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
	 * Execute a query on all nodes and deliver records via the listener; blocks until complete.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * client.query(null, stmt, new QueryListener() { ... });
	 * client.close();
	 * }</pre>
	 * <p>
	 * If maxConcurrentNodes is not 1, the listener may be called from multiple threads.
	 * Requires server version 6.0+ for secondary index queries.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param listener				where to send results
	 * @throws AerospikeException	when query fails
	 * @see #query(QueryPolicy, Statement)
	 * @see QueryListener
	 */
	public final void query(
		QueryPolicy policy,
		Statement statement,
		QueryListener listener
	) throws AerospikeException {
		if (policy == null) {
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
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
	 * Execute a query for specified partitions and deliver records via the listener; blocks until complete.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * PartitionFilter filter = PartitionFilter.all();
	 * client.query(null, stmt, filter, new QueryListener() { ... });
	 * client.close();
	 * }</pre>
	 * <p>
	 * Completion status is stored in partitionFilter when the query ends; use it to resume later.
	 * If maxConcurrentNodes is not 1, the listener may be called from multiple threads.
	 * Requires server version 6.0+ for secondary index queries.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		partition filter (e.g. {@link com.aerospike.client.query.PartitionFilter#all()}); updated on completion for resume
	 * @param listener				where to send results
	 * @throws AerospikeException	when query fails
	 * @see #queryPartitions(QueryPolicy, Statement, PartitionFilter)
	 * @see #queryPartitions(EventLoop, RecordSequenceListener, QueryPolicy, Statement, PartitionFilter)
	 * @see QueryListener
	 * @see PartitionFilter
	 */
	public final void query(
		QueryPolicy policy,
		Statement statement,
		PartitionFilter partitionFilter,
		QueryListener listener
	) throws AerospikeException {
		if (policy == null) {
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
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
	 * Execute a query on a single node and return a record iterator.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * Node node = client.getNodes()[0];
	 * RecordSet rs = client.queryNode(null, stmt, node);
	 * try { while (rs.next()) { Record r = rs.getRecord(); } } finally { rs.close(); }
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param node					server node to execute query
	 * @return						record iterator
	 * @throws AerospikeException	when query fails
	 * @see #query(QueryPolicy, Statement)
	 * @see #queryPartitions(QueryPolicy, Statement, PartitionFilter)
	 * @see RecordSet
	 * @see Node
	 */
	public final RecordSet queryNode(QueryPolicy policy, Statement statement, Node node)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
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
	 * Execute a query for specified partitions and return a record iterator.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * PartitionFilter filter = PartitionFilter.all();
	 * RecordSet rs = client.queryPartitions(null, stmt, filter);
	 * try { while (rs.next()) { Record r = rs.getRecord(); } } finally { rs.close(); }
	 * client.close();
	 * }</pre>
	 * <p>
	 * Requires server version 6.0+ for secondary index queries.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		filter on a subset of data partitions (e.g. PartitionFilter.all())
	 * @return						record iterator
	 * @throws AerospikeException	when query fails
	 * @see #query(QueryPolicy, Statement, PartitionFilter, QueryListener)
	 * @see #queryPartitions(EventLoop, RecordSequenceListener, QueryPolicy, Statement, PartitionFilter)
	 * @see PartitionFilter
	 * @see RecordSet
	 */
	public final RecordSet queryPartitions(
		QueryPolicy policy,
		Statement statement,
		PartitionFilter partitionFilter
	) throws AerospikeException {
		if (policy == null) {
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
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
	 * Asynchronously execute a query for specified partitions; each record via onRecord().
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Statement stmt = new Statement("ns", "set");
	 * PartitionFilter filter = PartitionFilter.all();
	 * client.queryPartitions(loop, new RecordSequenceListener() { ... }, null, stmt, filter);
	 * }</pre>
	 * <p>
	 * Requires server version 6.0+ for secondary index queries.
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param partitionFilter		filter on a subset of data partitions (e.g. PartitionFilter.all())
	 * @throws AerospikeException	when event loop registration fails or query fails
	 * @see #queryPartitions(QueryPolicy, Statement, PartitionFilter)
	 * @see #query(QueryPolicy, Statement, PartitionFilter, QueryListener)
	 * @see RecordSequenceListener
	 * @see PartitionFilter
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
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
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
	 * Execute a query with an aggregation UDF and return a result iterator; UDF is specified by package/function.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * ResultSet rs = client.queryAggregate(null, stmt, "myudf", "sum_single_bin", Value.get("bin1"));
	 * try { while (rs.next()) { Object v = rs.getObject(); } } finally { rs.close(); }
	 * client.close();
	 * }</pre>
	 * <p>
	 * Aggregation runs on server and client (final reduce); UDF must exist on both. UDF file: {@code <udf dir>/<package name>.lua}
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition
	 * @param packageName			server package where aggregation UDF resides
	 * @param functionName			aggregation function name
	 * @param functionArgs			arguments to pass to function, if any
	 * @return						result iterator
	 * @throws AerospikeException	when query fails
	 * @see #queryAggregate(QueryPolicy, Statement)
	 * @see ResultSet
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
	 * Execute a query with the statement's aggregation function (set via setAggregateFunction()) and return result iterator.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * stmt.setAggregateFunction("myudf", "sum_single_bin", Value.get("bin1"));
	 * ResultSet rs = client.queryAggregate(null, stmt);
	 * try { while (rs.next()) { Object v = rs.getObject(); } } finally { rs.close(); }
	 * client.close();
	 * }</pre>
	 * <p>
	 * Aggregation runs on server and client (final reduce); UDF must exist on both.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition (aggregation set via setAggregateFunction())
	 * @return						result iterator
	 * @throws AerospikeException	when query fails
	 * @see #queryAggregate(QueryPolicy, Statement, String, String, Value...)
	 * @see #queryAggregateNode(QueryPolicy, Statement, Node)
	 * @see ResultSet
	 */
	public final ResultSet queryAggregate(QueryPolicy policy, Statement statement)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
		}

		Node[] nodes = cluster.validateNodes();
		QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, nodes);
		return executor.getResultSet();
	}

	/**
	 * Execute a query with aggregation on a single node; use statement's setAggregateFunction() first.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Statement stmt = new Statement("ns", "set");
	 * stmt.setAggregateFunction("myudf", "sum_single_bin", Value.get("bin1"));
	 * Node node = client.getNodes()[0];
	 * ResultSet rs = client.queryAggregateNode(null, stmt, node);
	 * try { while (rs.next()) { Object v = rs.getObject(); } } finally { rs.close(); }
	 * client.close();
	 * }</pre>
	 * <p>
	 * Aggregation runs on server and client (final reduce); UDF must exist on both.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				query definition (aggregation set via setAggregateFunction())
	 * @param node					server node to execute query
	 * @return						result iterator
	 * @throws AerospikeException	when query fails
	 * @see #queryAggregate(QueryPolicy, Statement)
	 * @see ResultSet
	 * @see Node
	 */
	public final ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node)
		throws AerospikeException {
		if (policy == null) {
			policy = mergedQueryPolicyDefault;
		} else if (configProvider != null) {
			policy = new QueryPolicy(policy, configProvider);
		}

		QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, new Node[] {node});
		return executor.getResultSet();
	}

	//--------------------------------------------------------
	// Secondary Index functions
	//--------------------------------------------------------

	/**
	 * Create a scalar secondary index; returns a task to wait for completion.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * IndexTask task = client.createIndex(null, "ns", "set", "idx_bin1", "bin1", IndexType.STRING);
	 * task.waitTillComplete(1000);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @return						task to wait for completion
	 * @throws AerospikeException	when index create fails
	 * @see IndexTask
	 * @see #dropIndex(Policy, String, String, String)
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
	 * Create a complex (CDT) secondary index on a collection bin; returns a task to wait for completion.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * IndexTask task = client.createIndex(null, "ns", "set", "idx_list", "bin1", IndexType.STRING, IndexCollectionType.LIST);
	 * task.waitTillComplete(1000);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @param ctx					optional context to index on elements within a CDT
	 * @return						task to wait for completion
	 * @throws AerospikeException	when index create fails
	 * @see #createIndex(Policy, String, String, String, String, IndexType)
	 * @see IndexTask
	 * @see IndexCollectionType
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
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}

		Node node = this.cluster.getRandomNode();
		String command = buildCreateIndexInfoCommand(node, namespace, setName, indexName, binName, indexType, indexCollectionType, ctx, null);
		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(policy, node,  command);

		if (response.equalsIgnoreCase("OK")) {
			// Return task that could optionally be polled for completion.
			return new IndexTask(cluster, policy, namespace, indexName, true);
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "Create index failed: " + response);
	}

	/**
	 * Asynchronously create a complex (CDT) secondary index on a collection bin.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.createIndex(loop, new IndexListener() { ... }, null, "ns", "set", "idx_list", "bin1", IndexType.STRING, IndexCollectionType.LIST);
	 * }</pre>
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
	 * @throws AerospikeException	when index create fails
	 * @see #createIndex(Policy, String, String, String, String, IndexType, IndexCollectionType, CTX...)
	 * @see IndexListener
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
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}
		Node node = this.cluster.getRandomNode();
		String command = buildCreateIndexInfoCommand(node, namespace, setName, indexName, binName, indexType, indexCollectionType, ctx, null);
		sendIndexInfoCommand(eventLoop, listener, policy, node, namespace, indexName, command, true);
	}

	/**
	 * Create an expression-based secondary index; returns a task to wait for completion.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Expression exp = com.aerospike.client.exp.Exp.build(com.aerospike.client.exp.Exp.eq(com.aerospike.client.exp.Exp.stringBin("bin1"), com.aerospike.client.exp.Exp.val(1)));
	 * IndexTask task = client.createIndex(null, "ns", "set", "idx_exp", IndexType.STRING, IndexCollectionType.DEFAULT, exp);
	 * task.waitTillComplete(1000);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @param exp					expression on which to build the index
	 * @return						task to wait for completion
	 * @throws AerospikeException	when index create fails
	 * @see #createIndex(Policy, String, String, String, String, IndexType)
	 * @see Expression
	 * @see IndexTask
	 */
	public final IndexTask createIndex(
		Policy policy,
		String namespace,
		String setName,
		String indexName,
		IndexType indexType,
		IndexCollectionType indexCollectionType,
		Expression exp
	) throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}
		Node node = this.cluster.getRandomNode();
		String command = buildCreateIndexInfoCommand(node, namespace, setName, indexName, null, indexType, indexCollectionType, null, exp);

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(policy, node, command);

		if (response.equalsIgnoreCase("OK")) {
			// Return task that could optionally be polled for completion.
			return new IndexTask(cluster, policy, namespace, indexName, true);
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "Create index failed: " + response);
	}

	/**
	 * Asynchronously create an expression-based secondary index.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * Expression exp = com.aerospike.client.exp.Exp.build(com.aerospike.client.exp.Exp.eq(com.aerospike.client.exp.Exp.stringBin("bin1"), com.aerospike.client.exp.Exp.val(1)));
	 * client.createIndex(loop, new IndexListener() { ... }, null, "ns", "set", "idx_exp", IndexType.STRING, IndexCollectionType.DEFAULT, exp);
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @param exp					expression on which to build the index
	 * @throws AerospikeException	when index create fails
	 * @see #createIndex(Policy, String, String, String, IndexType, IndexCollectionType, Expression)
	 * @see IndexListener
	 */
	public final void createIndex(
		EventLoop eventLoop,
		IndexListener listener,
		Policy policy,
		String namespace,
		String setName,
		String indexName,
		IndexType indexType,
		IndexCollectionType indexCollectionType,
		Expression exp
	) throws AerospikeException {
		if (eventLoop == null) {
			eventLoop = cluster.eventLoops.next();
		}

		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}
		Node node = this.cluster.getRandomNode();
		String command = buildCreateIndexInfoCommand(node, namespace, setName, indexName, null, indexType, indexCollectionType, null, exp);
		sendIndexInfoCommand(eventLoop, listener, policy, node, namespace, indexName, command, true);
	}

	/**
	 * Delete a secondary index; returns a task to wait for completion.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * IndexTask task = client.dropIndex(null, "ns", "set", "idx_bin1");
	 * task.waitTillComplete(1000);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @return						task to wait for completion
	 * @throws AerospikeException	when index drop fails
	 * @see #createIndex(Policy, String, String, String, String, IndexType)
	 * @see IndexTask
	 */
	public final IndexTask dropIndex(
		Policy policy,
		String namespace,
		String setName,
		String indexName
	) throws AerospikeException {
		if (policy == null) {
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}
 		Node node = this.cluster.getRandomNode();
		String command = buildDropIndexInfoCommand(node, namespace, setName, indexName);

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(policy, node, command);

		if (response.equalsIgnoreCase("OK")) {
			return new IndexTask(cluster, policy, namespace, indexName, false);
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "Drop index failed: " + response);
	}

	/**
	 * Asynchronously delete a secondary index.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.dropIndex(loop, new IndexListener() { ... }, null, "ns", "set", "idx_bin1");
	 * }</pre>
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @throws AerospikeException	when index drop fails
	 * @see #dropIndex(Policy, String, String, String)
	 * @see IndexListener
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
			policy = mergedWritePolicyDefault;
		} else if (configProvider != null) {
			policy = new Policy(policy, configProvider);
		}
		Node node = this.cluster.getRandomNode();
		String command = buildDropIndexInfoCommand(node, namespace, setName, indexName);
		sendIndexInfoCommand(eventLoop, listener, policy, node, namespace, indexName, command, false);
	}

	//-----------------------------------------------------------------
	// Async Info functions (sync info functions located in Info class)
	//-----------------------------------------------------------------

	/**
	 * Asynchronously send info command(s) to a node; results delivered to the listener.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * EventLoops eventLoops = new NioEventLoops(4);
	 * ClientPolicy clientPolicy = new ClientPolicy();
	 * clientPolicy.eventLoops = eventLoops;
	 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
	 * EventLoop loop = eventLoops.next();
	 * client.info(loop, new InfoListener() { ... }, null, null, "build", "statistics");
	 * }</pre>
	 * <p>
	 * Info is a name/value protocol for node configuration and status. Supported commands:
	 * <a href="https://www.aerospike.com/docs/reference/info/index.html">aerospike.com/docs/reference/info</a>
	 *
	 * @param eventLoop				event loop that will process the command. If NULL, the event
	 * 								loop will be chosen by round-robin.
	 * @param listener				where to send results
	 * @param policy				info configuration parameters, pass in null for defaults
	 * @param node					server node to execute command, pass in null for random node
	 * @param commands				list of info commands (e.g. "build", "statistics")
	 * @throws AerospikeException	when info commands fail
	 * @see Info
	 * @see InfoListener
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
	 * Set or remove XDR filter for a datacenter and namespace; null filter removes the filter.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * // Filter: only replicate records where bin1 equals 1 (build expression via Exp, same as in filter policies)
	 * Expression filter = com.aerospike.client.exp.Exp.build(
	 *     com.aerospike.client.exp.Exp.eq(com.aerospike.client.exp.Exp.intBin("bin1"), com.aerospike.client.exp.Exp.val(1)));
	 * client.setXDRFilter(null, "DC1", "ns", filter);
	 * // Remove filter for the datacenter/namespace:
	 * client.setXDRFilter(null, "DC1", "ns", null);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				info configuration parameters, pass in null for defaults
	 * @param datacenter			XDR datacenter name
	 * @param namespace				namespace - equivalent to database name
	 * @param filter				expression filter, or null to remove
	 * @throws AerospikeException	when command fails
	 * @see Expression
	 * @see com.aerospike.client.exp.Exp
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
	 * Create a user with password and roles; password is hashed (bcrypt) before sending.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.createUser(null, "newuser", "password", java.util.Arrays.asList("read-write"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param password				user password in clear-text format
	 * @param roles					list of role names; see {@link com.aerospike.client.admin.Role}
	 * @throws AerospikeException	when command fails
	 * @see #dropUser(AdminPolicy, String)
	 * @see #queryUser(AdminPolicy, String)
	 */
	public final void createUser(AdminPolicy policy, String user, String password, List<String> roles)
		throws AerospikeException {
		String hash = AdminCommand.hashPassword(password);
		AdminCommand command = new AdminCommand();
		command.createUser(cluster, policy, user, hash, roles);
	}

	/**
	 * Create a PKI user with roles (TLS/certificate auth). Server 8.1+ only.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.createPkiUser(null, "pkiuser", java.util.Arrays.asList("read-write"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param roles					list of role names; see {@link com.aerospike.client.admin.Role}
	 * @throws AerospikeException	when command fails
	 * @see #createUser(AdminPolicy, String, String, List)
	 * @see #dropUser(AdminPolicy, String)
	 */
	public final void createPkiUser(AdminPolicy policy, String user, List<String> roles)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.createPkiUser(cluster, policy, user, roles);
	}

	/**
	 * Remove a user from the cluster.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.dropUser(null, "olduser");
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @throws AerospikeException	when command fails
	 * @see #createUser(AdminPolicy, String, String, List)
	 * @see #queryUser(AdminPolicy, String)
	 */
	public final void dropUser(AdminPolicy policy, String user)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.dropUser(cluster, policy, user);
	}

	/**
	 * Change a user's password (caller or another user if admin).
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.changePassword(null, "myuser", "newpassword");
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param password				new password in clear-text format
	 * @throws AerospikeException	when command fails
	 * @see #createUser(AdminPolicy, String, String, List)
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
	 * Add roles to a user's list of roles.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.grantRoles(null, "myuser", java.util.Arrays.asList("read-write", "user-admin"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param roles					role names; see {@link com.aerospike.client.admin.Role}
	 * @throws AerospikeException	when command fails
	 * @see #revokeRoles(AdminPolicy, String, List)
	 * @see #queryUser(AdminPolicy, String)
	 */
	public final void grantRoles(AdminPolicy policy, String user, List<String> roles)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.grantRoles(cluster, policy, user, roles);
	}

	/**
	 * Remove roles from a user's list of roles.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.revokeRoles(null, "myuser", java.util.Arrays.asList("user-admin"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param roles					role names to remove; see {@link com.aerospike.client.admin.Role}
	 * @throws AerospikeException	when command fails
	 * @see #grantRoles(AdminPolicy, String, List)
	 * @see #queryUser(AdminPolicy, String)
	 */
	public final void revokeRoles(AdminPolicy policy, String user, List<String> roles)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.revokeRoles(cluster, policy, user, roles);
	}

	/**
	 * Create a user-defined role with the given privileges.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Privilege p = new Privilege();
	 * p.code = PrivilegeCode.READ_WRITE;
	 * p.namespace = "ns";
	 * client.createRole(null, "myrole", java.util.Collections.singletonList(p));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges assigned to the role
	 * @throws AerospikeException	when command fails
	 * @see #dropRole(AdminPolicy, String)
	 * @see #queryRole(AdminPolicy, String)
	 * @see Privilege
	 */
	public final void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.createRole(cluster, policy, roleName, privileges);
	}

	/**
	 * Create a user-defined role with optional privileges and IP whitelist.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Privilege p = new Privilege();
	 * p.code = PrivilegeCode.READ_WRITE;
	 * p.namespace = "ns";
	 * client.createRole(null, "myrole", java.util.Collections.singletonList(p), java.util.Arrays.asList("10.1.2.0/24"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			optional list of privileges assigned to role
	 * @param whitelist				optional list of allowable IP addresses (e.g. 10.1.2.0/24)
	 * @throws AerospikeException	when command fails
	 * @see #createRole(AdminPolicy, String, List)
	 * @see #setWhitelist(AdminPolicy, String, List)
	 */
	public final void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.createRole(cluster, policy, roleName, privileges, whitelist, 0, 0);
	}

	/**
	 * Create a user-defined role with optional privileges, whitelist, and read/write quotas.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Privilege p = new Privilege();
	 * p.code = PrivilegeCode.READ_WRITE;
	 * p.namespace = "ns";
	 * client.createRole(null, "myrole", java.util.Collections.singletonList(p), java.util.Arrays.asList("10.1.2.0/24"), 1000, 500);
	 * client.close();
	 * }</pre>
	 * <p>
	 * Quotas require server "enable-quotas" to be true.
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			optional list of privileges assigned to role
	 * @param whitelist				optional list of allowable IP addresses (e.g. 10.1.2.0/24)
	 * @param readQuota				maximum reads per second, or zero for no limit
	 * @param writeQuota			maximum writes per second, or zero for no limit
	 * @throws AerospikeException	when command fails
	 * @see #createRole(AdminPolicy, String, List, List)
	 * @see #setQuotas(AdminPolicy, String, int, int)
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
	 * Drop a user-defined role.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.dropRole(null, "myrole");
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @throws AerospikeException	when command fails
	 * @see #createRole(AdminPolicy, String, List)
	 * @see #queryRole(AdminPolicy, String)
	 */
	public final void dropRole(AdminPolicy policy, String roleName)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.dropRole(cluster, policy, roleName);
	}

	/**
	 * Grant privileges to a user-defined role.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Privilege p = new Privilege();
	 * p.code = PrivilegeCode.DATA_ADMIN;
	 * p.namespace = "ns";
	 * client.grantPrivileges(null, "myrole", java.util.Collections.singletonList(p));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges to add to the role
	 * @throws AerospikeException	when command fails
	 * @see #revokePrivileges(AdminPolicy, String, List)
	 * @see #queryRole(AdminPolicy, String)
	 * @see Privilege
	 */
	public final void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.grantPrivileges(cluster, policy, roleName, privileges);
	}

	/**
	 * Revoke privileges from a user-defined role.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Privilege p = new Privilege();
	 * p.code = PrivilegeCode.DATA_ADMIN;
	 * p.namespace = "ns";
	 * client.revokePrivileges(null, "myrole", java.util.Collections.singletonList(p));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges to remove from the role
	 * @throws AerospikeException	when command fails
	 * @see #grantPrivileges(AdminPolicy, String, List)
	 * @see #queryRole(AdminPolicy, String)
	 * @see Privilege
	 */
	public final void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.revokePrivileges(cluster, policy, roleName, privileges);
	}

	/**
	 * Set or remove IP whitelist for a role; null or empty removes the existing whitelist.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.setWhitelist(null, "myrole", java.util.Arrays.asList("10.1.2.0/24", "192.168.1.0/24"));
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param whitelist				list of allowable IP addresses (e.g. 10.1.2.0/24), or null to remove
	 * @throws AerospikeException	when command fails
	 * @see #createRole(AdminPolicy, String, List, List)
	 * @see #queryRole(AdminPolicy, String)
	 */
	public final void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.setWhitelist(cluster, policy, roleName, whitelist);
	}

	/**
	 * Set read/write quotas for a role; zero removes the limit. Requires server "enable-quotas".
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * client.setQuotas(null, "myrole", 1000, 500);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param readQuota				maximum reads per second, or zero for no limit
	 * @param writeQuota			maximum writes per second, or zero for no limit
	 * @throws AerospikeException	when command fails
	 * @see #createRole(AdminPolicy, String, List, List, int, int)
	 * @see #queryRole(AdminPolicy, String)
	 */
	public final void setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota)
		throws AerospikeException {
		AdminCommand command = new AdminCommand();
		command.setQuotas(cluster, policy, roleName, readQuota, writeQuota);
	}

	/**
	 * Retrieve a single user and their roles.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * User u = client.queryUser(null, "myuser");
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @return						user with roles, or null if not found
	 * @throws AerospikeException	when command fails
	 * @see #queryUsers(AdminPolicy)
	 * @see #createUser(AdminPolicy, String, String, List)
	 * @see User
	 */
	public final User queryUser(AdminPolicy policy, String user)
		throws AerospikeException {
		AdminCommand.UserCommand command = new AdminCommand.UserCommand(1);
		return command.queryUser(cluster, policy, user);
	}

	/**
	 * Retrieve all users and their roles.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * java.util.List&lt;User&gt; users = client.queryUsers(null);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @return						list of users with roles
	 * @throws AerospikeException	when command fails
	 * @see #queryUser(AdminPolicy, String)
	 * @see User
	 */
	public final List<User> queryUsers(AdminPolicy policy)
		throws AerospikeException {
		AdminCommand.UserCommand command = new AdminCommand.UserCommand(100);
		return command.queryUsers(cluster, policy);
	}

	/**
	 * Retrieve a single role definition (privileges, whitelist, quotas).
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * Role r = client.queryRole(null, "myrole");
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @return						role definition, or null if not found
	 * @throws AerospikeException	when command fails
	 * @see #queryRoles(AdminPolicy)
	 * @see #createRole(AdminPolicy, String, List)
	 * @see Role
	 */
	public final Role queryRole(AdminPolicy policy, String roleName)
		throws AerospikeException {
		AdminCommand.RoleCommand command = new AdminCommand.RoleCommand(1);
		return command.queryRole(cluster, policy, roleName);
	}

	/**
	 * Retrieve all role definitions.
	 * <p>Example usage for this method.</p>
	 * <pre>{@code
	 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
	 * java.util.List&lt;Role&gt; roles = client.queryRoles(null);
	 * client.close();
	 * }</pre>
	 *
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @return						list of roles
	 * @throws AerospikeException	when command fails
	 * @see #queryRole(AdminPolicy, String)
	 * @see Role
	 */
	public final List<Role> queryRoles(AdminPolicy policy)
		throws AerospikeException {
		AdminCommand.RoleCommand command = new AdminCommand.RoleCommand(100);
		return command.queryRoles(cluster, policy);
	}

	//-------------------------------------------------------
	// Internal Methods
	//-------------------------------------------------------

	private String buildCreateIndexInfoCommand(
		Node node,
		String namespace,
		String setName,
		String indexName,
		String binName,
		IndexType indexType,
		IndexCollectionType indexCollectionType,
		CTX[] ctx,
		Expression exp
	) {
		StringBuilder sb = new StringBuilder(1024);
		Version currentServerVersion = node.getServerVersion();
		String createIndexCommand = currentServerVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "sindex-create:namespace=": "sindex-create:ns=";

		sb.append(createIndexCommand);
		sb.append(namespace);

		if (setName != null && setName.length() > 0) {
			sb.append(";set=");
			sb.append(setName);
		}

		sb.append(";indexname=");
		sb.append(indexName);

		if (exp != null && exp.size() > 0) {
			String base64 = exp.getBase64();

			sb.append(";exp=");
			sb.append(base64);

			if (indexCollectionType != IndexCollectionType.DEFAULT) {
				sb.append(";indextype=");
				sb.append(indexCollectionType);
			}

			sb.append(";type=");
			sb.append(indexType);
		} else {
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

			if (node.serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1)) {
				sb.append(";bin=");
				sb.append(binName);
				sb.append(";type=");
				sb.append(indexType);
			} else {
				sb.append(";indexdata=");
				sb.append(binName);
				sb.append(',');
				sb.append(indexType);
			}
		}


		return sb.toString();
	}

	private String buildDropIndexInfoCommand(Node node, String namespace, String setName, String indexName) {
		StringBuilder sb = new StringBuilder(500);
		Version currentServerVersion = node.getServerVersion();
		String deleteIndexCommand = currentServerVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "sindex-delete:namespace=": "sindex-delete:ns=";

		sb.append(deleteIndexCommand);
		sb.append(namespace);

		if (setName != null && setName.length() > 0) {
			sb.append(";set=");
			sb.append(setName);
		}
		sb.append(";indexname=");
		sb.append(indexName);
		return sb.toString();
	}

	private String sendInfoCommand(Policy policy, Node node, String command) {
		Connection conn = node.getConnection(policy.connectTimeout, policy.socketTimeout);
		Info info;

		try {
			info = new Info(node, conn, command);
			node.putConnection(conn);
		}
		catch (Throwable e) {
			node.closeConnection(conn);
			throw e;
		}
		return info.getValue();
	}

	private void sendIndexInfoCommand(
		EventLoop eventLoop,
		IndexListener listener,
		Policy policy,
		Node node,
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
		}, new InfoPolicy(policy), node, command);
	}

	private static int parseIndexErrorCode(String response) {
		Info.Error error = new Info.Error(response);
		return (error.code == 0)? ResultCode.SERVER_ERROR : error.code;
	}
}
