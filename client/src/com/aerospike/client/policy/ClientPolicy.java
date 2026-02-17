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
package com.aerospike.client.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import com.aerospike.client.Log;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.StaticConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicClientConfig;
import com.aerospike.client.configuration.serializers.staticconfig.StaticClientConfig;
import com.aerospike.client.util.Util;

/**
 * Container object for client policy Command.
 */
public class ClientPolicy {
	/**
	 * Optional event loops to use in asynchronous commands.
	 * <p>
	 * Default: null (async methods are disabled)
	 */
	public EventLoops eventLoops;

	/**
	 * User authentication to cluster.  Leave null for clusters running without restricted access.
	 * <p>
	 * Default: null
	 */
	public String user;

	/**
	 * Password authentication to cluster.  The password will be stored by the client and sent to server
	 * in hashed format.  Leave null for clusters running without restricted access.
	 * <p>
	 * Default: null
	 */
	public String password;

	/**
	 * Expected cluster name. If populated and {@link #validateClusterName} is true, the clusterName
	 * must match the cluster-name field in the service section in each server configuration. This
	 * ensures that the specified seed nodes belong to the expected cluster on startup.  If not,
	 * the client will refuse to add the node to the client's view of the cluster.
	 * <p>
	 * Default: null
	 */
	public String clusterName;

	/**
	 * Authentication mode.
	 * <p>
	 * Default: AuthMode.INTERNAL
	 */
	public AuthMode authMode = AuthMode.INTERNAL;

	/**
	 * Cluster tend info call timeout in milliseconds.  The timeout when opening a connection
	 * to the server node for the first time and when polling each node for cluster status.
	 * <p>
	 * Default: 1000
	 */
	public int timeout = 1000;

	/**
	 * Login timeout in milliseconds.  The timeout is used when user authentication is enabled and
	 * a node login is being performed.
	 * <p>
	 * Default: 5000
	 */
	public int loginTimeout = 5000;

	/**
	 * Cluster close timeout in milliseconds. Time to wait for pending async commands to complete
	 * when {@link com.aerospike.client.AerospikeClient#close()} is called. If close() is called
	 * from an event loop thread, the wait is not applied because that would cause deadlock.
	 * The possible values are:
	 * <ul>
	 * <li>-1: Close cluster immediately</li>
	 * <li>0: Wait indefinitely for pending async commands to complete before closing the cluster</li>
	 * <li>> 0: Wait milliseconds for pending async commands to complete before closing the cluster</li>
	 * </ul>
	 * Default: 0
	 */
	public int closeTimeout;

	/**
	 * Minimum number of synchronous connections allowed per server node.  Preallocate min connections
	 * on client node creation.  The client will periodically allocate new connections if count falls
	 * below min connections.
	 * <p>
	 * Server proto-fd-idle-ms and client {@link ClientPolicy#maxSocketIdle} should be set to zero
	 * (no reap) if minConnsPerNode is greater than zero.  Reaping connections can defeat the purpose
	 * of keeping connections in reserve for a future burst of activity.
	 * <p>
	 * <p>
	 * Servers 8.1+ have deprecated proto-fd-idle-ms. When proto-fd-idle-ms is ultimately removed,
     * the server will stop automatically reaping based on socket idle timeouts.
	 * <p>
	 * Default: 0
	 */
	public int minConnsPerNode;

	/**
	 * Maximum number of synchronous connections allowed per server node.  Commands will go
	 * through retry logic and potentially fail with "ResultCode.NO_MORE_CONNECTIONS" if the maximum
	 * number of connections would be exceeded.
	 * <p>
	 * The number of connections used per node depends on concurrent commands in progress
	 * plus sub-commands used for parallel multi-node commands (batch, scan, and query).
	 * One connection will be used for each command.
	 * <p>
	 * Default: 100
	 */
	public int maxConnsPerNode = 100;

	/**
	 * Minimum number of asynchronous connections allowed per server node.  Preallocate min connections
	 * on client node creation.  The client will periodically allocate new connections if count falls
	 * below min connections.
	 * <p>
	 * Server proto-fd-idle-ms and client {@link ClientPolicy#maxSocketIdle} should be set to zero
	 * (no reap) if asyncMinConnsPerNode is greater than zero.  Reaping connections can defeat the purpose
	 * of keeping connections in reserve for a future burst of activity.
	 * <p>
	 * Default: 0
	 */
	public int asyncMinConnsPerNode;

	/**
	 * Maximum number of asynchronous connections allowed per server node.  Commands will go
	 * through retry logic and potentially fail with "ResultCode.NO_MORE_CONNECTIONS" if the maximum
	 * number of connections would be exceeded.
	 * <p>
	 * The number of connections used per node depends on concurrent commands in progress
	 * plus sub-commands used for parallel multi-node commands (batch, scan, and query).
	 * One connection will be used for each command.
	 * <p>
	 * If the value is -1, the value will be set to {@link ClientPolicy#maxConnsPerNode}.
	 * <p>
	 * Default: -1 (Use maxConnsPerNode)
	 */
	public int asyncMaxConnsPerNode = -1;

	/**
	 * Number of synchronous connection pools used for each node.  Machines with 8 cpu cores or
	 * less usually need just one connection pool per node.  Machines with a large number of cpu
	 * cores may have their synchronous performance limited by contention for pooled connections.
	 * Contention for pooled connections can be reduced by creating multiple mini connection pools
	 * per node.
	 * <p>
	 * Default: 1
	 */
	public int connPoolsPerNode = 1;

	/**
	 * Maximum socket idle in seconds.  Socket connection pools will discard sockets
	 * that have been idle longer than the maximum.
	 * <p>
	 * Connection pools are now implemented by a LIFO stack.  Connections at the tail of the
	 * stack will always be the least used.  These connections are checked for maxSocketIdle
	 * once every 30 tend iterations (usually 30 seconds).
	 * <p>
	 * If server's proto-fd-idle-ms is greater than zero, then maxSocketIdle should be
	 * at least a few seconds less than the server's proto-fd-idle-ms, so the client does not
	 * attempt to use a socket that has already been reaped by the server.
	 * <p>
	 * If server's proto-fd-idle-ms is zero (no reap), then maxSocketIdle should also be zero.
	 * Connections retrieved from a pool in commands will not be checked for maxSocketIdle
	 * when maxSocketIdle is zero.  Idle connections will still be trimmed down from peak
	 * connections to min connections (minConnsPerNode and asyncMinConnsPerNode) using a
	 * hard-coded 55 second limit in the cluster tend thread.
	 * <p>
	 * <p>
	 * Servers 8.1+ have deprecated proto-fd-idle-ms. When proto-fd-idle-ms is ultimately removed,
     * the server will stop automatically reaping based on socket idle timeouts.
	 * <p>
	 * Default: 0
	 */
	public int maxSocketIdle = 0;

	/**
	 * Maximum number of errors allowed per node per {@link #errorRateWindow} before backoff
	 * algorithm throws {@link com.aerospike.client.AerospikeException.Backoff} on database
	 * commands to that node. If maxErrorRate is zero, there is no error limit and
	 * the exception will never be thrown.
	 * <p>
	 * The counted error types are any error that causes the connection to close (socket errors
	 * and client timeouts) and {@link com.aerospike.client.ResultCode#DEVICE_OVERLOAD}.
	 * <p>
	 * Default: 100
	 */
	public int maxErrorRate = 100;

	/**
	 * The number of cluster tend iterations that defines the window for {@link #maxErrorRate}.
	 * One tend iteration is defined as {@link #tendInterval} plus the time to tend all nodes.
	 * At the end of the window, the error count is reset to zero and backoff state is removed
	 * on all nodes.
	 * <p>
	 * Default: 1
	 */
	public int errorRateWindow = 1;

	/**
	 * Interval in milliseconds between cluster tends by maintenance thread.
	 * <p>
	 * Default: 1000
	 */
	public int tendInterval = 1000;

	/**
	 * Should cluster instantiation fail if the client fails to connect to a seed or
	 * all the seed's peers.
	 * <p>
	 * If true, throw an exception if all seed connections fail or a seed is valid,
	 * but all peers from that seed are not reachable.
	 * <p>
	 * If false, a partial cluster will be created and the client will automatically connect
	 * to the remaining nodes when they become available.
	 * <p>
	 * Default: true
	 */
	public boolean failIfNotConnected = true;

	/**
	 * When validateClusterName is true and {@link #clusterName} is populated, verify that
	 * clusterName matches the cluster-name field in the service section in each server
	 * configuration. This ensures that the specified seed nodes belong to the expected cluster on
	 * startup. If not, the client will refuse to add the node to the client's view of the cluster.
	 * <p>
	 * Default: true
	 */
	public boolean validateClusterName = true;

	/**
	 * Default read policy that is used when read command's policy is null.
	 */
	public Policy readPolicyDefault = new Policy();

	/**
	 * Default write policy that is used when write command's policy is null.
	 */
	public WritePolicy writePolicyDefault = new WritePolicy();

	/**
	 * Default scan policy that is used when scan command's policy is null.
	 */
	public ScanPolicy scanPolicyDefault = new ScanPolicy();

	/**
	 * Default query policy that is used when query command's policy is null.
	 */
	public QueryPolicy queryPolicyDefault = new QueryPolicy();

	/**
	 * Default parent policy used in batch read commands. Parent policy fields
	 * include socketTimeout, totalTimeout, maxRetries, etc...
	 */
	public BatchPolicy batchPolicyDefault = BatchPolicy.ReadDefault();

	/**
	 * Default parent policy used in batch write commands. Parent policy fields
	 * include socketTimeout, totalTimeout, maxRetries, etc...
	 */
	public BatchPolicy batchParentPolicyWriteDefault = BatchPolicy.WriteDefault();

	/**
	 * Default write policy used in batch operate commands.
	 * Write policy fields include generation, expiration, durableDelete, etc...
	 */
	public BatchWritePolicy batchWritePolicyDefault = new BatchWritePolicy();

	/**
	 * Default delete policy used in batch delete commands.
	 */
	public BatchDeletePolicy batchDeletePolicyDefault = new BatchDeletePolicy();

	/**
	 * Default user defined function policy used in batch UDF excecute commands.
	 */
	public BatchUDFPolicy batchUDFPolicyDefault = new BatchUDFPolicy();

	/**
	 * Default transaction policy when verifying record versions in a batch.
	 */
	public TxnVerifyPolicy txnVerifyPolicyDefault = new TxnVerifyPolicy();

	/**
	 * Default transaction policy when rolling the transaction records forward (commit)
	 * or back (abort) in a batch.
	 */
	public TxnRollPolicy txnRollPolicyDefault = new TxnRollPolicy();

	/**
	 * Default info policy that is used when info command's policy is null.
	 */
	public InfoPolicy infoPolicyDefault = new InfoPolicy();

	/**
	 * TLS secure connection policy for TLS enabled servers.
	 * TLS connections are only supported for AerospikeClient synchronous commands.
	 * <p>
	 * Default: null (Use normal sockets)
	 */
	public TlsPolicy tlsPolicy;

	/**
	 * TCP keep-alive configuration. If assigned, enable TCP keep-alive when using
	 * the native Netty epoll library.
	 * <p>
	 * Default: null (Do not enable TCP keep-alive)
	 */
	public TCPKeepAlive keepAlive;

	/**
	 * An IP translation table is used in cases where different clients use different server
	 * IP addresses.  This may be necessary when using clients from both inside and outside
	 * a local area network.  Default is no translation.
	 * <p>
	 * The key is the IP address returned from friend info requests to other servers.  The
	 * value is the real IP address used to connect to the server.
	 * <p>
	 * Default: null (no IP address translation)
	 */
	public Map<String,String> ipMap;

	/**
	 * This field is ignored and deprecated. The client now supports virtual threads and thread pools
	 * are no longer used. This field only exists to maintain api compatibility when switching between
	 * aerospike-client-jdk21 and aerospike-client-jdk8 packages.
	 */
	@Deprecated
	public ExecutorService threadPool;

	/**
	 * This field is ignored and deprecated. The client now supports virtual threads and thread pools
	 * are no longer used. This field only exists to maintain api compatibility when switching between
	 * aerospike-client-jdk21 and aerospike-client-jdk8 packages.
	 */
	@Deprecated
	public boolean sharedThreadPool;

	/**
	 * Flag to signify if alternate IP address discovery info commands should be used.
	 * <p>
	 * If false, use:<br>
	 * 	IP address: service-clear-std<br>
	 * 	TLS IP address: service-tls-std<br>
	 * 	Peers addresses: peers-clear-std<br>
	 * 	Peers TLS addresses: peers-tls-std
	 * 	<p>
	 * If true, use:<br>
	 * 	IP address: service-clear-alt<br>
	 * 	TLS IP address: service-tls-alt<br>
	 * 	Peers addresses: peers-clear-alt<br>
	 * 	Peers TLS addresses: peers-tls-alt
	 * 	<p>
	 * Default: false (use original "services" info request)
	 */
	public boolean useServicesAlternate;

	/**
	 * For testing purposes only.  Do not modify.
	 * <p>
	 * Should the AerospikeClient instance communicate with the first seed node only
	 * instead of using the data partition map to determine which node to send the
	 * database command.
	 * <p>
	 * Default: false
	 */
	public boolean forceSingleNode;

	/**
	 * Track server rack data.  This field is useful when directing read commands to the server node
	 * that contains the key and exists on the same rack as the client.  This serves to lower cloud
	 * provider costs when nodes are distributed across different racks/data centers.
	 * <p>
	 * {@link ClientPolicy#rackId} or {@link ClientPolicy#rackIds}, {@link Replica#PREFER_RACK}
	 * and server rack configuration must also be set to enable this functionality.
	 * <p>
	 * Default: false
	 */
	public boolean rackAware;

	/**
	 * Rack where this client instance resides. If {@link ClientPolicy#rackIds} is set, rackId is ignored.
	 * <p>
	 * {@link ClientPolicy#rackAware}, {@link Replica#PREFER_RACK} and server rack
	 * configuration must also be set to enable this functionality.
	 * <p>
	 * Default: 0
	 */
	public int rackId;

	/**
	 * List of acceptable racks in order of preference. Nodes in rackIds[0] are chosen first.
	 * If a node is not found in rackIds[0], then nodes in rackIds[1] are searched, and so on.
	 * If rackIds is set, {@link ClientPolicy#rackId} is ignored.
	 * <p>
	 * {@link ClientPolicy#rackAware}, {@link Replica#PREFER_RACK} and server rack
	 * configuration must also be set to enable this functionality.
	 * <p>
	 * Default: null
	 */
	public List<Integer> rackIds;

	/**
	 * Application ID. Metrics are loosely tied to this. Changing the appId will not reset the metric counters.
	 */
	public String appId;

	/**
	 * Copy client policy from another client policy AND override certain policy attributes if they exist in the
	 * configProvider. Any policy overrides will not get logged.
	 */
	public ClientPolicy(ClientPolicy other, ConfigurationProvider configProvider) {
		this(other);
		updateFromConfig(configProvider, false);
	}

	/**
	 * Copy client policy from another client policy AND override certain policy attributes if they exist in the
	 * configProvider. Any default policy overrides will get logged.
	 */
	public ClientPolicy(ClientPolicy other, ConfigurationProvider configProvider, boolean isDefaultPolicy) {
		this(other);
		updateFromConfig(configProvider, isDefaultPolicy);
	}

	/**
	 * Copy client policy from another client policy.
	 */
	public ClientPolicy(ClientPolicy other) {
		this.eventLoops = other.eventLoops;
		this.user = other.user;
		this.password = other.password;
		this.clusterName = other.clusterName;
		this.authMode = other.authMode;
		this.timeout = other.timeout;
		this.loginTimeout = other.loginTimeout;
		this.closeTimeout = other.closeTimeout;
		this.minConnsPerNode = other.minConnsPerNode;
		this.maxConnsPerNode = other.maxConnsPerNode;
		this.asyncMinConnsPerNode = other.asyncMinConnsPerNode;
		this.asyncMaxConnsPerNode = other.asyncMaxConnsPerNode;
		this.connPoolsPerNode = other.connPoolsPerNode;
		this.maxSocketIdle = other.maxSocketIdle;
		this.maxErrorRate = other.maxErrorRate;
		this.errorRateWindow = other.errorRateWindow;
		this.tendInterval = other.tendInterval;
		this.failIfNotConnected = other.failIfNotConnected;
		this.validateClusterName = other.validateClusterName;
		this.readPolicyDefault = new Policy(other.readPolicyDefault);
		this.writePolicyDefault = new WritePolicy(other.writePolicyDefault);
		this.scanPolicyDefault = new ScanPolicy(other.scanPolicyDefault);
		this.queryPolicyDefault = new QueryPolicy(other.queryPolicyDefault);
		this.batchPolicyDefault = new BatchPolicy(other.batchPolicyDefault);
		this.batchParentPolicyWriteDefault = new BatchPolicy(other.batchParentPolicyWriteDefault);
		this.batchWritePolicyDefault = new BatchWritePolicy(other.batchWritePolicyDefault);
		this.batchDeletePolicyDefault = new BatchDeletePolicy(other.batchDeletePolicyDefault);
		this.batchUDFPolicyDefault = new BatchUDFPolicy(other.batchUDFPolicyDefault);
		this.txnVerifyPolicyDefault = new TxnVerifyPolicy(other.txnVerifyPolicyDefault);
		this.txnRollPolicyDefault = new TxnRollPolicy(other.txnRollPolicyDefault);
		this.infoPolicyDefault = new InfoPolicy(other.infoPolicyDefault);
		this.tlsPolicy = (other.tlsPolicy != null)? new TlsPolicy(other.tlsPolicy) : null;
		this.keepAlive = (other.keepAlive != null)? new TCPKeepAlive(other.keepAlive) : null;
		this.ipMap = other.ipMap;
		this.threadPool = other.threadPool;
		this.sharedThreadPool = (other.threadPool != null);
		this.useServicesAlternate = other.useServicesAlternate;
		this.forceSingleNode = other.forceSingleNode;
		this.rackAware = other.rackAware;
		this.rackId = other.rackId;
		this.rackIds = (other.rackIds != null)? new ArrayList<Integer>(other.rackIds) : null;
		this.appId = other.appId;
	}

	/**
	 * Default constructor.
	 */
	public ClientPolicy() {
	}

	private void validateErrorRateConfig() {
		float ratio = (errorRateWindow == 0)? (float) 0.0 : ((float)maxErrorRate / (float)errorRateWindow);

		if ( !(1 <= ratio && ratio <= 100)) {
			int mer = maxErrorRate;
			int erw = errorRateWindow;

			this.maxErrorRate = 100;
			this.errorRateWindow = 1;

			Log.warn("Invalid circuit breaker configuration: max_error_rate: " + mer + ", error_rate_window: " + erw +
					 ", ratio: " + ratio +". The ratio (max_error_rate/error_rate_window) must be between 1 and 100." +
					 " Resetting to defaults - max_error_rate: " + maxErrorRate + " and error_rate_window: " +
					 errorRateWindow);
		}
	}

	private void updateFromConfig(ConfigurationProvider configProvider, boolean log) {
		boolean logUpdate = false;
		if (configProvider == null) {
			return;
		}
		Configuration config = configProvider.fetchConfiguration();
		if (config == null) {
			return;
		}
		StaticConfiguration sConfig = config.getStaticConfiguration();
		if (sConfig == null) {
			return;
		}
		StaticClientConfig staCC = sConfig.getStaticClientConfig();
		if (staCC == null) {
			return;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}
		if (staCC.maxConnectionsPerNode != null && this.maxConnsPerNode != staCC.maxConnectionsPerNode.value) {
			this.maxConnsPerNode = staCC.maxConnectionsPerNode.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.maxConnsPerNode = " + this.maxConnsPerNode);
			}
		}
		if (staCC.minConnectionsPerNode != null && this.minConnsPerNode != staCC.minConnectionsPerNode.value) {
			this.minConnsPerNode = staCC.minConnectionsPerNode.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.minConnsPerNode = " + this.minConnsPerNode);
			}
		}
		if (staCC.asyncMaxConnectionsPerNode != null && this.asyncMaxConnsPerNode != staCC.asyncMaxConnectionsPerNode.value) {
			this.asyncMaxConnsPerNode = staCC.asyncMaxConnectionsPerNode.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.asyncMaxConnsPerNode = " + this.asyncMaxConnsPerNode);
			}
		}
		if (staCC.asyncMinConnectionsPerNode != null && this.asyncMinConnsPerNode != staCC.asyncMinConnectionsPerNode.value) {
			this.asyncMinConnsPerNode = staCC.asyncMinConnectionsPerNode.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.asyncMinConnsPerNode = " + this.asyncMinConnsPerNode);
			}
		}

		DynamicConfiguration dConfig = config.getDynamicConfiguration();
		if (dConfig == null) {
			return;
		}
		DynamicClientConfig dynCC = dConfig.getDynamicClientConfig();
		if (dynCC == null) {
			return;
		}
		if (dynCC.appId != null && !Objects.equals(this.appId, dynCC.appId.value)) {
			this.appId = dynCC.appId.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.appId = " + this.appId);
			}
		}
		if (dynCC.timeout != null && this.timeout != dynCC.timeout.value) {
			this.timeout = dynCC.timeout.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.timeout = " + this.timeout);
			}
		}
		if (dynCC.errorRateWindow != null && this.errorRateWindow != dynCC.errorRateWindow.value) {
			this.errorRateWindow = dynCC.errorRateWindow.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.errorRateWindow = " + this.errorRateWindow);
			}
		}
		if (dynCC.maxErrorRate != null && this.maxErrorRate != dynCC.maxErrorRate.value) {
			this.maxErrorRate = dynCC.maxErrorRate.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.maxErrorRate = " + this.maxErrorRate);
			}
		}
		validateErrorRateConfig();
		if (dynCC.failIfNotConnected != null && this.failIfNotConnected != dynCC.failIfNotConnected.value) {
			this.failIfNotConnected = dynCC.failIfNotConnected.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.failIfNotConnected = " + this.failIfNotConnected);
			}
		}
		if (dynCC.loginTimeout != null && this.loginTimeout != dynCC.loginTimeout.value) {
			this.loginTimeout = dynCC.loginTimeout.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.loginTimeout = " + this.loginTimeout);
			}
		}
		if (dynCC.maxSocketIdle != null && this.maxSocketIdle != dynCC.maxSocketIdle.value) {
			if (dynCC.maxSocketIdle.value < 0) {
				Log.error("Invalid maxSocketIdle in config: " + dynCC.maxSocketIdle.value);
			} else {
				this.maxSocketIdle = dynCC.maxSocketIdle.value;
				if (logUpdate) {
					Log.info("Set ClientPolicy.maxSocketIdle = " + this.maxSocketIdle);
				}
			}
		}
		if (dynCC.rackAware != null && this.rackAware != dynCC.rackAware.value) {
			this.rackAware = dynCC.rackAware.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.rackAware = " + this.rackAware);
			}
		}
		if (dynCC.rackIds != null && !Util.rackIdsEqual(this.rackIds, dynCC.rackIds.stream().mapToInt(i->i).toArray())) {
			this.rackIds = dynCC.rackIds;
			if (logUpdate) {
				Log.info("Set ClientPolicy.rackIds = " + this.rackIds);
			}
		}
		if (dynCC.tendInterval != null && this.tendInterval != dynCC.tendInterval.value) {
			this.tendInterval = dynCC.tendInterval.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.tendInterval = " + this.tendInterval);
			}
		}
		if (dynCC.useServiceAlternative != null && this.useServicesAlternate != dynCC.useServiceAlternative.value) {
			this.useServicesAlternate = dynCC.useServiceAlternative.value;
			if (logUpdate) {
				Log.info("Set ClientPolicy.useServicesAlternate = " + this.useServicesAlternate);
			}
		}
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setEventLoops(EventLoops eventLoops) {
		this.eventLoops = eventLoops;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public void setAuthMode(AuthMode authMode) {
		this.authMode = authMode;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setLoginTimeout(int loginTimeout) {
		this.loginTimeout = loginTimeout;
	}

	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	public void setMinConnsPerNode(int minConnsPerNode) {
		this.minConnsPerNode = minConnsPerNode;
	}

	public void setMaxConnsPerNode(int maxConnsPerNode) {
		this.maxConnsPerNode = maxConnsPerNode;
	}

	public void setAsyncMinConnsPerNode(int asyncMinConnsPerNode) {
		this.asyncMinConnsPerNode = asyncMinConnsPerNode;
	}

	public void setAsyncMaxConnsPerNode(int asyncMaxConnsPerNode) {
		this.asyncMaxConnsPerNode = asyncMaxConnsPerNode;
	}

	public void setConnPoolsPerNode(int connPoolsPerNode) {
		this.connPoolsPerNode = connPoolsPerNode;
	}

	public void setMaxSocketIdle(int maxSocketIdle) {
		this.maxSocketIdle = maxSocketIdle;
	}

	public void setMaxErrorRate(int maxErrorRate) {
		this.maxErrorRate = maxErrorRate;
	}

	public void setErrorRateWindow(int errorRateWindow) {
		this.errorRateWindow = errorRateWindow;
	}

	public void setTendInterval(int tendInterval) {
		this.tendInterval = tendInterval;
	}

	public void setFailIfNotConnected(boolean failIfNotConnected) {
		this.failIfNotConnected = failIfNotConnected;
	}

	public void setValidateClusterName(boolean validateClusterName) {
		this.validateClusterName = validateClusterName;
	}

	public void setReadPolicyDefault(Policy readPolicyDefault) {
		this.readPolicyDefault = readPolicyDefault;
	}

	public void setWritePolicyDefault(WritePolicy writePolicyDefault) {
		this.writePolicyDefault = writePolicyDefault;
	}

	public void setScanPolicyDefault(ScanPolicy scanPolicyDefault) {
		this.scanPolicyDefault = scanPolicyDefault;
	}

	public void setQueryPolicyDefault(QueryPolicy queryPolicyDefault) {
		this.queryPolicyDefault = queryPolicyDefault;
	}

	public void setBatchPolicyDefault(BatchPolicy batchPolicyDefault) {
		this.batchPolicyDefault = batchPolicyDefault;
	}

	public void setBatchParentPolicyWriteDefault(BatchPolicy batchParentPolicyWriteDefault) {
		this.batchParentPolicyWriteDefault = batchParentPolicyWriteDefault;
	}

	public void setBatchWritePolicyDefault(BatchWritePolicy batchWritePolicyDefault) {
		this.batchWritePolicyDefault = batchWritePolicyDefault;
	}

	public void setBatchDeletePolicyDefault(BatchDeletePolicy batchDeletePolicyDefault) {
		this.batchDeletePolicyDefault = batchDeletePolicyDefault;
	}

	public void setBatchUDFPolicyDefault(BatchUDFPolicy batchUDFPolicyDefault) {
		this.batchUDFPolicyDefault = batchUDFPolicyDefault;
	}

	public void setTxnVerifyPolicyDefault(TxnVerifyPolicy txnVerifyPolicyDefault) {
		this.txnVerifyPolicyDefault = txnVerifyPolicyDefault;
	}

	public void setTxnRollPolicyDefault(TxnRollPolicy txnRollPolicyDefault) {
		this.txnRollPolicyDefault = txnRollPolicyDefault;
	}

	public void setInfoPolicyDefault(InfoPolicy infoPolicyDefault) {
		this.infoPolicyDefault = infoPolicyDefault;
	}

	public void setTlsPolicy(TlsPolicy tlsPolicy) {
		this.tlsPolicy = tlsPolicy;
	}

	public void setKeepAlive(TCPKeepAlive keepAlive) {
		this.keepAlive = keepAlive;
	}

	public void setIpMap(Map<String, String> ipMap) {
		this.ipMap = ipMap;
	}

	public void setThreadPool(ExecutorService threadPool) {
		this.threadPool = threadPool;
	}

	public void setSharedThreadPool(boolean sharedThreadPool) {
		this.sharedThreadPool = sharedThreadPool;
	}

	public void setUseServicesAlternate(boolean useServicesAlternate) {
		this.useServicesAlternate = useServicesAlternate;
	}

	public void setForceSingleNode(boolean forceSingleNode) {
		this.forceSingleNode = forceSingleNode;
	}

	public void setRackAware(boolean rackAware) {
		this.rackAware = rackAware;
	}

	public void setRackId(int rackId) {
		this.rackId = rackId;
	}

	public void setRackIds(List<Integer> rackIds) {
		this.rackIds = rackIds;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}
}
