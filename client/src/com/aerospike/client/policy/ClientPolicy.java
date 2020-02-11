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
package com.aerospike.client.policy;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.aerospike.client.async.EventLoops;

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
	 * Expected cluster name.  If not null, server nodes must return this cluster name in order to
	 * join the client's view of the cluster. Should only be set when connecting to servers that
	 * support the "cluster-name" info command.
	 * <p>
	 * Default: null
	 */
	public String clusterName;

	/**
	 * Authentication mode used when user/password is defined.
	 * <p>
	 * Default: AuthMode.INTERNAL
	 */
	public AuthMode authMode = AuthMode.INTERNAL;

	/**
	 * Initial host connection timeout in milliseconds.  The timeout when opening a connection
	 * to the server host for the first time.
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
	 * Maximum number of connections allowed per server node.  Transactions will go through retry
	 * logic and potentially fail with "ResultCode.NO_MORE_CONNECTIONS" if the maximum number of
	 * connections would be exceeded.
	 * <p>
	 * The number of connections used per node depends on concurrent commands in progress
	 * plus sub-commands used for parallel multi-node commands (batch, scan, and query).
	 * One connection will be used for each command.
	 * <p>
	 * Default: 300
	 */
	public int maxConnsPerNode = 300;

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
	 * that have been idle longer than the maximum.  The value is limited to 24 hours (86400).
	 * <p>
	 * It's important to set this value to a few seconds less than the server's proto-fd-idle-ms
	 * (default 60000 milliseconds or 1 minute), so the client does not attempt to use a socket
	 * that has already been reaped by the server.
	 * <p>
	 * Connection pools are now implemented by a LIFO stack.  Connections at the tail of the
	 * stack will always be the least used.  These connections are checked for maxSocketIdle
	 * once every 30 tend iterations (usually 30 seconds).
	 * <p>
	 * Default: 55
	 */
	public int maxSocketIdle = 55;

	/**
	 * Interval in milliseconds between cluster tends by maintenance thread.
	 * <p>
	 * Default: 1000
	 */
	public int tendInterval = 1000;

	/**
	 * Throw exception if all seed connections fail on cluster instantiation.
	 * <p>
	 * Default: true
	 */
	public boolean failIfNotConnected = true;

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
	 * Default batch policy that is used when batch command's policy is null.
	 */
	public BatchPolicy batchPolicyDefault = new BatchPolicy();

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
	 * A IP translation table is used in cases where different clients use different server
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
	 * Underlying thread pool used in synchronous batch, scan, and query commands. These commands
	 * are often sent to multiple server nodes in parallel threads.  A thread pool improves
	 * performance because threads do not have to be created/destroyed for each command.
	 * The default, null, indicates that the following daemon thread pool will be used:
	 * <pre>
	 * threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
	 *     public final Thread newThread(Runnable runnable) {
	 *			Thread thread = new Thread(runnable);
	 *			thread.setDaemon(true);
	 *			return thread;
	 *		}
	 *	});
	 * </pre>
	 * Daemon threads automatically terminate when the program terminates.
	 * <p>
	 * Default: null (use Executors.newCachedThreadPool)
	 */
	public ExecutorService threadPool;

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
	public boolean sharedThreadPool;

	/**
	 * Should use "services-alternate" instead of "services" in info request during cluster
	 * tending.  "services-alternate" returns server configured external IP addresses that client
	 * uses to talk to nodes.  "services-alternate" can be used in place of providing a client "ipMap".
	 * <p>
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
	 * {@link ClientPolicy#rackId}, {@link Replica#PREFER_RACK} and server rack
	 * configuration must also be set to enable this functionality.
	 * <p>
	 * Default: false
	 */
	public boolean rackAware;

	/**
	 * Rack where this client instance resides.
	 * <p>
	 * {@link ClientPolicy#rackAware}, {@link Replica#PREFER_RACK} and server rack
	 * configuration must also be set to enable this functionality.
	 * <p>
	 * Default: 0
	 */
	public int rackId;

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
		this.maxConnsPerNode = other.maxConnsPerNode;
		this.connPoolsPerNode = other.connPoolsPerNode;
		this.maxSocketIdle = other.maxSocketIdle;
		this.tendInterval = other.tendInterval;
		this.failIfNotConnected = other.failIfNotConnected;
		this.readPolicyDefault = new Policy(other.readPolicyDefault);
		this.writePolicyDefault = new WritePolicy(other.writePolicyDefault);
		this.scanPolicyDefault = new ScanPolicy(other.scanPolicyDefault);
		this.queryPolicyDefault = new QueryPolicy(other.queryPolicyDefault);
		this.batchPolicyDefault = new BatchPolicy(other.batchPolicyDefault);
		this.infoPolicyDefault = new InfoPolicy(other.infoPolicyDefault);
		this.tlsPolicy = (other.tlsPolicy != null)? new TlsPolicy(other.tlsPolicy) : null;
		this.ipMap = other.ipMap;
		this.threadPool = other.threadPool;
		this.sharedThreadPool = (other.threadPool != null);
		this.useServicesAlternate = other.useServicesAlternate;
		this.forceSingleNode = other.forceSingleNode;
		this.rackAware = other.rackAware;
		this.rackId = other.rackId;
	}

	/**
	 * Default constructor.
	 */
	public ClientPolicy() {
	}
}
