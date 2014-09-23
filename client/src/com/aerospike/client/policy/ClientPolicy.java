/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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

/**
 * Container object for client policy Command.
 */
public class ClientPolicy {
	/**
	 * User authentication to cluster.  Leave null for clusters running without restricted access.
	 */
	public String user;

	/**
	 * Password authentication to cluster.  The password will be stored by the client and sent to server
	 * in hashed format.  Leave null for clusters running without restricted access.
	 */
	public String password;

	/**
	 * Initial host connection timeout in milliseconds.  The timeout when opening a connection 
	 * to the server host for the first time.
	 */
	public int timeout = 1000;

	/**
	 * Estimate of incoming threads concurrently using synchronous methods in the client instance.
	 * This field is used to size the synchronous connection pool for each server node.
	 */
	public int maxThreads = 300;
	
	/**
	 * Maximum socket idle in seconds.  Socket connection pools will discard sockets
	 * that have been idle longer than the maximum.
	 */
	public int maxSocketIdle = 14;

	/**
	 * Interval in milliseconds between cluster tends by maintenance thread.  Default: 1 second
	 */
	public int tendInterval = 1000;

	/**
	 * Throw exception if host connection fails during addHost().
	 */
	public boolean failIfNotConnected;
	
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
	 * A IP translation table is used in cases where different clients use different server 
	 * IP addresses.  This may be necessary when using clients from both inside and outside 
	 * a local area network.  Default is no translation.
	 *  
	 * The key is the IP address returned from friend info requests to other servers.  The 
	 * value is the real IP address used to connect to the server.
	 */
	public Map<String,String> ipMap;

	/**
	 * Underlying thread pool used in batch, scan, and query commands. These commands are often 
	 * sent to multiple server nodes in parallel threads.  A thread pool improves performance
	 * because threads do not have to be created/destroyed for each command.
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
	 */
	public boolean sharedThreadPool;
}
