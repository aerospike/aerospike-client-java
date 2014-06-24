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

import java.util.concurrent.ExecutorService;

/**
 * Container object for client policy Command.
 */
public class ClientPolicy {
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
	 * Throw exception if host connection fails during addHost().
	 */
	public boolean failIfNotConnected;
	
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
	 *</pre>
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
