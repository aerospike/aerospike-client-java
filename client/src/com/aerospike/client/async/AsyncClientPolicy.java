/*
 * Copyright 2012-2018 Aerospike, Inc.
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
package com.aerospike.client.async;

import java.util.concurrent.ExecutorService;

import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * THIS CLASS IS OBSOLETE.
 * <p>
 * This class is not used by the new efficient asynchronous client.
 * This class exists solely to provide compatibility with legacy applications.
 * <p>
 * Container object for client policy Command.
 */
public final class AsyncClientPolicy extends ClientPolicy {	
	/**
	 * How to handle cases when the asynchronous maximum number of concurrent connections 
	 * have been reached.  
	 */
	public MaxCommandAction asyncMaxCommandAction = MaxCommandAction.BLOCK;

	/**
	 * Maximum number of concurrent asynchronous commands that are active at any point in time.
	 * Concurrent commands can be used to estimate concurrent connections.
	 * The number of concurrent open connections is limited by:
	 * <p>
	 * max open connections = asyncMaxCommands * <number of nodes in cluster>
	 * </p>
	 * The actual number of open connections consumed depends on how balanced the commands are 
	 * between nodes and if asyncMaxConnAction is ACCEPT.
	 * <p>
	 * The maximum open connections should not exceed the total socket file descriptors available
	 * on the client machine.  The socket file descriptors available can be determined by the
	 * following command:
	 * <p>
	 * ulimit -n
	 */
	public int asyncMaxCommands = 200;

	/**
	 * Maximum milliseconds to wait for an asynchronous network selector event.  
	 * The default value of zero indicates the selector should not timeout.
	 */
	public int asyncSelectorTimeout;

	/**
	 * Number of selector threads used to process asynchronous network events.  The default is
	 * a single threaded network handler.  Some applications may benefit from increasing this
	 * value to the number of CPU cores on the executing machine.
	 */
	public int asyncSelectorThreads = 1;
	
	/**
	 * Asynchronous socket read/user callback task thread pool. If asyncTaskThreadPool is not
	 * defined (default), asynchronous tasks will be run in the same thread as the selector.
	 * If asyncTaskThreadPool is defined, the socket read and user callback will be run in a
	 * separate thread from the selector thread.
	 * <p>
	 * A variable sized thread pool can handle any amount of tasks.
	 * <pre>
	 * // Daemon threads automatically terminate when the program terminates.
	 * asyncTaskThreadPool = Executors.newCachedThreadPool(new ThreadFactory() {
	 *     public final Thread newThread(Runnable runnable) {
	 *			Thread thread = new Thread(runnable);
	 *			thread.setDaemon(true);
	 *			return thread;
	 *		}
	 * });
	 * </pre>
	 * If a fixed size thread pool is desired, the size should be the same as asyncMaxCommands.
	 * <pre>
	 * asyncTaskThreadPool = Executors.newFixedThreadPool(asyncMaxCommands, new ThreadFactory() {
	 *     public final Thread newThread(Runnable runnable) {
	 *			Thread thread = new Thread(runnable);
	 *			thread.setDaemon(true);
	 *			return thread;
	 *		}
	 * });
	 * </pre>
	 * Deadlock can occur when asyncTaskThreadPool is not defined, asyncMaxCommandAction equals
	 * BLOCK and there are many instances of nested async commands  (one command triggers new
	 * commands in the user callback).  It is imperative that asyncTaskThreadPool be defined if
	 * your application is using this scenario.
	 */
	public ExecutorService asyncTaskThreadPool;
	
	/**
	 * Default read policy that is used when asynchronous read command's policy is null.
	 */
	public Policy asyncReadPolicyDefault = new Policy();
	
	/**
	 * Default write policy that is used when asynchronous write command's policy is null.
	 */
	public WritePolicy asyncWritePolicyDefault = new WritePolicy();
	
	/**
	 * Default scan policy that is used when asynchronous scan command's policy is null.
	 */
	public ScanPolicy asyncScanPolicyDefault = new ScanPolicy();

	/**
	 * Default scan policy that is used when asynchronous query command's policy is null.
	 */
	public QueryPolicy asyncQueryPolicyDefault = new QueryPolicy();

	/**
	 * Default batch policy that is used when asynchronous batch command's policy is null.
	 */
	public BatchPolicy asyncBatchPolicyDefault = new BatchPolicy();

	/**
	 * Default constructor.
	 */
	public AsyncClientPolicy() {
		// Setting sleepBetweenRetries > 0 is a bad idea in asynchronous mode when 
		// there is no taskThreadPool defined.  Each failed command would sleep in sequence 
		// (not parallel), thus compounding recovery time.  Reset sleep time to zero because
		// there is no asyncTaskThreadPool by default.
		asyncReadPolicyDefault.sleepBetweenRetries = 0;
		asyncWritePolicyDefault.sleepBetweenRetries = 0;
		asyncScanPolicyDefault.sleepBetweenRetries = 0;
		asyncQueryPolicyDefault.sleepBetweenRetries = 0;
		asyncBatchPolicyDefault.sleepBetweenRetries = 0;
		
		// Running batch commands in sequence is not an advantage in asynchronous mode, so
		// default to issuing commands to all referenced nodes in parallel.
		asyncBatchPolicyDefault.maxConcurrentThreads = 0;
	}
}
