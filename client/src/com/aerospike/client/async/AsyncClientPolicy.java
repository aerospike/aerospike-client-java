/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.async;

import java.util.concurrent.ExecutorService;

import com.aerospike.client.policy.ClientPolicy;

/**
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
	 * Asynchronous socket read/user callback task thread pool.  The default, null, indicates 
	 * asynchronous tasks should be run in the same thread as the selector.
	 * <p>
	 * Example: asyncTaskThreadPool = Executors.newCachedThreadPool();
	 */
	public ExecutorService asyncTaskThreadPool;
}
