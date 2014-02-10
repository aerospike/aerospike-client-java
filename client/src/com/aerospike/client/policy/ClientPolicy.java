/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
