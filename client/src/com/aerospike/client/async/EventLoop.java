/*
 * Copyright 2012-2021 Aerospike, Inc.
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

import java.util.concurrent.TimeUnit;

import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;

/**
 * Single event loop used to run asynchronous Aerospike operations.
 * <p>
 * Obtain an event loop from {@link EventLoops#next()} or {@link EventLoops#get(int)} and pass it
 * to async client methods (e.g. {@link com.aerospike.client.IAerospikeClient#get}).
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy policy = new ClientPolicy();
 * policy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(policy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * client.get(loop, new Policy(), new RecordListener() { ... }, key);
 * }</pre>
 *
 * @see EventLoops
 * @see NioEventLoop
 * @see NettyEventLoop
 */
public interface EventLoop {
	/**
	 * Executes an async command on this event loop; runs immediately if called from the event loop thread, otherwise enqueues.
	 * @param cluster cluster (must not be null)
	 * @param command command to run (must not be null)
	 */
	public void execute(Cluster cluster, AsyncCommand command);

	/**
	 * Schedules a runnable on this event loop; always enqueues and never runs in the caller thread.
	 * @param command runnable to run (must not be null)
	 */
	public void execute(Runnable command);

	/** Retry async batch command. For internal use only. */
	public void executeBatchRetry(Runnable other, AsyncCommand command, long deadline);

	/**
	 * Schedules a runnable to run after the given delay.
	 * @param command runnable to run (must not be null)
	 * @param delay delay amount
	 * @param unit unit of delay (must not be null)
	 */
	public void schedule(Runnable command, long delay, TimeUnit unit);

	/**
	 * Schedules a reusable task to run after the given delay.
	 * @param task task to run (must not be null)
	 * @param delay delay amount
	 * @param unit unit of delay (must not be null)
	 */
	public void schedule(ScheduleTask task, long delay, TimeUnit unit);

	/**
	 * Creates an async connector command for the given cluster and node.
	 * @param cluster cluster (must not be null)
	 * @param node node (must not be null)
	 * @param listener listener (must not be null)
	 * @return new connector command
	 */
	public AsyncConnector createConnector(Cluster cluster, Node node, AsyncConnector.Listener listener);

/**
	 * Approximate number of commands currently being processed on this event loop.
	 * <p>
	 * Value is approximate when called from a different thread. For accuracy from another thread, run this inside {@link #execute(Runnable)} on this event loop.
	 * @return approximate process size (non-negative)
	 */
	public int getProcessSize();

	/**
	 * Approximate number of commands on this event loop's delay queue not yet started.
	 * <p>
	 * Value is approximate when called from a different thread. For accuracy from another thread, run this inside {@link #execute(Runnable)} on this event loop.
	 * @return approximate queue size (non-negative)
	 */
	public int getQueueSize();

	/** @return this event loop's index in its {@link EventLoops} array */
	public int getIndex();

	/** @return true if the current thread is this event loop's thread */
	public boolean inEventLoop();

	/** For internal use only. */
	public EventState createState();
}
