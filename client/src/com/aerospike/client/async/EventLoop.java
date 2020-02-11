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
package com.aerospike.client.async;

import java.util.concurrent.TimeUnit;

import com.aerospike.client.cluster.Cluster;

/**
 * Aerospike event loop interface.
 */
public interface EventLoop {
	/**
	 * Execute async command.  Execute immediately if in event loop.
	 * Otherwise, place command on event loop queue.
	 */
	public void execute(Cluster cluster, AsyncCommand command);

	/**
	 * Schedule execution of runnable command on event loop.
	 * Command is placed on event loop queue and is never executed directly.
	 */
	public void execute(Runnable command);

	/**
	 * Retry async batch command.  For internal use only.
	 */
	public void executeBatchRetry(Runnable other, AsyncCommand command, long deadline);

	/**
	 * Schedule execution of runnable command with delay.
	 */
	public void schedule(Runnable command, long delay, TimeUnit unit);

	/**
	 * Schedule execution with a reusable ScheduleTask.
	 */
	public void schedule(ScheduleTask task, long delay, TimeUnit unit);

	/**
	 * Return the approximate number of commands currently being processed on
	 * the event loop.  The value is approximate because the call may be from a
	 * different thread than the event loop’s thread and there are no locks or
	 * atomics used.
	 *
	 * If accuracy is important and not running in the event loop thread,
	 * the slower execute(Runnable) can be called to run this method in the
	 * event loop thread.
	 */
	public int getProcessSize();

	/**
	 * Return the approximate number of commands stored on this event loop's
	 * delay queue that have not been started yet.  The value is approximate
	 * because the call may be from a different thread than the event loop’s
	 * thread and there are no locks or atomics used.
	 *
	 * If accuracy is important and not running in the event loop thread,
	 * the slower execute(Runnable) can be called to run this method in the
	 * event loop thread.
	 */
	public int getQueueSize();

	/**
	 * Return event loop array index.
	 */
	public int getIndex();

	/**
	 * Is current thread the event loop thread.
=	 */
	public boolean inEventLoop();

	/**
	 * For internal use only.
	 */
	public EventState createState();
}
