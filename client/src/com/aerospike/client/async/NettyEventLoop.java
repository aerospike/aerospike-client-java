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

import com.aerospike.client.Log;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.util.Util;

/**
 * Aerospike wrapper around netty event loop.
 * Implements the Aerospike EventLoop interface.
 */
public final class NettyEventLoop extends EventLoopBase {

	final io.netty.channel.EventLoop eventLoop;
	final NettyEventLoops parent;

    /**
     * Construct Aerospike event loop wrapper from netty event loop.
     */
	public NettyEventLoop(EventPolicy policy, io.netty.channel.EventLoop eventLoop, NettyEventLoops parent, int index) {
		super(policy, index);

		this.eventLoop = eventLoop;
		this.parent = parent;
	}

	/**
	 * Return netty event loop.
	 */
	public io.netty.channel.EventLoop get() {
		return eventLoop;
	}

	/**
	 * Execute async command.  Execute immediately if in event loop.
	 * Otherwise, place command on event loop queue.
	 */
	@Override
	public void execute(Cluster cluster, AsyncCommand command) {
		new NettyCommand(this, cluster, command);
	}

	/**
	 * Schedule execution of runnable command on event loop.
	 * Command is placed on event loop queue and is never executed directly.
	 */
	@Override
	public void execute(Runnable command) {
		eventLoop.execute(command);
	}

	/**
	 * Execute async batch retry.
	 */
	public void executeBatchRetry(Runnable other, AsyncCommand command, long deadline) {
		new NettyCommand((NettyCommand)other, command, deadline);
	}

	/**
	 * Schedule execution of runnable command with delay.
	 */
	@Override
	public void schedule(Runnable command, long delay, TimeUnit unit) {
		eventLoop.schedule(command, delay, unit);
	}

	/**
	 * Schedule execution with a reusable ScheduleTask.
	 */
	@Override
	public void schedule(ScheduleTask task, long delay, TimeUnit unit) {
		eventLoop.schedule(task, delay, unit);
	}

	/**
	 * Is current thread the event loop thread.
=	 */
	@Override
	public boolean inEventLoop() {
		return eventLoop.inEventLoop();
	}

	final void tryDelayQueue() {
		if (maxCommandsInProcess > 0 && !usingDelayQueue) {
			// Try executing commands from the delay queue.
			executeFromDelayQueue();
		}
	}

	final void executeFromDelayQueue() {
		usingDelayQueue = true;

		try {
			NettyCommand cmd;
			while (pending < maxCommandsInProcess && (cmd = (NettyCommand)delayQueue.pollFirst()) != null) {
				if (cmd.state == AsyncCommand.COMPLETE) {
					// Command timed out and user has already been notified.
					continue;
				}
				cmd.executeCommandFromDelayQueue();
			}
		}
		catch (Exception e) {
			Log.error("Unexpected async error: " + Util.getErrorMessage(e));
		}
		finally {
			usingDelayQueue = false;
		}
	}
}
