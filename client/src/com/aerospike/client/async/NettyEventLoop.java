/*
 * Copyright 2012-2017 Aerospike, Inc.
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

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.cluster.Cluster;

/**
 * Aerospike wrapper around netty event loop.
 * Implements the Aerospike EventLoop interface.
 */
public final class NettyEventLoop implements EventLoop {

	final io.netty.channel.EventLoop eventLoop;
	final ArrayDeque<byte[]> bufferQueue;
	final HashedWheelTimer timer;
	final NettyEventLoops parent;
	final int index;

    /**
     * Construct Aerospike event loop wrapper from netty event loop.
     */
	public NettyEventLoop(EventPolicy policy, io.netty.channel.EventLoop eventLoop, NettyEventLoops parent, int index) {
		this.eventLoop = eventLoop;
		this.parent = parent;
		this.index = index;
		this.bufferQueue = new ArrayDeque<byte[]>(policy.commandsPerEventLoop);
		this.timer = new HashedWheelTimer(this, policy.minTimeout, TimeUnit.MILLISECONDS, policy.ticksPerWheel);
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
	 * Return event loop array index.
	 */
	@Override
	public int getIndex() {
		return index;
	}

	/**
	 * Is current thread the event loop thread.
=	 */
	public boolean inEventLoop() {
		return eventLoop.inEventLoop();
	}

	/**
     * For internal use only.
     */
	@Override
	public EventState createState() {
		return new EventState(this, index);
	}
}
