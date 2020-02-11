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

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;

/**
 * Common methods for Netty and NIO event loop.
 */
public abstract class EventLoopBase implements EventLoop {

	final ArrayDeque<Runnable> delayQueue;
	final ArrayDeque<byte[]> bufferQueue;
	final HashedWheelTimer timer;
	final int index;
	final int maxCommandsInProcess;
	final int maxCommandsInQueue;
	int pending;
    boolean usingDelayQueue;

    /**
     * Common event loop constructor.
     */
	public EventLoopBase(EventPolicy policy, int index) {
		if (policy.maxCommandsInProcess > 0 && policy.maxCommandsInProcess < 5) {
			throw new AerospikeException("maxCommandsInProcess " + policy.maxCommandsInProcess + " must be 0 or >= 5");
		}
		delayQueue = (policy.maxCommandsInProcess > 0) ? new ArrayDeque<Runnable>(policy.queueInitialCapacity) : null;
		bufferQueue = new ArrayDeque<byte[]>(policy.commandsPerEventLoop);
		timer = new HashedWheelTimer(this, policy.minTimeout, TimeUnit.MILLISECONDS, policy.ticksPerWheel);
		this.index = index;
		this.maxCommandsInProcess = policy.maxCommandsInProcess;
		this.maxCommandsInQueue = policy.maxCommandsInQueue;
	}

	/**
	 * Return the approximate number of commands currently being processed on
	 * the event loop.  The value is approximate because the call may be from a
	 * different thread than the event loop’s thread and there are no locks or
	 * atomics used.
	 */
	public int getProcessSize() {
		return pending;
	}

	/**
	 * Return the approximate number of commands stored on this event loop's
	 * delay queue that have not been started yet.  The value is approximate
	 * because the call may be from a different thread than the event loop’s
	 * thread and there are no locks or atomics used.
	 */
	public int getQueueSize() {
		return (delayQueue != null) ? delayQueue.size() : 0;
	}

	/**
	 * Return event loop array index.
	 */
	@Override
	public int getIndex() {
		return index;
	}

	/**
     * For internal use only.
     */
	@Override
	public EventState createState() {
		return new EventState(this, index);
	}
}
