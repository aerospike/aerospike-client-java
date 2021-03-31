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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.async.HashedWheelTimer.HashedWheelTimeout;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.util.Util;

/**
 * Create async connection and place in connection pool.
 * Used for min connections functionality.
 */
public abstract class AsyncConnector implements Runnable, TimerTask {

	final EventLoopBase eventLoop;
	final EventState eventState;
	final Cluster cluster;
	final Node node;
	final AsyncConnector.Listener listener;
	final HashedWheelTimeout timeoutTask;
	final int index;
	int state;

	AsyncConnector(EventLoopBase eventLoop, Cluster cluster, Node node, AsyncConnector.Listener listener) {
		this.eventLoop = eventLoop;
		this.eventState = cluster.eventState[eventLoop.index];
		this.cluster = cluster;
		this.node = node;
		this.listener = listener;
		this.timeoutTask = new HashedWheelTimeout(this);
		this.index = eventLoop.getIndex();
	}

	public final boolean execute() {
		if (! node.reserveAsyncConnectionSlot(index)) {
			return false;
		}

		if (eventState.errors < 5) {
			run();
		}
		else {
			// Avoid recursive error stack overflow by placing request at end of queue.
			eventLoop.execute(this);
		}
		return true;
	}

	@Override
	public void run() {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cluster.connectTimeout);

		eventLoop.timer.addTimeout(timeoutTask, deadline);
		state = AsyncCommand.CONNECT;

		try {
			createConnection();
			eventState.errors = 0;
		}
		catch (AerospikeException ae) {
			eventState.errors++;
			fail(ae);
		}
		catch (Exception e) {
			eventState.errors++;
			fail(new AerospikeException(e));
		}
	}

	@Override
	public final void timeout() {
		fail(new AerospikeException.Timeout(node, cluster.connectTimeout, cluster.connectTimeout, cluster.connectTimeout));
	}

	final void success() {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}

		addConnection();
		close();

		try {
			listener.onSuccess(this);
		}
		catch (Exception e) {
			Log.error("onSuccess() error: " + Util.getErrorMessage(e));
		}
	}

	final void fail(AerospikeException ae) {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}

		closeConnection();
		close();

		try {
			listener.onFailure(ae);
		}
		catch (Exception e) {
			Log.error("onFailure() error: " + Util.getErrorMessage(e));
		}
	}

	private final void close() {
		timeoutTask.cancel();
		state = AsyncCommand.COMPLETE;
	}

	abstract void createConnection();
	abstract void addConnection();
	abstract void closeConnection();

	interface Listener {
		void onSuccess(AsyncConnector ac);
		void onFailure(AerospikeException ae);
	}
}
