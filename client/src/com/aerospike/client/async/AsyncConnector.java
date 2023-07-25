/*
 * Copyright 2012-2023 Aerospike, Inc.
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

	final EventLoopBase eventLoopBase;
	final EventState eventState;
	final Cluster cluster;
	final Node node;
	final AsyncConnector.Listener listener;
	final HashedWheelTimeout timeoutTask;
	int state;

	AsyncConnector(EventLoopBase eventLoopBase, Cluster cluster, Node node, AsyncConnector.Listener listener) {
		this.eventLoopBase = eventLoopBase;
		this.eventState = cluster.eventState[eventLoopBase.index];
		this.cluster = cluster;
		this.node = node;
		this.listener = listener;
		this.timeoutTask = new HashedWheelTimeout(this);
	}

	public final boolean execute() {
		if (! node.reserveAsyncConnectionSlot(eventLoopBase.index)) {
			return false;
		}

		try {
			if (eventState.errors < 5) {
				run();
			}
			else {
				// Avoid recursive error stack overflow by placing request at end of queue.
				eventLoopBase.execute(this);
			}
			return true;
		}
		catch (Throwable e) {
			Log.warn("Failed to create conn: " + Util.getErrorMessage(e));
			node.decrAsyncConnection(eventLoopBase.index);
			return false;
		}
	}

	@Override
	public void run() {
		try {
			long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cluster.connectTimeout);
			eventLoopBase.timer.addTimeout(timeoutTask, deadline);

			createConnection();
			eventState.errors = 0;
		}
		catch (AerospikeException ae) {
			eventState.errors++;
			fail(ae);
		}
		catch (Throwable e) {
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

		close();

		if (addConnection()) {
			try {
				listener.onSuccess(this);
			}
			catch (Throwable e) {
				Log.error("onSuccess() error: " + Util.getErrorMessage(e));
			}
		}
		else {
			try {
				listener.onFailure();
			}
			catch (Throwable e) {
				Log.error("onFailure() error: " + Util.getErrorMessage(e));
			}
		}
	}

	final void fail(AerospikeException ae) {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}

		close();
		closeConnection();

		try {
			listener.onFailure(ae);
		}
		catch (Throwable e) {
			Log.error("onFailure() error: " + Util.getErrorMessage(e));
		}
	}

	private final void close() {
		timeoutTask.cancel();
		state = AsyncCommand.COMPLETE;
	}

	abstract void createConnection();
	abstract boolean addConnection();
	abstract void closeConnection();

	interface Listener {
		void onSuccess(AsyncConnector ac);
		void onFailure(AerospikeException ae);
		void onFailure();
	}
}
