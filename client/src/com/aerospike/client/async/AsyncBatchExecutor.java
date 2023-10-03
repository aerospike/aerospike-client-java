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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncBatch.AsyncBatchCommand;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.listener.BatchRecordArrayListener;

public abstract class AsyncBatchExecutor implements BatchNodeList.IBatchStatus {
	public static final class BatchRecordArrayExecutor extends AsyncBatchExecutor {
		private final BatchRecordArrayListener listener;
		private final BatchRecord[] records;

		public BatchRecordArrayExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchRecordArrayListener listener,
			BatchRecord[] records
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.records = records;
		}

		protected void onSuccess() {
			listener.onSuccess(records, getStatus());
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(records, ae);
		}
	}

	final EventLoop eventLoop;
	final Cluster cluster;
	private AerospikeException exception;
	private AsyncCommand[] commands;
	private int completedCount;  // Not atomic because all commands run on same event loop thread.
	private final boolean hasResultCode;
	boolean done;
	boolean error;

	public AsyncBatchExecutor(EventLoop eventLoop, Cluster cluster, boolean hasResultCode) {
		this.eventLoop = eventLoop;
		this.cluster = cluster;
		this.hasResultCode = hasResultCode;
		cluster.addTran();
	}

	public void execute(AsyncCommand[] cmds) {
		this.commands = cmds;

		for (AsyncCommand cmd : cmds) {
			eventLoop.execute(cluster, cmd);
		}
	}

	public void executeBatchRetry(AsyncBatchCommand[] cmds, AsyncBatchCommand orig, Runnable other, long deadline) {
		// Create new commands array.
		AsyncCommand[] target = new AsyncCommand[commands.length + cmds.length - 1];
		int count = 0;

		for (AsyncCommand cmd : commands) {
			if (cmd != orig) {
				target[count++] = cmd;
			}
		}

		for (AsyncCommand cmd : cmds) {
			target[count++] = cmd;
		}
		commands = target;

		for (AsyncCommand cmd : cmds) {
			eventLoop.executeBatchRetry(other, cmd, deadline);
		}
	}

	final void childSuccess() {
		if (++completedCount == commands.length) {
			// All commands complete. Notify success if an exception has not already occurred.
			if (! done) {
				done = true;

				if (exception == null) {
					onSuccess();
				}
				else {
					onFailure(exception);
				}
			}
		}
	}

	final void childFailure(AerospikeException ae) {
		if (exception == null) {
			exception = ae;
		}
		childSuccess();
	}

	@Override
	public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite) {
		// Only used in executors with sequence listeners.
		// These executors will override this method.
	}

	@Override
	public void batchKeyError(AerospikeException e) {
		error = true;

		if (! hasResultCode) {
			// Legacy batch read commands that do not store a key specific resultCode.
			// Store exception which will be passed to the listener on batch completion.
			if (exception == null) {
				exception = e;
			}
		}
	}

	public void setRowError() {
		// Indicate that a key specific error occurred.
		error = true;
	}

	public boolean getStatus() {
		return !error;
	}

	abstract void onSuccess();
	abstract void onFailure(AerospikeException ae);
}
