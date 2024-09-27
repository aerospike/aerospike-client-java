/*
 * Copyright 2012-2024 Aerospike, Inc.
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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncBatch.AsyncBatchCommand;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;

public abstract class AsyncBatchExecutor implements BatchNodeList.IBatchStatus {
	public static final class BatchRecordArray extends AsyncBatchExecutor {
		private final BatchRecordArrayListener listener;
		private final BatchRecord[] records;

		public BatchRecordArray(
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

	public static final class BatchRecordSequence extends AsyncBatchExecutor {
		private final BatchRecordSequenceListener listener;
		private final boolean[] sent;

		public BatchRecordSequence(
			EventLoop eventLoop,
			Cluster cluster,
			BatchRecordSequenceListener listener,
			boolean[] sent
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.sent = sent;
		}

		public void setSent(int index) {
			sent[index] = true;
		}

		public boolean exchangeSent(int index) {
			boolean prev = sent[index];
			sent[index] = true;
			return prev;
		}

		@Override
		public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite) {
			sent[index] = true;
			BatchRecord record = new BatchRecord(key, null, ae.getResultCode(), inDoubt, hasWrite);
			AsyncBatch.onRecord(listener, record, index);
		}

		@Override
		protected void onSuccess() {
			listener.onSuccess();
		}

		@Override
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class ExistsArray extends AsyncBatchExecutor {
		private final ExistsArrayListener listener;
		private final Key[] keys;
		private final boolean[] existsArray;

		public ExistsArray(
			EventLoop eventLoop,
			Cluster cluster,
			ExistsArrayListener listener,
			Key[] keys,
			boolean[] existsArray
		) {
			super(eventLoop, cluster, false);
			this.listener = listener;
			this.keys = keys;
			this.existsArray = existsArray;
		}

		protected void onSuccess() {
			listener.onSuccess(keys, existsArray);
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(new AerospikeException.BatchExists(existsArray, ae));
		}
	}

	public static final class ExistsSequence extends AsyncBatchExecutor {
		private final ExistsSequenceListener listener;

		public ExistsSequence(
			EventLoop eventLoop,
			Cluster cluster,
			ExistsSequenceListener listener
		) {
			super(eventLoop, cluster, false);
			this.listener = listener;
		}

		protected void onSuccess() {
			listener.onSuccess();
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class ReadList extends AsyncBatchExecutor {
		private final BatchListListener listener;
		private final List<BatchRead> records;

		public ReadList(
			EventLoop eventLoop,
			Cluster cluster,
			BatchListListener listener,
			List<BatchRead> records
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.records = records;
		}

		protected void onSuccess() {
			listener.onSuccess(records);
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class ReadSequence extends AsyncBatchExecutor {
		private final BatchSequenceListener listener;

		public ReadSequence(
			EventLoop eventLoop,
			Cluster cluster,
			BatchSequenceListener listener
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
		}

		protected void onSuccess() {
			listener.onSuccess();
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class GetArray extends AsyncBatchExecutor {
		private final RecordArrayListener listener;
		private final Key[] keys;
		private final Record[] records;

		public GetArray(
			EventLoop eventLoop,
			Cluster cluster,
			RecordArrayListener listener,
			Key[] keys,
			Record[] records
		) {
			super(eventLoop, cluster, false);
			this.listener = listener;
			this.keys = keys;
			this.records = records;
		}

		protected void onSuccess() {
			listener.onSuccess(keys, records);
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(new AerospikeException.BatchRecords(records, ae));
		}
	}

	public static final class GetSequence extends AsyncBatchExecutor {
		private final RecordSequenceListener listener;

		public GetSequence(
			EventLoop eventLoop,
			Cluster cluster,
			RecordSequenceListener listener
		) {
			super(eventLoop, cluster, false);
			this.listener = listener;
		}

		@Override
		protected void onSuccess() {
			listener.onSuccess();
		}

		@Override
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class OperateList extends AsyncBatchExecutor {
		private final BatchOperateListListener listener;
		private final List<BatchRecord> records;

		public OperateList(
			EventLoop eventLoop,
			Cluster cluster,
			BatchOperateListListener listener,
			List<BatchRecord> records
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.records = records;
		}

		protected void onSuccess() {
			listener.onSuccess(records, getStatus());
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class OperateSequence extends AsyncBatchExecutor {
		private final BatchRecordSequenceListener listener;

		public OperateSequence(
			EventLoop eventLoop,
			Cluster cluster,
			BatchRecordSequenceListener listener
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
		}

		protected void onSuccess() {
			listener.onSuccess();
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	//-------------------------------------------------------
	// Base Executor
	//-------------------------------------------------------

	final EventLoop eventLoop;
	final Cluster cluster;
	private ArrayList<AerospikeException> subExceptions;
	private AerospikeException exception;
	private AsyncCommand[] commands;
	private int completedCount;  // Not atomic because all commands run on same event loop thread.
	private final boolean hasResultCode;
	boolean done;
	boolean error;

	/**
	 * For internal use only.
	 */
	protected AsyncBatchExecutor(EventLoop eventLoop, Cluster cluster, boolean hasResultCode) {
		this.eventLoop = eventLoop;
		this.cluster = cluster;
		this.hasResultCode = hasResultCode;
		cluster.addCommandCount();
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
					exception.setSubExceptions(subExceptions);
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

	public void addSubException(AerospikeException ae) {
		// All batch sub-commands are run in the same event loop, so mutex is not required.
		if (subExceptions == null) {
			subExceptions = new ArrayList<AerospikeException>();
		}
		subExceptions.add(ae);
	}

	public boolean getStatus() {
		return !error;
	}

	abstract void onSuccess();
	abstract void onFailure(AerospikeException ae);
}
