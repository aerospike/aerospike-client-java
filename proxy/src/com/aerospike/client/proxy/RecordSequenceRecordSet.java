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
package com.aerospike.client.proxy;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;

/**
 * A {@link RecordSequenceListener} that implements a {@link RecordSet}.
 */
public class RecordSequenceRecordSet extends RecordSet implements RecordSequenceListener {
	private final long taskId;
	private volatile boolean valid = true;
	private final BlockingQueue<KeyRecord> queue;
	protected volatile KeyRecord record;
	private volatile AerospikeException exception;

	public RecordSequenceRecordSet(long taskId, int capacity) {
		this.queue = new ArrayBlockingQueue<>(capacity);
		this.taskId = taskId;
	}

	/**
	 * Retrieve next record.  This method will block until a record is retrieved
	 * or the query is cancelled.
	 *
	 * @return whether record exists - if false, no more records are available
	 */
	public boolean next() throws AerospikeException {
		if (!valid) {
			checkForException();
			return false;
		}

		try {
			record = queue.take();
		}
		catch (InterruptedException ie) {
			valid = false;

			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + taskId + " take " +
					"interrupted");
			}
			return false;
		}

		if (record == END) {
			valid = false;
			checkForException();
			return false;
		}
		return true;
	}

	private void checkForException() {
		if (exception != null) {
			abort();
			throw exception;
		}
	}

	protected void abort() {
		valid = false;
		queue.clear();

		// Send end command to transaction thread.
		// It's critical that the end offer succeeds.
		while (!queue.offer(END)) {
			// Queue must be full. Remove one item to make room.
			if (queue.poll() == null) {
				// Can't offer or poll.  Nothing further can be done.
				if (Log.debugEnabled()) {
					Log.debug("RecordSet " + taskId + " both offer and poll failed on abort");
				}
				break;
			}
		}
	}

	public void close() {
		valid = false;
	}

	@Override
	public Record getRecord() {
		return record.record;
	}

	@Override
	public Key getKey() {
		return record.key;
	}

	@Override
	public KeyRecord getKeyRecord() {
		return record;
	}

	@Override
	public void onRecord(Key key, Record record) throws AerospikeException {
		if (!valid) {
			// Abort the query.
			throw new AerospikeException.QueryTerminated();
		}

		try {
			// This put will block if queue capacity is reached.
			queue.put(new KeyRecord(key, record));
		}
		catch (InterruptedException ie) {
			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + taskId + " put interrupted");
			}

			// Valid may have changed.  Check again.
			if (valid) {
				abort();
			}

			// Abort the query.
			throw new AerospikeException.QueryTerminated();
		}
	}

	@Override
	public void onSuccess() {
		if (!valid) {
			return;
		}

		try {
			// This put will block if queue capacity is reached.
			queue.put(END);
		}
		catch (InterruptedException ie) {
			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + taskId + " put interrupted");
			}

			// Valid may have changed.  Check again.
			if (valid) {
				abort();
			}
		}
	}

	@Override
	public void onFailure(AerospikeException ae) {
		exception = ae;
		abort();
	}
}

