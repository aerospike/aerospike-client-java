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
package com.aerospike.client.query;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Iterable stream of {@link KeyRecord} results from a secondary index query.
 *
 * <p>Producer threads fill an internal queue with records from the server; the single user thread calls
 * {@link #next()} to block until the next record is available, then {@link #getRecord()} to access it.
 * Call {@link #close()} when done to release resources and optionally terminate the query early.
 *
 * <p>Call query with Statement then iterate with next() and getRecord(); close the RecordSet when done.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * RecordSet rs = client.query(queryPolicy, stmt);
 * try {
 *     while (rs.next()) {
 *         KeyRecord kr = rs.getRecord();
 *         Key key = kr.key;
 *         Record rec = kr.record;
 *         // process key, rec
 *     }
 * } finally {
 *     rs.close();
 * }
 * }</pre>
 *
 * @see com.aerospike.client.AerospikeClient#query(com.aerospike.client.policy.QueryPolicy, Statement)
 * @see KeyRecord
 */
public class RecordSet implements Iterable<KeyRecord>, Closeable {
	/** Sentinel value placed on the queue when no more records are available. */
	public static final KeyRecord END = new KeyRecord(null, null);

	private final IQueryExecutor executor;
	private final BlockingQueue<KeyRecord> queue;
	private KeyRecord record;
	private volatile boolean valid = true;

	/**
	 * Initialize record set with underlying producer/consumer queue.
	 */
	protected RecordSet(IQueryExecutor executor, int capacity) {
		this.executor = executor;
		this.queue = new ArrayBlockingQueue<KeyRecord>(capacity);
	}

	/**
	 * For internal use only.
	 */
	protected RecordSet() {
		this.executor = null;
		this.queue = null;
	}

	//-------------------------------------------------------
	// Record traversal methods
	//-------------------------------------------------------

	/**
	 * Advances to the next record, blocking until one is available or the query ends.
	 *
	 * @return {@code true} if a record is available (use {@link #getRecord()}), {@code false} if no more records or query was closed
	 * @throws AerospikeException	when the query failed on the server (e.g. timeout, connection error, or invalid statement).
	 */
	public boolean next() throws AerospikeException {
		if (! valid) {
			executor.checkForException();
			return false;
		}

		try {
			record = queue.take();
		}
		catch (InterruptedException ie) {
			valid = false;

			/*
			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + executor.statement.taskId + " take interrupted");
			}
			*/
			return false;
		}

		if (record == END) {
			valid = false;
			executor.checkForException();
			return false;
		}
		return true;
	}

	/**
	 * Closes this record set and releases resources; may signal the server to stop the query.
	 *
	 * <p>Call in a {@code finally} block to ensure resources are released. After close, {@link #next()} returns {@code false}.
	 */
	public void close() {
		valid = false;

		// Check if more records are available.
		if (record != END && queue.poll() != END) {
			// Some query threads may still be running. Stop these threads.
			executor.stopThreads(new AerospikeException.QueryTerminated());
		}
	}

	/**
	 * Returns an iterator over the records in this set.
	 *
	 * @return an iterator over {@link KeyRecord} elements
	 */
	@Override
	public Iterator<KeyRecord> iterator() {
		return new RecordSetIterator(this);
	}

	//-------------------------------------------------------
	// Meta-data retrieval methods
	//-------------------------------------------------------

	/**
	 * Returns the key of the current record (valid after {@link #next()} returns {@code true}).
	 *
	 * @return the key of the current record
	 */
	public Key getKey() {
		return record.key;
	}

	/**
	 * Returns the record (bins, generation, expiration) of the current record (valid after {@link #next()} returns {@code true}).
	 *
	 * @return the record data for the current record
	 */
	public Record getRecord() {
		return record.record;
	}

	/**
	 * Returns the current {@link KeyRecord} (key and record together).
	 *
	 * @return the key and record for the current record
	 */
	public KeyRecord getKeyRecord() {
		return record;
	}

	//-------------------------------------------------------
	// Methods for internal use only.
	//-------------------------------------------------------

	/**
	 * Put a record on the queue.
	 */
	protected final boolean put(KeyRecord record) {
		if (! valid) {
			return false;
		}

		try {
			// This put will block if queue capacity is reached.
			queue.put(record);
			return true;
		}
		catch (InterruptedException ie) {
			/*
			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + executor.statement.taskId + " put interrupted");
			}
			*/

			// Valid may have changed.  Check again.
			if (valid) {
				abort();
			}
			return false;
		}
	}

	/**
	 * Abort retrieval with end token.
	 */
	protected void abort() {
		valid = false;
		queue.clear();

		// Send end command to command thread.
		// It's critical that the end offer succeeds.
		while (! queue.offer(END)) {
			// Queue must be full. Remove one item to make room.
			if (queue.poll() == null) {
				// Can't offer or poll.  Nothing further can be done.
				/*
				if (Log.debugEnabled()) {
					Log.debug("RecordSet " + executor.statement.taskId + " both offer and poll failed on abort");
				}
				*/
				break;
			}
		}
	}

	/**
	 * Support standard iteration interface for RecordSet.
	 */
	private static class RecordSetIterator implements Iterator<KeyRecord>, Closeable {

		private final RecordSet recordSet;
		private boolean more;

		RecordSetIterator(RecordSet recordSet) {
			this.recordSet = recordSet;
			more = this.recordSet.next();
		}

		@Override
		public boolean hasNext() {
			return more;
		}

		@Override
		public KeyRecord next() {
			KeyRecord kr = recordSet.getKeyRecord();
			more = recordSet.next();
			return kr;
		}

		@Override
		public void close() {
			recordSet.close();
		}
	}
}
